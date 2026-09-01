#!/usr/bin/env bash
# Portable CI runner + attestation cache.
#
# One definition of "CI" (ci/checks.tsv) that both GitHub Actions and a developer
# laptop execute. A successful run publishes an ATTESTATION — a git ref naming the
# check, a content fingerprint of everything the check depends on, the platform,
# and the capabilities the environment actually had. The gate in the workflow
# looks those refs up and skips any check already proven for the exact tree it is
# about to test, whoever proved it.
#
#   scripts/ci/ci.sh fingerprint [check...]   print check -> fingerprint
#   scripts/ci/ci.sh caps                     capabilities this environment provides
#   scripts/ci/ci.sh gate [check...]          decide skip/run per check (writes $GITHUB_OUTPUT)
#   scripts/ci/ci.sh run [check...]           run checks here, attest each success
#   scripts/ci/ci.sh attest <check>           publish an attestation by hand
#   scripts/ci/ci.sh local [check...]         start CI's services, then run the checks
#   scripts/ci/ci.sh down                     stop the local CI services
#   scripts/ci/ci.sh gc [days]                delete attestations older than N days (default 30)
#   scripts/ci/ci.sh selftest                 exercise this script's own logic
#
# Bash 3.2 compatible (macOS ships it): no associative arrays, no mapfile.
set -euo pipefail

NS=refs/ci-attest/v1
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
REMOTE=${CI_ATTEST_REMOTE:-origin}
# Bump to invalidate every attestation at once (e.g. after fixing a fingerprint bug).
EPOCH=1
# Capabilities this environment provides; set once per run by cmd_run.
CAPS=''

cd "$ROOT"

# Container CI jobs run as root over a checkout owned by another uid, and git then
# refuses every command in it with "detected dubious ownership" (exit 128). Only
# touches the global config when git is already unusable here, which is the same
# thing anyone would have to do by hand.
git rev-parse --git-dir >/dev/null 2>&1 || git config --global --add safe.directory "$ROOT"

die() { echo "ci: $*" >&2; exit 1; }
note() { echo "── $*" >&2; }

sha256() { if command -v sha256sum >/dev/null 2>&1; then sha256sum | cut -d' ' -f1; else shasum -a 256 | cut -d' ' -f1; fi; }

# ---------------------------------------------------------------- path sets

# Everything the Rust build and its tests read. build.rs codegens from proto/, and
# .cargo/config.toml holds THE definition of `cargo lint`, so a change to either
# changes what every check does.
PATHSET_rs='src tests benches Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml .cargo schemas vendor .config/nextest.toml'
# rustfmt reads .rs files and rustfmt.toml and nothing else — not Cargo.lock, not
# the proto definitions. This is the one check where narrowing is clearly sound.
PATHSET_fmt='src tests benches rustfmt.toml rust-toolchain.toml'
# Prepended to every check: changing what a check DOES, or the environment it runs
# in, must invalidate it.
PATHSET_meta='ci/checks.tsv ci/compose.yml scripts/ci .github/workflows/ci.yml'

pathset() { eval "printf '%s' \"\${PATHSET_$1:-}\""; }

expand_inputs() {
  local out='' tok
  for tok in $1; do
    case "$tok" in
      @*) out="$out $(pathset "${tok#@}")" ;;
      *) out="$out $tok" ;;
    esac
  done
  printf '%s' "$out"
}

# ---------------------------------------------------------------- checks file

checks_all() { grep -v '^#' ci/checks.tsv | grep -v '^[[:space:]]*$' | cut -f1; }

check_field() { # <name> <1-based field>
  local line
  line=$(grep -v '^#' ci/checks.tsv | awk -F'\t' -v n="$1" '$1==n' | head -1)
  [ -n "$line" ] || die "unknown check '$1' (have: $(checks_all | tr '\n' ' '))"
  printf '%s' "$line" | cut -f"$2"
}

check_requires() { check_field "$1" 2; }
check_inputs() { check_field "$1" 3; }

# ---------------------------------------------------------------- fingerprint

# A tree object for the WORKING tree (tracked edits + new non-ignored files), not
# for HEAD. So a fingerprint taken on a dirty checkout still matches the commit
# that later records exactly that content — you can attest before you commit.
WORKTREE_TREE=''
worktree_tree() {
  if [ -z "$WORKTREE_TREE" ]; then
    local idx
    idx=$(mktemp -t ci-index.XXXXXX)
    GIT_INDEX_FILE=$idx git read-tree HEAD
    GIT_INDEX_FILE=$idx git add -A .
    WORKTREE_TREE=$(GIT_INDEX_FILE=$idx git write-tree)
    rm -f "$idx"
  fi
  printf '%s' "$WORKTREE_TREE"
}

# Fingerprints are PINNED once per run. Steps mutate the tree as they go — hpack
# rewrites monoscope.cabal, `npm ci` can touch a lockfile, the lint job refactors
# src/ in place — and recomputing afterwards would attest a fingerprint the gate
# never looked up, so nothing would ever be reused.
pin_fingerprints() { # <check...>
  local c
  mkdir -p .ci
  : > .ci/fingerprints.tsv
  for c in "$@"; do printf '%s\t%s\n' "$c" "$(fingerprint "$c")" >> .ci/fingerprints.tsv; done
  export CI_FINGERPRINTS=.ci/fingerprints.tsv
}

fingerprint() { # <check>
  local paths tree pinned
  if [ -n "${CI_FINGERPRINTS:-}" ] && [ -f "${CI_FINGERPRINTS}" ]; then
    pinned=$(awk -F'\t' -v n="$1" '$1==n{print $2}' "$CI_FINGERPRINTS")
    [ -n "$pinned" ] && { printf '%s' "$pinned"; return 0; }
  fi
  tree=$(worktree_tree)
  paths=$(expand_inputs "$PATHSET_meta $(check_inputs "$1")")
  # ls-tree lists blob SHAs, so this hashes content without reading a single file.
  # Missing pathspecs are silently empty, which is what we want for optional files.
  { printf 'monoscope-ci\t%s\t%s\t%s\n' "$EPOCH" "$1" "$(check_requires "$1")"
    # shellcheck disable=SC2086
    git ls-tree -r --full-tree "$tree" -- $paths | sort
  } | sha256
}

# ---------------------------------------------------------------- capabilities

probe_tcp() { # host port
  (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && exec 3<&- 2>/dev/null || return 1
}

# Split a postgres URL into host/port for probing without needing psql.
url_hostport() { # url -> "host port"
  local rest=${1#*://}
  rest=${rest#*@}
  rest=${rest%%/*}
  case "$rest" in *:*) printf '%s %s' "${rest%%:*}" "${rest##*:}" ;;
                  *) printf '%s 5432' "$rest" ;; esac
}

detect_caps() {
  local caps=''
  command -v cargo >/dev/null 2>&1 && caps="$caps rust"
  cargo nextest --version >/dev/null 2>&1 && caps="$caps nextest"
  docker info >/dev/null 2>&1 && caps="$caps docker"
  # MinIO is claimed only for a reachable endpoint at the address the tests use.
  # shellcheck disable=SC2086
  probe_tcp $(url_hostport "${AWS_S3_ENDPOINT:-http://127.0.0.1:9000}") && caps="$caps minio"
  printf '%s' "$(echo "$caps" | tr ' ' '\n' | grep -v '^$' | sort -u | tr '\n' ' ' | sed 's/ $//')"
}

# provides ⊇ requires
caps_satisfy() { # "<provides>" "<requires>"
  local r
  for r in $2; do
    case " $1 " in *" $r "*) ;; *) return 1 ;; esac
  done
  return 0
}

platform_tag() { printf '%s-%s' "$(uname -s | tr '[:upper:]' '[:lower:]')" "$(uname -m | sed 's/arm64/aarch64/;s/x86_64/amd64/')"; }

# ---------------------------------------------------------------- attestations

# refs/ci-attest/v1/<check>/<fingerprint>/<platform>/<caps.joined>/<yyyymmdd>
# Everything the gate needs is in the ref NAME, so deciding costs one ls-remote
# and zero object fetches.
attest_ref() { # <check> <fingerprint> <caps> [platform]
  printf '%s/%s/%s/%s/%s/%s' "$NS" "$1" "$2" "${4:-$(platform_tag)}" "$(echo "$3" | tr ' ' '.')" "$(date -u +%Y%m%d)"
}

REMOTE_REFS_CACHE=''
remote_refs() {
  if [ -z "$REMOTE_REFS_CACHE" ]; then
    REMOTE_REFS_CACHE=$(mktemp -t ci-refs.XXXXXX)
    if [ "${CI_ATTEST_DISABLED:-}" = "true" ]; then
      note "CI_ATTEST_DISABLED=true — ignoring all attestations"
    else
      git ls-remote "$REMOTE" "$NS/*" 2>/dev/null | cut -f2 > "$REMOTE_REFS_CACHE" || true
    fi
  fi
  cat "$REMOTE_REFS_CACHE"
}

# Echo the matching ref if this check is already proven for this fingerprint by
# an environment good enough for it, else nothing.
find_attestation() { # <check>
  local fp req ref caps
  fp=$(fingerprint "$1")
  req=$(check_requires "$1")
  for ref in $(remote_refs | grep "^$NS/$1/$fp/" || true); do
    caps=$(printf '%s' "$ref" | cut -d/ -f7 | tr '.' ' ')
    if caps_satisfy "$caps" "$req"; then printf '%s' "$ref"; return 0; fi
  done
  return 1
}

record_result() { # <check> — attest now, or stage for a caller that has push access
  local fp caps
  fp=$(fingerprint "$1")
  caps=${CAPS:-$(detect_caps)}
  if [ -n "${CI_ATTEST_OUT:-}" ]; then
    mkdir -p "$(dirname "$CI_ATTEST_OUT")"
    printf '%s\t%s\t%s\t%s\n' "$1" "$fp" "$caps" "$(platform_tag)" >> "$CI_ATTEST_OUT"
    note "recorded $1 for publishing"
  else
    publish_attestation "$1" "$fp" "$caps" "$(platform_tag)"
  fi
}

# Publish the results staged by a run that could not push (the local CI container
# has no credentials — origin is ssh and the image has no keys).
cmd_publish() { # [file]
  local f c fp caps plat
  f=${1:-${CI_ATTEST_OUT:-.ci/attest.tsv}}
  [ -s "$f" ] || { note "nothing to publish ($f)"; return 0; }
  while IFS=$'\t' read -r c fp caps plat; do
    [ -n "$c" ] && publish_attestation "$c" "$fp" "$caps" "$plat"
  done < "$f"
  rm -f "$f"
}

publish_attestation() { # <check> [fingerprint] [caps] [platform]
  local fp caps plat ref tree commit runner_id
  fp=${2:-$(fingerprint "$1")}
  caps=${3:-$(detect_caps)}
  plat=${4:-$(platform_tag)}
  ref=$(attest_ref "$1" "$fp" "$caps" "$plat")
  # Who proved it, so a ref traces back to a run or a machine. NOT
  # ${X:+a$X}${X:-b} — the :- arm yields $X itself when set, which emitted the
  # run id twice and made the link unusable.
  if [ -n "${GITHUB_RUN_ID:-}" ]; then runner_id="github-run-$GITHUB_RUN_ID"; else runner_id="$(whoami)@$(hostname)"; fi
  tree=$(git mktree </dev/null)
  commit=$(GIT_AUTHOR_NAME=${GIT_AUTHOR_NAME:-ci-attest} GIT_AUTHOR_EMAIL=${GIT_AUTHOR_EMAIL:-ci@monoscope.tech} \
           GIT_COMMITTER_NAME=${GIT_COMMITTER_NAME:-ci-attest} GIT_COMMITTER_EMAIL=${GIT_COMMITTER_EMAIL:-ci@monoscope.tech} \
           git commit-tree "$tree" -m "check=$1
fingerprint=$fp
caps=$caps
platform=$plat
commit=$(git rev-parse HEAD)
runner=$runner_id")
  if git push -q "$REMOTE" "${commit}:${ref}" 2>/dev/null; then
    note "attested $1 → $ref"
  else
    note "could not publish attestation for $1 (no push access to $REMOTE?) — result is still valid, just not cached"
  fi
}

# ---------------------------------------------------------------- check bodies

# The env CI's build-test job sets. Defined once here so `make ci` and the
# workflow cannot drift: the workflow exports these from ci.yml, and a local run
# gets them from this function. Anything already set in the environment wins, so
# TIMEFUSION_TEST_S3_ENDPOINT can point at a persistent MinIO.
test_env() {
  export AWS_SDK_LOAD_CONFIG=false \
         AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL:-http://127.0.0.1:9000} \
         AWS_REGION=${AWS_REGION:-us-east-1} \
         AWS_S3_BUCKET=${AWS_S3_BUCKET:-timefusion-test} \
         AWS_S3_ENDPOINT=${AWS_S3_ENDPOINT:-http://127.0.0.1:9000} \
         TIMEFUSION_TEST_S3_ENDPOINT=${TIMEFUSION_TEST_S3_ENDPOINT:-http://127.0.0.1:9000} \
         AWS_ALLOW_HTTP=true \
         AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-minioadmin} \
         AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-minioadmin} \
         PGWIRE_PORT=${PGWIRE_PORT:-12345} \
         PORT=${PORT:-8080} \
         TIMEFUSION_TABLE_PREFIX=${TIMEFUSION_TABLE_PREFIX:-timefusion-ci-test} \
         BATCH_INTERVAL_MS=1000 MAX_BATCH_SIZE=1000 ENABLE_BATCH_QUEUE=true \
         MAX_PG_CONNECTIONS=100 AWS_S3_LOCKING_PROVIDER= \
         TIMEFUSION_FOYER_MEMORY_MB=10 TIMEFUSION_FOYER_DISK_MB=50 \
         TIMEFUSION_FOYER_METADATA_MEMORY_MB=10 TIMEFUSION_FOYER_METADATA_DISK_MB=50 \
         TIMEFUSION_FOYER_SHARDS=2 \
         CARGO_TERM_COLOR=always RUST_BACKTRACE=1 CARGO_INCREMENTAL=0
  # macOS: the AWS SDK's rustls provider reads native roots from the keychain and
  # can fail to parse any, panicking with "TrustStore configured to enable native
  # roots but no valid root certificates parsed!" — from inside TLS setup, so the
  # test that dies looks unrelated to certificates (it was variant_column and
  # variant_functions, which touch neither). Point rustls at the system bundle
  # instead. Guarded on the file existing and the var being unset, so Linux CI —
  # where native roots work and the path is ca-certificates.crt — is untouched.
  if [ -z "${SSL_CERT_FILE:-}" ] && [ -f /etc/ssl/cert.pem ]; then export SSL_CERT_FILE=/etc/ssl/cert.pem; fi
}

run_body() { # <check>
  case "$1" in
    fmt)    cargo fmt --all --check ;;
    # `cargo lint` is THE definition, aliased in .cargo/config.toml. A bare
    # `cargo clippy` is not equivalent — it misses --all-features and does not
    # deny warnings, so it passes on code CI rejects.
    clippy) cargo lint ;;
    test)
      test_env
      # CI splits this across two runners purely for wall-clock; a developer on
      # one machine wants the whole suite, so partitioning is opt-in via
      # CI_PARTITION rather than baked in. Either way the command lives here, so
      # the workflow and `make ci` cannot drift. A partitioned run proves only
      # its slice, so ci.yml sets CI_NO_ATTEST and attests once both shards pass.
      # shellcheck disable=SC2086
      cargo nextest run --profile ci --locked ${CI_PARTITION:+--partition "$CI_PARTITION"}
      ;;
    pg-smoke) run_pg_smoke ;;
    # Default features elsewhere: --all-features would pull `e2e` into the main
    # suite, and 40 tests each spinning their own MinIO container swamps the box.
    e2e)
      test_env
      cargo nextest run --profile ci --features e2e --locked -E 'binary(e2e)' --test-threads 2
      ;;
    *) die "no body for check '$1'" ;;
  esac
}

# Drive the real libpq 18 client against pgwire: it sends a protocol-3.2 probe at
# startup that replaying SQL never exercises, plus the catalog meta-commands.
run_pg_smoke() {
  local data_dir log pid ready=false _
  test_env
  data_dir=$(mktemp -d)
  log=$(mktemp -t tf-pg-smoke.XXXXXX)
  # Build BEFORE backgrounding. `cargo run &` would compile inside the background
  # job while the readiness loop is already counting, so any run whose build
  # exceeds the timeout fails here regardless of what the code does.
  cargo build --locked --quiet --bin timefusion
  TIMEFUSION_DATA_DIR="$data_dir" TIMEFUSION_ALLOW_INSECURE_AUTH=true PGWIRE_PASSWORD=postgres \
    cargo run --locked --quiet --bin timefusion >"$log" 2>&1 &
  pid=$!
  # shellcheck disable=SC2317
  # Guarded, and always returns 0. `cleanup` is a GLOBAL name (bash functions are
  # not scoped) while `pid`/`data_dir` are locals of this function, so a RETURN
  # trap firing anywhere they are unset makes `set -u` abort — which failed the
  # whole check on 2026-09-01 AFTER every smoke assertion had already passed,
  # reporting "line 332: pid: unbound variable" as the only symptom.
  cleanup() {
    local running="${pid:-}" dir="${data_dir:-}"
    [ -n "$running" ] && { kill "$running" 2>/dev/null || true; wait "$running" 2>/dev/null || true; }
    [ -n "$dir" ] && rm -rf "$dir"
    return 0
  }
  trap cleanup RETURN
  for _ in $(seq 1 60); do
    if docker run --rm --network host -e PGPASSWORD=postgres postgres:18.4 \
        psql "postgresql://postgres@127.0.0.1:$PGWIRE_PORT/postgres" -Atqc 'SELECT 1' >/dev/null 2>&1; then
      ready=true; break
    fi
    sleep 1
  done
  "$ready" || { cat "$log"; return 1; }
  for command in '\dt' '\d otel_logs_and_spans' '\l' '\du' '\dn'; do
    docker run --rm --network host -e PGPASSWORD=postgres postgres:18.4 \
      psql "postgresql://postgres@127.0.0.1:$PGWIRE_PORT/postgres" -v ON_ERROR_STOP=1 -c "$command" || return 1
  done
}

# ---------------------------------------------------------------- subcommands

selected_checks() { if [ "$#" -gt 0 ]; then printf '%s\n' "$@"; else checks_all; fi; }

cmd_fingerprint() { local c; for c in $(selected_checks "$@"); do printf '%s\t%s\n' "$c" "$(fingerprint "$c")"; done; }

cmd_gate() {
  local c ref out skip_all=true summary
  out=${GITHUB_OUTPUT:-/dev/null}
  summary=${GITHUB_STEP_SUMMARY:-/dev/null}
  # shellcheck disable=SC2046
  pin_fingerprints $(selected_checks "$@")
  # Hand the pinned values to the jobs that will do the work, so they attest the
  # exact fingerprints this gate just looked up.
  { echo 'fingerprints<<CI_FP_EOF'; cat .ci/fingerprints.tsv; echo CI_FP_EOF; } >> "$out"
  echo "| check | decision | attestation |" >> "$summary"
  echo "|---|---|---|" >> "$summary"
  for c in $(selected_checks "$@"); do
    if ref=$(find_attestation "$c"); then
      echo "$(printf '%s' "skip_$c" | tr '-' '_')=true" >> "$out"
      printf 'skip  %-18s %s\n' "$c" "$ref"
      echo "| \`$c\` | ⏭ skipped | \`$ref\` |" >> "$summary"
    else
      echo "$(printf '%s' "skip_$c" | tr '-' '_')=false" >> "$out"
      printf 'run   %-18s (fingerprint %s)\n' "$c" "$(fingerprint "$c")"
      echo "| \`$c\` | ▶ running | none for \`$(fingerprint "$c")\` |" >> "$summary"
      skip_all=false
    fi
  done
  echo "skip_all=$skip_all" >> "$out"
}

cmd_run() {
  local c req ref rc=0 unrunnable='' degraded=''
  CAPS=$(detect_caps)
  note "capabilities: ${CAPS:-none}"
  # shellcheck disable=SC2046
  [ -n "${CI_FINGERPRINTS:-}" ] || pin_fingerprints $(selected_checks "$@")
  for c in $(selected_checks "$@"); do
    req=$(check_requires "$c")
    # A missing capability is never a silent pass — this run can say nothing about
    # that check. CI_ALLOW_DEGRADED still runs it for the feedback (e.g. the suite
    # against the Postgres-as-TimeFusion fallback) but refuses to attest it, so a
    # weaker environment can never satisfy CI on a stronger one's behalf. Either
    # way the rest of the sweep continues and the exit code stays honest.
    if ! caps_satisfy "$CAPS" "$req"; then
      if [ "${CI_ALLOW_DEGRADED:-}" != "true" ]; then
        note "CANNOT RUN $c here — needs [$req], have [$CAPS]"
        unrunnable="$unrunnable $c"; rc=1; continue
      fi
      note "running $c DEGRADED — needs [$req], have [$CAPS]; result will not be attested"
      degraded="$degraded $c"
    elif [ "${CI_FORCE:-}" != "true" ] && ref=$(find_attestation "$c"); then
      note "$c already proven ($ref) — skipping (CI_FORCE=true to override)"
      continue
    fi
    note "running $c"
    if run_body "$c"; then
      # A green check stays green even if we cannot record it. Publishing touches
      # the network and the object store; neither is part of what the check proved.
      case " $degraded " in *" $c "*) ;; *) [ "${CI_NO_ATTEST:-}" = "true" ] \
        || record_result "$c" || note "could not record $c — it still passed, just isn't cached" ;; esac
    else
      rc=1
      note "FAILED $c"
      [ "${CI_KEEP_GOING:-}" = "true" ] || return 1
    fi
  done
  [ -z "$unrunnable" ] || note "not run here (CI will still have to):$unrunnable"
  [ -z "$degraded" ] || note "run degraded, NOT attested (CI will still run these):$degraded"
  return $rc
}

cmd_attest() { [ "$#" -ge 1 ] || die "attest needs a check name"; publish_attestation "$1"; }

cmd_gc() {
  local days cutoff ref d deleted=0
  days=${1:-30}
  cutoff=$(date -u -d "-$days days" +%Y%m%d 2>/dev/null || date -u -v-"$days"d +%Y%m%d)
  # Batched: one push per 200 refs, not one per ref — a year's worth of daily runs
  # is thousands of refs and a round trip each would take longer than the CI it saves.
  local batch=''
  for ref in $(remote_refs); do
    d=${ref##*/}
    case "$d" in [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) ;; *) continue ;; esac
    [ "$d" -lt "$cutoff" ] || continue
    batch="$batch :${ref}"
    deleted=$((deleted + 1))
    if [ "$((deleted % 200))" -eq 0 ]; then
      # shellcheck disable=SC2086
      git push -q "$REMOTE" $batch; batch=''
    fi
  done
  # shellcheck disable=SC2086
  [ -z "$batch" ] || git push -q "$REMOTE" $batch
  note "deleted $deleted attestation(s) older than $days days"
}

# ---------------------------------------------------------------- local services

COMPOSE_FILE=ci/compose.yml
compose() { docker compose -f "$COMPOSE_FILE" --project-name timefusion-ci "$@"; }

# Unlike a containerised runner, this runs the checks with YOUR toolchain — which
# is the faithful thing to do here, because CI does the same: ci.yml installs the
# channel from rust-toolchain.toml and runs cargo on the runner directly. So the
# only thing `local` has to provide is the MinIO that CI starts by hand.
cmd_local() {
  command -v docker >/dev/null 2>&1 || die "docker is required for the MinIO service"
  docker compose version >/dev/null 2>&1 || die "docker compose v2 is required"
  note "starting CI services (minio)…"
  compose up -d --wait minio
  # Buckets, the way CI seeds them. The bitnami image that auto-created them via
  # MINIO_DEFAULT_BUCKETS was withdrawn; this replaces it.
  compose run --rm mc >/dev/null 2>&1 || note "bucket seeding reported an error (they may already exist)"
  cmd_run "$@"
}

cmd_down() { compose down --remove-orphans; }

# ---------------------------------------------------------------- selftest

assert() { # <desc> <expected> <actual>
  if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1: expected [$2] got [$3]"; SELFTEST_RC=1; fi
}

cmd_selftest() {
  SELFTEST_RC=0

  assert "caps superset"      ok "$(caps_satisfy 'ghc pg minio tf-real' 'ghc pg' && echo ok)"
  assert "caps exact"         ok "$(caps_satisfy 'ghc' 'ghc' && echo ok)"
  assert "caps missing one"   no "$(caps_satisfy 'ghc pg minio' 'ghc pg minio tf-real' || echo no)"
  assert "caps empty require" ok "$(caps_satisfy '' '' && echo ok)"
  # 'tf' must not satisfy 'tf-real': substring matches would silently accept the
  # Postgres-as-TimeFusion fallback for a check that needs the real service.
  assert "caps no substring"  no "$(caps_satisfy 'ghc tf' 'tf-real' || echo no)"

  assert "url host:port"  'db 5433' "$(url_hostport postgresql://u:p@db:5433/x)"
  assert "url default pt" 'db 5432' "$(url_hostport postgresql://u:p@db/x)"
  assert "url no creds"   'h 9000'  "$(url_hostport http://h:9000)"

  assert "inputs expand" " src rustfmt.toml" "$(expand_inputs 'src rustfmt.toml')"
  assert "pathset expand nonempty" yes "$([ -n "$(expand_inputs '@rs')" ] && echo yes)"

  # Every check in the TSV must have a body and a fingerprint.
  local c seen=''
  for c in $(checks_all); do
    grep -q "^    $c)" "$0" || { echo "FAIL $c has no run_body case"; SELFTEST_RC=1; }
    seen="$seen $(fingerprint "$c")"
  done
  assert "fingerprints distinct" "$(echo $seen | tr ' ' '\n' | wc -l | tr -d ' ')" \
                                 "$(echo $seen | tr ' ' '\n' | sort -u | wc -l | tr -d ' ')"
  assert "fingerprint stable" "$(fingerprint fmt)" "$(WORKTREE_TREE=''; fingerprint fmt)"

  # A change under a check's inputs must move its fingerprint; one outside must not.
  # Probe with NEW files only — never edit-and-restore a tracked file, which would
  # discard whatever the developer has uncommitted in it.
  local before after probe
  probe=.ci-selftest-probe-$$
  before=$(fingerprint fmt)
  : > "src/$probe"; WORKTREE_TREE=''; after=$(fingerprint fmt); rm -f "src/$probe"
  assert "input change moves fp" changed "$([ "$before" != "$after" ] && echo changed)"

  # schemas/ is in @rs but deliberately not in @fmt — rustfmt never reads it.
  # (This probe used proto/, which left the tree with the gRPC ingest removal.)
  WORKTREE_TREE=''
  : > "schemas/$probe"; WORKTREE_TREE=''; after=$(fingerprint fmt); rm -f "schemas/$probe"
  assert "unrelated change keeps fp" same "$([ "$before" = "$after" ] && echo same)"

  # A typo in an input path is invisible — git silently matches nothing — and
  # silently narrows what the check depends on, which is how an untested change
  # ships. Every declared path must exist.
  local p missing=''
  for p in $(expand_inputs "$PATHSET_meta $(for c in $(checks_all); do check_inputs "$c"; echo; done | tr '\n' ' ')" | tr ' ' '\n' | sort -u); do
    [ -e "$p" ] || missing="$missing $p"
  done
  assert "every declared input path exists" "" "$missing"

  # Pinning is what survives a step that rewrites the tree mid-run.
  WORKTREE_TREE=''
  before=$(fingerprint fmt)
  pin_fingerprints fmt
  : > "src/$probe"; WORKTREE_TREE=''; after=$(fingerprint fmt); rm -f "src/$probe"
  unset CI_FINGERPRINTS; rm -rf .ci
  assert "pinned fp survives tree change" "$before" "$after"

  assert "ref roundtrip caps" 'ghc pg' \
    "$(printf '%s' "$(attest_ref x deadbeef 'ghc pg')" | cut -d/ -f7 | tr '.' ' ')"
  assert "ref roundtrip check" x "$(printf '%s' "$(attest_ref x deadbeef 'ghc')" | cut -d/ -f4)"
  assert "ref roundtrip fp" deadbeef "$(printf '%s' "$(attest_ref x deadbeef 'ghc')" | cut -d/ -f5)"

  [ "$SELFTEST_RC" -eq 0 ] && echo "selftest: all good"
  return $SELFTEST_RC
}

# ----------------------------------------------------------------------------

cmd=${1:-help}; shift || true
case "$cmd" in
  fingerprint) cmd_fingerprint "$@" ;;
  caps)        detect_caps; echo ;;
  gate)        cmd_gate "$@" ;;
  run)         cmd_run "$@" ;;
  attest)      cmd_attest "$@" ;;
  publish)     cmd_publish "$@" ;;
  local)       cmd_local "$@" ;;
  down)        cmd_down ;;
  gc)          cmd_gc "$@" ;;
  selftest)    cmd_selftest ;;
  checks)      checks_all ;;
  *)           sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//' ;;
esac
