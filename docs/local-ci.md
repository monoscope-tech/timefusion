# Running CI on your own machine

Our GitHub runners are 4 vCPU. Your laptop almost certainly isn't. And most of
what CI does on any given push it has already done — the PR that produced the
merge ran the same checks over the same bytes.

So CI now asks, before every check: **has anyone already proven this?** If yes it
skips. It does not matter who proved it — a previous run, a re-run, or you.

Nothing here is mandatory. Push and wait for CI as before, or run `make ci` and
watch CI have almost nothing left to do.

## The short version

```bash
make ci                      # everything CI runs
make ci CHECKS="fmt clippy"  # just these
make ci-status               # what CI would run right now, without running it
make ci-down                 # stop the MinIO service
```

`make prepush` is still the right thing for the fast inner loop. `make ci` is the
whole gate, and unlike `prepush` its results are **published**, so CI can reuse
them.

## How it works

`ci/checks.tsv` is the one definition of what CI is; both `.github/workflows/ci.yml`
and `make ci` read it. Each check declares:

- **inputs** — the paths whose content the result depends on. Every check also
  implicitly depends on `ci/checks.tsv`, `ci/compose.yml`, `scripts/ci/`, and
  `ci.yml`, so changing what a check *does* invalidates it everywhere.
- **requires** — the capabilities the environment must have for the result to
  mean anything (`rust`, `protoc`, `nextest`, `minio`, `docker`).

A check's **fingerprint** is a SHA-256 over the git blob hashes of its inputs. It
is computed from the *working* tree, not from `HEAD`, so you can run `make ci`
before committing and the attestation still matches the commit you make from it.

A passing check is published as an empty commit at

```
refs/ci-attest/v1/<check>/<fingerprint>/<platform>/<capabilities>/<date>
```

Everything the gate needs is in the ref name, so deciding costs one `ls-remote`
and no object fetches. They are not branches or tags, so they never appear in
either list and a normal `git fetch` ignores them.

The gate reuses an attestation only when the fingerprints match **and** the
recorded capabilities are a superset of what the check requires — an environment
that lacked MinIO can never satisfy a check that needs it.

Attestations are ordinary pushed refs, so producing one requires push access.
Fork PRs can't create them, and can't be affected by them.

## Why this is unusually faithful here

`rust-toolchain.toml` pins the channel and components, and `ci.yml` installs
exactly that. So `cargo` on your machine **is** the compiler CI uses — there is
no container to reproduce. That is why `make ci` runs the checks natively and
only supplies the one thing CI starts by hand: MinIO, from the same pinned image
with the same buckets (`ci/compose.yml`).

The lint definition lives in the `cargo lint` alias in `.cargo/config.toml`, so
the workflow, the autofix job and `make ci` all invoke the identical flags. There
is no second copy to drift.

Two differences worth knowing:

- **CI splits `test` across two runners** with `--partition hash:N/2`, purely for
  wall-clock. Locally you get the whole suite. A partitioned run proves only its
  slice, so CI records the check once, from a job that runs only after **both**
  shards are green — a half-passing run can never let a later run skip the suite.
- **`e2e` starts a MinIO container per test** via testcontainers, so it needs
  Docker and is capped at two threads. It is the slowest check by far; `make ci
  CHECKS="fmt clippy test"` is the usual pre-push sweep.

## macOS: root certificates

`make ci` exports `SSL_CERT_FILE=/etc/ssl/cert.pem` when it is unset and that
file exists. Without it the AWS SDK's rustls provider reads native roots from the
keychain, fails to parse any, and panics:

```
TrustStore configured to enable native roots but no valid root certificates parsed!
```

The panic comes from inside TLS setup, so the test that dies looks unrelated to
certificates — it surfaced as `variant_column` and `variant_functions` failing,
neither of which touches TLS. Linux CI is unaffected (native roots work there and
the bundle lives at a different path), so this only ever fires locally.

## One test is sensitive to a busy machine

`database::tests::test_batch_queue_under_load` has a 30-second internal deadline.
Run on its own it finishes in ~10s; run as part of the full local suite (918
tests at `num-cpus` threads) it can exceed 30s and fail, twice in a row, with

```
Error: Test timed out after 30 seconds
```

That is contention, not a regression — confirm with

```bash
cargo nextest run -E 'test(test_batch_queue_under_load)'
```

CI does not hit it today only because it splits the suite across two runners, so
each box carries half the load. That makes it latent there too: a slower runner
or a heavier suite would trip it. The durable fix is a deadline that scales with
available parallelism rather than a fixed 30s, but that is a change to the test
and is left to whoever owns it.

## Knobs

| Variable | Effect |
|---|---|
| `CHECKS="a b"` | restrict `make ci` / `make ci-status` to these checks |
| `CI_FORCE=true` | re-run even when an attestation already exists |
| `CI_KEEP_GOING=true` | don't stop the sweep at the first failure |
| `CI_ALLOW_DEGRADED=true` | run checks whose capabilities are missing, unattested |
| `CI_NO_ATTEST=true` | run, publish nothing |
| `CI_PARTITION=hash:1/2` | run one slice of `test` (what CI's shards use) |
| `TIMEFUSION_TEST_S3_ENDPOINT` | reuse a persistent MinIO instead of the compose one |
| `CI_ATTEST_DISABLED=true` | ignore all attestations — set as a repo variable to force full CI runs |

## When you need to invalidate everything

The fingerprint covers repository content, not the toolchain. If a check's
behaviour changes without its inputs changing — a new clippy release, say — two
escape hatches:

- bump `EPOCH` in `scripts/ci/ci.sh` — invalidates every attestation at once;
- set the repo variable `CI_ATTEST_DISABLED=true` — the gate ignores all
  attestations until you unset it, with no code change.

Note that `rust-toolchain.toml` **is** an input to every check, so a toolchain
bump already invalidates everything by itself.

## Changing a check

Edit `ci/checks.tsv` and the matching case in `run_body` (`scripts/ci/ci.sh`).
`make ci-selftest` checks the two stay in sync, along with the fingerprint and
capability logic, and that every declared input path exists — a typo there
silently narrows what a check depends on, which is how an untested change ships.

Narrow a check's `inputs` only where it is provably sound: a too-wide set costs a
rerun, a too-narrow one ships an untested change. `fmt` is the one clear case —
rustfmt reads `.rs` files and `rustfmt.toml`, not `Cargo.lock` and not `proto/`.
