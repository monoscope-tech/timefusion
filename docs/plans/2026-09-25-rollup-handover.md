# Timefusion rollup work: handover

## Status 2026-09-26 (supersedes the sections below where they disagree)

| Release | What | State |
| --- | --- | --- |
| 1 — resource safety | DataFusion 54 fork `be42f6a9` (spill-reader ownership, quota-rejection cleanup) + sketch memory accounting | Merged #322, deployed 2026-09-25 21:49 UTC. Rollback drill and constrained comparison passed; see `2026-09-25-first-rollup-deployment.md` |
| 2 — rollup pause policy + Stage 0 range coverage | Durable `ROLLUP PAUSE/RESUME/POLICIES`, range coverage, derived claims wait for queued parents, a running base unit supersedes the queued base units inside it | Merged #323, deployed 02:11 UTC. Caused OOM kills (compaction admission priced after file selection); **hotfix `96b093f5` deployed ~04:20 UTC**, memory back to 15–34 GB, PSI 0 |
| 3 — DataFusion 55.1 / Arrow 59.3 upgrade | All forks ported: DataFusion `2f17a238`; Delta on upstream `29db7587` + DV writers, append-tolerant conflicts, OPTIMIZE isolation downgrade, footer ordering through `DeltaScanExec`, row-ordinal selections, conjunct-split pushdown, scan metrics; datafusion-postgres fork; pgwire 0.41 re-vendored; datafusion-variant bumped; JSON 0.55.4; tracing 55.0.0 | Merged #324 (`8343dcd7`), deployed ~05:18 UTC (Dockerfile moved to Rust 1.98 for DataFusion's MSRV). Follow-up `8645be45` fixed `variant_get` returning an untyped NULL for missing paths (broke COALESCE over sparse `->` keys); deployed, no errors since |
| 4 — packed repairs on | `timefusion_rollup_packed_repairs` defaults to **true** (env `TIMEFUSION_ROLLUP_PACKED_REPAIRS=false` is the kill switch). Interrupted packed replacement: resume publishes once from the staged paths and queues no raw work. Restart recovery re-queues only slices ABOVE the repaired hour — the row witnesses (whole-date count, rows below a slice's end) do exactly that for unpacked slices too, so packing adds no restart work. datafusion-postgres pinned to `68fdc80` | See git log for the deploy commit |

Production rollup policy: `sessions_1h_v1` **paused** (no consumers; §8.3). `dashboard_1m_v3` was paused
during the incident and resumed from 2026-01-01.

Behaviour changes in release 3: `variant_get`/`->` on a missing key returns SQL NULL (PostgreSQL
jsonb semantics, upstream datafusion-variant); struct→JSON keys still render sorted; `regproc`
casts stay text (our datafusion-postgres fork drops the upstream regproc lookup rewrite, whose
unknown names nulled a non-nullable oid).

Fixed (`c9b4e809`): a `->` path step bound as an extended-protocol parameter plans through `variant_get_step`.

Still open from the full plan (not started or not activated): Stage 1B single-pass execution,
Stage 1C shared scans, Stage 1D dependencies, Stage 3 batched publication, Stage 4 flush
aggregation, Stage 5 dedup fusion, Stage 6; adaptive batches / certified-clean
stay off; packed repairs are ON (release 4). The fork's own DBeaver test still casts to regproc and
needs updating in the datafusion-postgres fork.

---


Prepared on 2026-09-25, finalized after 19:48 UTC. The user requested a handover and a pause, not abandonment or completion.

This document is the entry point for the next engineer. It separates verified current state from earlier recorded evidence and unfinished work.
The closing section records the final validation-process disposition. Read that section before starting a build.

## 1. Executive status

The full objective is to implement [Rollups on a fixed server](2026-09-24-rollups-on-a-fixed-server.md), without increasing server capacity.
That objective is not complete. No production deployment, application push, or final release attestation occurred in this work session.
The dependency upgrade is also incomplete. Do not describe the local upgrade candidates as production-ready forks.

The nearest deliverable is a small resource-safety release. It is deliberately separate from the larger coverage and execution redesign.
It corrects sketch-memory accounting, spill-reader ownership, and disk-charge cleanup after quota rejection.
These corrections enforce existing resource limits more accurately. They do not establish a CPU saving or a server-capacity multiplier.

Current release state:

| Item | State |
| --- | --- |
| Isolated Timefusion release branch | Three local commits, clean worktree |
| DataFusion 54 resource patch | Three files staged, not committed or pushed |
| Dependency regression tests | 114 selected execution and spill tests passed |
| Strict dependency Clippy | Passed on the final patch in 15m22s |
| Separate dependency lint suite | Every step except Rust documentation passed. Documentation stopped for the requested pause |
| Immutable dependency pin in Timefusion | Not updated |
| Combined release CI and image | Not built or signed off |
| Resource comparison and persisted-data rollback drill | Incomplete |
| Production rollout | Not performed |
| Full rollup implementation | Large unfinished candidate, mostly uncommitted |

Do not restart from the dirty main worktree or discard the temporary worktrees. Several important changes exist only there.

## 2. Read these files first

All relative paths in this section are inside the main Timefusion repository.

1. This handover, including the closing process-status section.
2. [First-deployment checklist](2026-09-25-first-rollup-deployment.md): release scope, safety gates, topology, and rollback requirements.
3. [Full rollup plan](2026-09-24-rollups-on-a-fixed-server.md): architecture, stage dependencies, experiments, and chronological implementation evidence.
4. [Dependency-upgrade plan](2026-09-25-dependency-upgrade.md): candidate versions, port inventory, API-reuse findings, and remaining tests.
5. [Spill patch and review](../../patches/datafusion/README.md): patch identity, reproduction, and scoped Rust review.
6. `AGENTS.md`, `docs/local-ci.md`, and `ci/checks.tsv` in the checkout where you will work.

The main plan is about 1,470 lines because it contains an implementation history. Later evidence can supersede earlier pending statements.
Use current Git state and final test logs to resolve discrepancies. Do not treat every old appendix statement as the current status.

The main plan, deployment checklist, and upgrade plan are currently untracked files in the main repository.
Copies also exist in the large rollup worktree. The main copies are the documentation entry point for handover.
The code candidates in the different worktrees are not interchangeable.

## 3. User requirements and working rules

The user explicitly requested the following:

- Reduce total CPU, memory, and I/O per useful unit of work. Do not increase server capacity.
- Prioritize proper deployments and fewer coherent releases. Do not wait for arbitrary multi-day signoff periods.
- Keep the broader rollup optimizations separate from the first resource-safety release.
- Upgrade owned dependency forks onto compatible upstream versions while preserving required fork behavior.
- Research newer APIs and remove redundant custom code only after proving contract equivalence.
- Use `rs-distill`, `rs-evasion-review`, and `rs-minimal-tests` before committing Rust work.
- Freeze release batches, reuse valid checks, batch dependency pins, and avoid competing Cargo processes.
- Preserve unrelated worktree changes. Do not reset or clean the working directories.

The requested workflow rules are in the main `AGENTS.md` and in release commit `beffeacc`.
The main `AGENTS.md` already contained a large uncommitted rewrite. The release commit includes only the requested ten-line addition to its original instructions.

Skill locations on this laptop:

```text
/Users/tonyalaribe/.codex/skills/rs-distill/SKILL.md
/Users/tonyalaribe/.codex/skills/rs-evasion-review/SKILL.md
/Users/tonyalaribe/.codex/skills/rs-minimal-tests/SKILL.md
/Users/tonyalaribe/.agents/skills/rust-skills/SKILL.md
/Users/tonyalaribe/.agents/skills/simple-english/SKILL.md
/Users/tonyalaribe/.agents/skills/investigate/SKILL.md
```

Read applicable skills yourself. `rs-distill` requires confirmation before applying discretionary proposals.
The user already approved the coverage output-evidence enum and the in-place generation-range merge. Both are in the large rollup candidate.
Do not ask for those approvals again. Optional spill-test fixture consolidation was only proposed and remains deferred.

Use `apply_patch` for manual edits. Use targeted `cargo nextest run` selections during iteration.
Use the repository's `cargo lint` alias for Timefusion. The DataFusion fork has its own pinned toolchain and contributor instructions.
Before pushing Timefusion, use `make ci-signoff`. Record commands, results, and outstanding checks in the PR.
Do not publish an attestation for a check that did not pass. Do not weaken fingerprints or required capabilities to avoid a check.

No additional agents are running for this work. Do not spawn agents unless the user or applicable instructions authorize delegation.

## 4. Directory map: what lives where

On this Mac, `/tmp` resolves to `/private/tmp`. Both spellings can identify the same worktree.
The temporary directories were used to isolate release work and incompatible dependency stacks from a dirty main checkout.
This prevented accidental inclusion of unrelated changes, but it left important uncommitted work outside the normal project directory.

### 4.1 Main repository and release worktrees

| Exact path | Purpose and authoritative state | Preservation requirement |
| --- | --- | --- |
| `/Users/tonyalaribe/Projects/apitoolkit/timefusion` | Main repository, branch `master`, HEAD `2f2df90c275089a4af85f77d3655d41d5bb541d8`. Dirty and not the release candidate | Preserve all tracked and untracked changes |
| `/tmp/timefusion-release.MSCafN` | Small release branch `codex/rollup-resource-release-20260925`, HEAD `beffeacc`. Clean at preparation | Continue first-release integration here |
| `/tmp/timefusion-rollups.XCvHu4` | Larger candidate, detached HEAD `d9f00ccea1ccc55e49a2ffc44cc45b7930e8cbb5` | About 5,992 additions and 883 deletions across 19 tracked files, plus untracked code and fixtures. Do not delete |
| `/Users/tonyalaribe/Projects/apitoolkit/timefusion/target` | Shared Cargo cache used by these worktrees and dependency candidates | Do not clean it casually. Do not run competing Cargo processes against it |

The two Timefusion worktrees are registered in the main repository's Git metadata. They are not standalone clones.
Copying only their `.git` files does not create portable repositories. The local release commits live in the shared main Git object store.

Release commits, in order:

| Commit | Content |
| --- | --- |
| `584565f0` | Local CI MinIO health probe compatible with the pinned image |
| `3040f5fd3012306e211dd971cc61e3863b7d4f7c` | Retained sketch-memory accounting and regression tests |
| `beffeacc` | Requested batched-validation rules in `AGENTS.md` |

The release is based on `2f2df90c275089a4af85f77d3655d41d5bb541d8`.
The last read-only remote check also found that revision at `origin/master`. Recheck before rebasing or deployment.
The Timefusion remote is `git@github.com:monoscope-tech/timefusion.git`.

Main-worktree tracked changes at handover preparation:

```text
AGENTS.md
src/config.rs
src/database/histogram.rs
src/database/maintain.rs
src/database/mod.rs
src/database/rollup.rs
src/database/scan.rs
src/database/tests.rs
src/maintenance_coordinator.rs
src/observability.rs
src/rollup.rs
src/write/mem_buffer.rs
```

Main untracked material includes the three plan documents, `patches/`, inventory scripts, and `scripts/__pycache__/`.
The Python bytecode cache is not source and must not enter a release commit.
Do not commit the entire dirty main worktree as a shortcut to packaging the release.

### 4.2 DataFusion 54 patch checkout

```text
/tmp/timefusion-datafusion.u4aBXK
```

- Base and current HEAD: `155b68e1bdb7b8eff460f2a0b102da62a4e6357f`.
- Branch: `codex/spill-reader-disk-accounting`.
- Staged files: `datafusion/execution/src/disk_manager.rs`, `datafusion/physical-plan/src/spill/mod.rs`, and `datafusion/physical-plan/src/spill/spill_pool.rs`.
- Staged diff: 249 additions and 15 deletions across those three files.
- No dependency commit or push yet.

Important remote trap:

```text
origin = /Users/tonyalaribe/.cargo/git/checkouts/datafusion-17a447484b4d3361/155b68e
fork   = https://github.com/tonyalaribe/datafusion.git
```

`origin` is a local Cargo-cache repository. Do not push the dependency release there.
Use the explicit `fork` remote after validation. The remote branch did not exist at the last check.
The fork branch `fix/correlated-unnest-table-factor` points to the exact base revision.

Untracked local support files in this checkout:

```text
timefusion-local-patch.toml
timefusion-local-paths.toml
timefusion-original.Cargo.lock
```

These are local testing aids, not intended commit content. The current working lockfile is also local test-graph evidence.
Inspect it rather than assuming it is the upstream lockfile. Do not blindly stage it.

Reproducible patch copies exist in both Timefusion worktrees:

```text
patches/datafusion/spill-reader-disk-accounting.patch
```

Current patch and staged-diff SHA-256:

```text
d7b1992034397d8037a7f8fae1926c4a2612b2394cf341e8718429d318a93306
```

The earlier `2fd44d9b...` hash covered the reader-only patch. It is superseded by the hash above.
Forward application against the unchanged base index and reverse application against the working candidate passed before staging.
After staging, use `git diff --cached --no-ext-diff` to inspect the patch. Plain `git diff` is now empty for those changes.

### 4.3 Dependency-upgrade workspace

Root:

```text
/tmp/timefusion-dependency-upgrade.VYaIwA
```

| Subdirectory | Branch / HEAD | Current purpose |
| --- | --- | --- |
| `datafusion` | `timefusion-upgrade-55.1` / `7d3835c71f30cbd3c3ae4041732267f1f453097a` | DataFusion 55.1 candidate. 19 tracked files changed, 552 additions / 164 deletions |
| `delta-rs-55` | `timefusion-upgrade-55` / `29db75879be8df185fae765cee4c801f344093c2` | Delta candidate on the compatible DataFusion 55 line. Eight files changed, 1,875 additions / 231 deletions |
| `delta-kernel-028` | `timefusion-kernel-028-audit` / `ab66589067127b81c87690370e9b2c6e5129bcdf` | Kernel 0.28 audit and ports. Five tracked files changed, including its lockfile |
| `datafusion-postgres-55` | `timefusion-upgrade-55` / `eda0da032ed8d6003b5041fce67c1e5b2f101876` | PostgreSQL ecosystem candidate. 16 tracked files changed, plus untracked deferred tests |
| `delta-rs` | `main` / `0ec5175f37ffc3b3482254e806574a1c5bb46a18` | Reference object database, not the active candidate |
| `delta-kernel` | `buoyant/main` / `8ba063f8f84fec222000f66d40d70911d7c79675` | Reference object database, not the active candidate |
| `datafusion-postgres` | `master` / `224e2440244ade09e8019a3cb3f190deee964510` | Reference object database, not the active candidate |

The reference clones have no populated source checkout. Git can report apparent staged deletions for a no-checkout clone.
Do not mistake that state for intentional source removal or try to repair it by resetting unrelated work.
The `*-55` and `delta-kernel-028` directories contain the implementation candidates.

The PostgreSQL candidate has an important untracked file:

```text
datafusion-postgres/src/handlers/deferred_tests.rs
```

The large Timefusion candidate has important untracked files:

```text
benches/rollup_work.rs
scripts/rollup_cpu_inventory.py
scripts/rollup_work_inventory.py
scripts/test_rollup_cpu_inventory.py
scripts/test_rollup_work_inventory.py
tests/fixtures/session_list_rollup.sql
```

Preserve these alongside tracked diffs. A Git patch alone does not include untracked files.

### 4.4 Logs, tooling, and diagnostic captures

| Exact path | What it contains |
| --- | --- |
| `/tmp/timefusion-spill-quota-lint-20260925.log` | Current final-patch strict Clippy and subsequent lint-suite log. Contains per-check/final exit markers |
| `/tmp/timefusion-spill-quota-tests-20260925.log` | Passing 114-test execution/spill run, final exit zero |
| `/tmp/timefusion-spill-lint-final-20260925.log` | Earlier run: cached Clippy passed, Hawkeye 7.0.1 rejected the version-6 configuration |
| `/tmp/timefusion-spill-lint-remainder-20260925.log` | CI-pinned license, spelling, and Markdown passes. Earlier documentation run intentionally stopped with exit 143 |
| `/tmp/timefusion-resource-release-pr.md` | Draft release PR text. Not submitted. Update pending fields and stale validation status before use |
| `/tmp/timefusion-hawkeye-620.zoLM9U/hawkeye-aarch64-apple-darwin/hawkeye` | Hawkeye 6.2.0 binary, matching the fork's CI workflow |
| `/tmp/timefusion-hawkeye-620.zoLM9U` | Downloaded release archive and SHA-256 file. Archive checksum passed |
| `/tmp/timefusion-cpu-current.UnqhvA` | Six existing production SVG profiles, `cpu-001166.svg` through `cpu-001171.svg` |
| `/tmp/timefusion-cpu-profiles.1IV8iD` | Earlier 57-profile production sample, `cpu-000508.svg` through `cpu-000564.svg` |

The CPU captures are diagnostic material, not source files. Keep them out of Git and public PR attachments until disclosure is reviewed.
They were copied from existing captures, not produced by enabling a new production profiling workload.

There are thousands of other `/tmp/timefusion-*` directories, including UUID-named test data and scenario-prefixed fixtures.
Do not enumerate that entire glob into a terminal or delete it as a cleanup shortcut. It produces enormous output and mixes unrelated work.
This handover identifies the active task directories, not every temporary artifact on the machine.
Their presence is not evidence that this task owns them or that they are safe to delete.

`git worktree list` also shows many older Timefusion worktrees under `/Users/tonyalaribe/Projects/apitoolkit` and `.claude/worktrees`.
They are not substitutes for the three active Timefusion paths in this handover. Leave them alone unless their owner requests a change.

## 5. Exactly what the first release changes

### 5.1 Sketch memory

Release commit `3040f5fd` changes `src/read/functions.rs`.
`Sketch::heap_size` includes inline storage and retained heap capacity.
`SketchAccumulator::size` adds accumulator overhead without counting the sketch value twice.

`TDigestWrapper` now contains an optional `AccountedDigest`, which stores the digest and its retained centroid capacity.
The pinned `tdigests` 1.0.1 compressor allocates space for 200 centroids when compression runs, even if fewer centroids remain live.
The old code measured only live centroid length and therefore underreported retained memory.
The wrapper records capacity before compression and uses the known replacement allocation when compression occurs.

Small digests retain small charges. Merging replaces the obsolete allocation charge.
The serialized representation remains a bincode vector of `(mean, weight)` pairs.
HLL and t-digest accumulator storage tests cover the accumulator's own inline bytes.

Upgrade caution: this capacity accounting depends on the pinned compressor's allocation behavior.
Recheck it during a `tdigests` upgrade. Do not assume a future implementation retains the same capacity policy.

### 5.2 Spill-reader ownership

The DataFusion 54 reader now moves a boxed `(reader, tracked_file_owner)` pair through the existing stream states.
It allocates once per file, not once per batch. The tuple stores the reader first so the reader drops before the tracked owner.
The old code could drop the tracked owner while an open reader still held unread file data.
That released the disk charge too early and unlinked the file while the reader continued consuming it.

Regression cases cover partial cancellation, EOF, schema-mismatch errors, cancellation before a blocking read starts, and cancellation during an active read.
The active-read case uses a real FIFO and IPC bytes. It requires Unix and the `mkfifo` utility.
Portable cases remain present. The Unix case does not prove Windows cancellation behavior.

The spill-pool test fixture also changes its rotation threshold from `batch_size` to `batch_size - 1`.
The implementation rotates only when size is strictly greater than the threshold.
The previous fixture assumed one batch per file while actually allowing two. The corrected fixture retains partial-release and final-cleanup assertions.

### 5.3 Quota-error cleanup

The required resource gate found an additional existing DataFusion 54 bug.
`RefCountedTempFile::update_disk_usage` changed the global charge before checking the quota, but changed the file charge only on success.
After rejection, cleanup subtracted the old file charge and left the rejected growth charged globally.
Repeated rejected checks amplified the retained charge and could prevent later valid spills.

The regression failed with 10 bytes charged after two rejected checks and file deletion.
The fix records the file charge before returning the quota error. It changes no configured limit or public error type.
The case table covers new and previously charged files, repeated rejection, file deletion, and a subsequent successful spill.

DataFusion 55 has a different writer API. `FileSpillWriter::write` reserves bytes before writing and reverses its reservation on quota rejection.
Adapt the regression to that API. Do not mechanically port the version-54 update method.
This comparison does not establish correctness for every physical write failure or concurrent writer operation.

### 5.4 Local MinIO health probe

The pinned MinIO image has Bash but lacks the curl/wget/mc tools assumed by the old probe.
The release uses Bash TCP support to check for HTTP 200, with correct Compose dollar escaping.
The invalid-route check failed as expected, the container became healthy, and the main test suite passed afterward.
This changes local CI plumbing, not production configuration.

## 6. Validation ledger and what can be reused

### 6.1 Final DataFusion 54 patch

| Check | Evidence | Scope / limitation |
| --- | --- | --- |
| Quota regression before fix | Run `38b9e176-4af7-45e0-b1a7-0ad5667cbce5`, failed at retained-charge assertion | 44.41s build, expected red result |
| Execution plus spill tests after fix | Run `c1904d77-96e7-4168-8e20-2cd3feb19bf2`: 114 passed, 1,396 excluded | 45.43s build, 1.257s execution |
| Formatting and whitespace | Passed after final source edit | Patch hash recorded above |
| Strict all-target/all-feature Clippy | `CHECK_EXIT strict_clippy=0`, 15m22s | Final three-file patch, Rust 1.95.0 |
| Separate suite Clippy | Passed in 5m02s on the final patch | Different feature set from strict Clippy |
| TOML, license, spelling, Markdown | Passed on the final patch | CI-pinned license tool used |
| Complete `dev/rust_lint.sh` | Incomplete: documentation intentionally stopped | Final wrapper exit 143, not a source failure |

The test command was:

```sh
cd /tmp/timefusion-datafusion.u4aBXK
CARGO_TARGET_DIR=/Users/tonyalaribe/Projects/apitoolkit/timefusion/target \
cargo +1.98 nextest run --locked --config 'profile.dev.debug="line-tables-only"' \
  -p datafusion-execution -p datafusion-physical-plan --lib --no-fail-fast \
  -E 'package(datafusion-execution) | test(spill::)'
```

The strict command was:

```sh
cargo +1.95.0 clippy --all-targets --all-features --locked -- -D warnings
```

The subsequent `dev/rust_lint.sh` runs formatting, a separate Clippy configuration, TOML formatting, license headers, spelling, Markdown formatting, and Rust documentation.
Its Clippy command uses `--all-targets --workspace --features avro,integration-tests,extended_tests -- -D warnings`.
Its documentation command is `cargo doc --document-private-items --no-deps --workspace` with `RUSTDOCFLAGS=-D warnings`.

The fork pins Rust 1.95.0. Earlier Rust 1.98 lint failures do not replace the correct-toolchain result.
Hawkeye must be version 6.2.0 for this configuration. The installed global 7.0.1 failed to parse `licenserc.toml`.
Use the isolated downloaded binary via PATH. Do not rewrite the fork's license configuration to accommodate the wrong tool.

### 6.2 Timefusion release source, before the new dependency pin

These are earlier recorded results for the small release source. They do not establish validation of the future combined artifact.

| Check | Recorded result |
| --- | --- |
| Function tests | 57 passed, run `9d1a2704-a002-4ebe-b4c8-f928b90ba7a3` |
| Formatting and diff whitespace | Passed |
| `cargo lint` | Passed in 1m57s |
| Main local CI test selection | 2,074 passed, 16 excluded, 24 slow, run `75b36d22-5330-469e-bc0d-d83201a8a4e1` |
| Doctests | 16 passed |
| PostgreSQL smoke | PostgreSQL 18.4 client readiness, SELECT, and catalog commands passed |
| E2E | 65 passed, two excluded, one flaky test, run `9cb3e693-20f3-4824-a145-73f851f06bad` |
| Source-checked no-retry ordering rerun | Passed, run `5921aee9-8527-4c19-9a87-6f14fea60c50` |
| CI self-test | Passed |
| Deployment harness tests | Prepare: 3, lease: 13, run: 15, image helper: 2 passed |

The E2E test was `ordering_pushdown::one_unsorted_file_does_not_cost_the_majority_its_ordering`.
Its first attempt failed and its retry passed. Six direct cached-binary runs also passed, but those are diagnostic evidence only.
The later source-checked no-retry command passed in 6.340s. The original failure remains unexplained, not fixed by assertion.

The main test run used `CI_NO_ATTEST=true`. No final passing attestation was published for this release.
Deployment-harness tests use local fixtures. They are not an actual production deployment or persisted-data rollback drill.

### 6.3 Cache and scheduling pitfalls

Timefusion uses Rust 1.98. The old DataFusion fork uses Rust 1.95.0.
The shared target directory contains artifacts from multiple worktrees, feature sets, and toolchains.
An earlier large-candidate run reused the wrong Timefusion test binary. Its apparent pass was rejected as candidate evidence.
A later source-checked rebuild passed the intended selection. Preserve source provenance, not just test counts.

Do not run a cached binary directly and call that final release validation.
Do not launch several Cargo commands that wait on the same target lock.
Do not run `cargo clean` to resolve uncertainty without first understanding the source/configuration mismatch.
Check logs and active processes before restarting. A quiet compiler is not a stopped compiler.

The laptop has 10 logical CPUs and 32 GiB RAM. Recent observations showed heavy existing swap use and about 52 GiB free disk space.
Other applications and builds exist on the laptop. Do not terminate unrelated processes to accelerate this task.

## 7. Exact next steps for the small release

1. Read the closing process-status section and the final lint log.
2. Preserve the worktrees and untracked source before any temporary-directory cleanup or machine transfer.
3. Complete only the unresolved dependency checks. Reuse the final-patch strict-Clippy and 114-test results while their inputs remain unchanged.
4. Recheck the staged three-file diff, its checksum, and the scoped Rust review.
5. Commit only those three dependency files after required checks pass.
6. Push the new branch explicitly to `fork`, not `origin`. Record the immutable commit SHA.
7. In `/tmp/timefusion-release.MSCafN`, replace all 33 DataFusion patch references together with that SHA.
8. Regenerate the lockfile. Review dependency changes and confirm one coherent DataFusion family.
9. Keep the dependency pin in a separate Timefusion commit. Do not copy temporary local path overrides into the release.
10. Freeze the combined artifact and run one integrated `make ci-signoff`.
11. Complete representative constrained-resource comparisons, persisted-data rollback, and restart/flush-handoff checks on the actual release artifact.
12. Update the PR draft with final command results, limitations, source identities, and artifact digest.
13. Follow the existing deployment mechanism only after the deployment scope and gates are satisfied.

Inspect the references with:

```sh
cd /tmp/timefusion-release.MSCafN
rg -n '155b68e1|tonyalaribe/datafusion' Cargo.toml Cargo.lock
git status --short
```

The current PostgreSQL fork reference is still a branch in the manifest, with its resolved commit in the lockfile.
Do not accidentally advance unrelated branch dependencies while regenerating the DataFusion pin.
The broader upgrade must eventually use reviewed compatible revisions, not moving branches or local paths.

Before integrated CI, inspect `ci/checks.tsv` and `docs/local-ci.md` in the release worktree.
Local MinIO was healthy at port 9000. Other Monoscope development containers also exist and are not owned by this handover.
Use explicit local test endpoints. Do not load `.env.prod` for local tests.

Example final validation shape, after the pin and lockfile are stable:

```sh
cd /tmp/timefusion-release.MSCafN
CARGO_TARGET_DIR=/Users/tonyalaribe/Projects/apitoolkit/timefusion/target \
TIMEFUSION_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
make ci-signoff
```

Confirm the local test credentials and bucket requirements from repository configuration before use.
Do not set `CI_FORCE=true` unless a recorded reason invalidates an otherwise matching result.
Do not set `CI_NO_ATTEST=true` for final signoff if GitHub must reuse the results.

`make ci-signoff` can publish an immutable candidate image and CI attestations. It does not itself update production's `latest` image or restart the service.
A master push can trigger deployment automation. Do not treat a master push as a harmless intermediate checkpoint.
The existing deployment runner requires current merged master, an immutable image digest, and its deployment lease.

## 8. Full rollup implementation: what exists and what remains

The larger candidate is `/tmp/timefusion-rollups.XCvHu4`, not the small release branch.
The full plan remains the requirements source. The following is a takeover map, not a completion claim.

| Area | Implemented or investigated | Remaining gate / work |
| --- | --- | --- |
| Work attribution | Existing CPU profiles and publication/shard inventory analyzed | Reconcile per-lane/spec CPU with whole-process CPU and representative workload |
| Session suspension | Durable pause/resume policy, journal persistence, admission guards, planner filtering, authenticated pgwire controls | Consumer inventory, activation decision, production acceptance, rollback compatibility |
| Byte-aware batches | Candidate uses existing batch sizing mechanism, benchmark exists | Did not meet the proposed 10% CPU gate. Keep disabled |
| Stage 0 coverage | Shared proof work across census, routing, no-op decisions, tickets, output evidence, recovery | Complete hybrid visibility/race and packed-repair correctness gates before activation |
| Historical coverage | Range-based evidence, approved in-place merge, empty/file-backed/unknown output states | Remaining dirty-fragment proof and restart cases |
| Certified-clean path | Candidate and source/deletion-vector regressions | Keep disabled until producer, publication, replay, and consumer evidence agree |
| Single-pass execution | Compact sort-input candidate and mechanism benchmarks | Final-group memory bound, real I/O/RSS measurements, oversized work, full acceptance |
| Discovery scheduling | Bounded historical discovery and deadline tests | Fairness, lock hold time, cancellation, and metadata-memory limits |
| Publication/checkpoints | Partial checkpoint-sharing work and I/O outside journal lock | Full batched publication queue is not implemented |
| Logical revisions | Source epochs and recovery tests | No proven pinned cross-Delta/MemBuffer source-view protocol |
| Flush aggregation | Design only / incomplete | Must not add independent synchronous commit pressure or endanger flush |
| Dedup fusion | Design and prerequisites | Avoid dependence on a starved dedup lane. Prove bounded resources and net saving |
| Fine-grained tracking | Conditional design | Minute masks require measured value over hour masks |
| Hierarchy / HLL removal | Design and inventory | Schema compatibility, useful consumers, independent acceptance |

Important implementation locations:

- `src/database/maintain.rs`: the largest candidate diff. Coverage recovery, planning, repair, and execution paths.
- `src/maintenance_coordinator.rs`: durable policy, ranges, admission, task state, and journal work.
- `src/database/rollup.rs`: maintenance integration, compact-sort input, and resource tests.
- `src/rollup.rs`: routing and coverage consumers.
- `src/write/mem_buffer.rs`: source visibility and exclusion behavior.
- `src/server/mod.rs`: authenticated administrative policy controls.
- `src/read/optimizers.rs`: PostgreSQL array-literal compatibility.
- `src/database/tests.rs`: substantial real-storage regression coverage.
- `benches/rollup_work.rs`: local ABBA/BAAB mechanism experiments.
- `tests/fixtures/session_list_rollup.sql`: actual client-query-shape routing fixture.

### 8.1 Coverage design decisions to preserve

Do not build a parallel coverage system without mapping the existing day, slice, epoch, and witness machinery.
The relevant pieces include `rollup_coverage`, `rollup_slice_coverage`, `rollup_source_epochs`, and bounded witnesses.
Coverage consumers previously disagreed. A replacement proof must serve census, skip decisions, `rollup_ticket_current`, and routing consistently.

A day can be restored from slices only when all hours are valid or explicitly empty, and no hour is dirty.
Absence of a slice is not proof of an empty hour.
Stage 0 deliberately revisits the earlier no-day-coverage/no-skip behavior. Preserve the explicit conditions and regression evidence.

Advancing the frontier must process only the new interval. Historical work needs an explicit hole or relevant mutation.
Daily Parquet partitioning must not force a full-day logical refresh.
One timestamp cannot represent historical holes. One dirty flag for an entire day is too broad.
Historical backfill is lower priority and must not delay keeping recent data current.

The approved metadata direction reuses WAL/commit records, publication metadata, compact intervals, bounded in-memory exception maps, coalesced work, and batched checkpoints.
It does not introduce synchronous per-row ledger commits or keep every retained project-minute resident.
Existing historical source data does not require a bulk rewrite into a minute format.

Physical fingerprints and witnesses remain an audit/backstop when freshness becomes logical.
Do not remove the mechanism that detects forgotten invalidation without replacing its correctness protection.

### 8.2 Specific unfinished correctness findings

- Two packed-repair cases completed successfully, but two interrupted/dirty-fragment cases remained red in the implementation record.
- Nineteen source-epoch recovery tests passed, but this is not a pinned cross-table/MemBuffer snapshot proof.
- The certified-clean path had deletion-vector and publication-race findings. Some later selections passed, but logical-only activation is not justified.
- Journal policy records and snapshots use version 2. An older binary cannot read them.
- Binary rollback must use a compatible reader or a tested migration. Deleting policy metadata can resume unwanted historical work and is not safe.
- A single oversized bucket must either make progress within an explicit mechanism or report a capacity limit without spinning.
- The single-pass final-group collection still needs a demonstrated memory bound. Source-scan savings do not prove bounded peak RSS.

Earlier recorded policy/control evidence includes 212 selected tests and lint passes after a source-provenance correction.
Backfill and admission tests found and corrected enqueue-only checks, paused eligibility, and failed-persistence admission bugs.
Those results do not establish safe production activation or rollback.

### 8.3 Session consumer finding

The historical schema comment about `max(concat(...url))` is stale relative to the inspected client SQL.
The inspected Monoscope source revision was `f6699ef3d` and matched the then-observed application image tag.
Its session list uses a fallback session key, ordered `ARRAY_AGG`, trace distinct counts, and an end-time expression.

Six component routing probes passed. A simple session-ID count could route, while the other inspected shapes did not.
The full client fixture plans successfully and receives no rollup route after the array-literal compatibility fix.
This proves eligibility for that fixed query shape, not absence of direct readers or infrequent consumers.
Do not pause `sessions_1h_v1` solely because the intended client query is currently unroutable.
The inventory still needs actual consumers, direct table reads, and an appropriate usage horizon.

### 8.4 Performance evidence is not a capacity promise

Synthetic single-pass experiments showed roughly 65–76% CPU savings and 87.5% lower source-read estimates in selected cases.
Those numbers do not measure whole-server savings. Intermediate I/O can increase, and some memory bounds remain unresolved.
The byte-aware batch candidate failed its proposed 10% CPU gate. Do not promote it because it is a small code change.

The plan requires matched workload, accepted traffic, query mix, backlog, and deferred work.
For numerical gates, it specifies counterbalanced ABBA and BAAB blocks and confidence bounds.
A saturated server can remain busy after an optimization. Measure useful throughput and CPU-seconds per accepted event, not utilization alone.
Do not count a growing maintenance backlog as resource savings.

## 9. Dependency upgrade: candidate inventory and next work

The intended compatible family is DataFusion 55.1 and Arrow/Parquet 59, with matching Delta, kernel, PostgreSQL, JSON, tracing, and Variant support.
Do not independently upgrade packages across incompatible Arrow/DataFusion families.
No final Timefusion manifest or lockfile exists for the complete upgraded stack.

### 9.1 DataFusion 55.1

The candidate ports existing fork behavior for output metrics, positional Parquet reads, sort pushdown, shared sort-buffer accounting, UPDATE FROM, DEALLOCATE ALL, and correlated UNNEST.
Earlier focused selections passed for these areas: metrics 75, positional reads 25, sort pushdown 136, deallocation 33, shared memory 102, and UNNEST 31.
These are historical focused results, not one combined stack signoff.

Upstream 55 changed the spill implementation to an asynchronous decoder and pluggable spill-file API.
The local 55 patch corrected retention after EOF, not the version-54 premature owner-drop defect.
Its earlier 45 spill tests passed. The later version-54 queued/active cancellation tests are not automatically ported or proven on 55.
Adapt behavior tests to the actual new implementation. Reuse upstream memory counters and writer APIs where they satisfy the contract.

### 9.2 Delta candidate

The broadest recorded candidate run passed 1,439 tests, with nine excluded, in 56.270s after a 2m27s build.
Run ID: `5d7c5efb-5826-43be-90a9-59e6d1f2fb2f`.
The selection covered `deltalake-core` library tests, `it_datafusion`, and Variant with the DataFusion feature.

Implemented or partly ported:

- Physical deletion-vector row ordinals and immutable bitmap sharing.
- Selected-file scan configuration through upstream `SelectedFileScanFactory`.
- SortBy/SortByDedup work and selected-path regressions.
- Optimize writer caps and file-convergence tests.
- Cancellation-aware tasks and a typed read timeout.
- Snapshot advancement with materialized metadata, append/remove processing, and fallback for metadata/checkpoint boundaries.
- Tag preservation and real deletion-vector lifecycle cases.
- Variant behavior that allows some old normalization patches to be omitted.

Still required:

- Deletion-vector write APIs for DELETE, UPDATE, and joined updates.
- Merge/deletion-vector behavior, conflict detection, snapshot isolation, and live-DV vacuum protection.
- Non-empty row-tracking fixtures, not only default or absent values.
- Application-level scan ordering, Parquet access-plan behavior, and statistics API integration.
- Full Timefusion integration and resource measurements.

The snapshot fast path remains a performance hypothesis until compared with upstream behavior on the same histories.
Upstream avoids rereading old logs but still transforms cached file entries through replay. Cached I/O is not zero CPU work.
Do not delete the custom path based only on the existence of a newer API. Do not retain it based only on passing correctness tests either.

### 9.3 Kernel candidate

The candidate includes schema-related caching and output-schema work. Earlier focused groups of eight and 250 tests passed for relevant areas.
The upstream executor already avoids an old nested blocking-task pattern. That old patch need not be recreated.

API-reuse proposals remain proposals unless explicitly implemented and verified:

- Use public `DeletionVectorDescriptor::absolute_path` with a correct object-store path adapter.
- `relative_path` is crate-private. Do not plan around calling it externally.
- Kernel DV reads check version, size, magic, and CRC, but async boundaries and malformed-input behavior need regressions.
- `DeletionVectorWriteResult::to_descriptor` and iterator-based deleted-row input can replace some manual construction.
- `LogicalFileView::remove_action` does not preserve the row-tracking fields required by the existing helper. It is not an equivalent replacement.

### 9.4 PostgreSQL candidate and remaining ecosystem

The PostgreSQL candidate includes deferred DML, listener behavior, cursor/hooks, binary UUID/JSON encoding, and metadata work.
Earlier focused groups passed: 32 deferred-DML cases, 34 listener cases, 68 encoding cases, and 120 metadata cases.
Catalog semantics, full client compatibility, and full-stack integration remain open.
Preserve the untracked deferred-test source when transferring this checkout.

Recorded compatible release candidates include JSON 0.55.4 and tracing 55.0.0.
The Variant dependency still needs a compatible DataFusion 55 / Arrow 59 port. Its then-current upstream candidate remained on DataFusion 54.
Recheck upstream release state before finalizing pins. The upgrade document contains the inspected references and existing fork revisions.

## 10. Production access and measured findings

The user explicitly authorized SSH access as:

```sh
ssh ubuntu@captain.s.past3.tech
```

The account has sudo access. That authorization covered reading or enabling CPU profiles for this task.
The latest diagnostic work only read existing profiles and runtime state. It did not enable profiling, change permissions, alter configuration, or deploy code.
Do not include credentials or environment dumps in the handover or PR.

Monoscope CLI authentication worked, but service discovery returned HTTP 500.
The last `monoscope services list --since 1h` attempt still failed. The authorized SSH path was used instead.
Do not repeatedly retry broken discovery as the only way to continue the investigation.

Profile location on the server:

```text
/home/ubuntu/timefusion-data/profiles
```

Only about ten captures are retained there, so older files can disappear quickly. Local exported copies are important.
Use the inventory script in the large rollup checkout to analyze the SVG captures.

Example read-only analysis of the existing local files:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 \
  /tmp/timefusion-rollups.XCvHu4/scripts/rollup_cpu_inventory.py \
  /tmp/timefusion-cpu-current.UnqhvA/*.svg
```

### 10.1 Last recorded production snapshot

At 18:23 UTC on 2026-09-25:

- Host: 48 logical CPUs, load averages 68.88 / 59.99 / 63.59.
- Two short vmstat samples: 10–12% idle, zero reported I/O wait.
- Timefusion container: approximately 33.8 cores used, with a 44-core quota.
- Container memory: 51.91 GiB against 120 GiB. Host available memory was approximately 99.9 GiB.
- Service: `srv-captain--timefusion`, one replica, stop-first updates.
- Observed container: `a45370c324c2`, healthy, started 2026-09-24 at 21:28:57 UTC.

Observed image:

```text
ghcr.io/monoscope-tech/timefusion@sha256:84504e8dc915bca8d8ff1e00102c1a64f246165b713d26033c55e7ad3989387b
```

Between 18:25:31 and 18:26:31 UTC, cgroup CPU usage increased by 1,707.576 CPU-seconds: approximately 28.46 cores on average.
CPU-throttling counters and memory-limit/OOM event counters did not increase during that interval.
The quota was therefore not the immediate constraint in that minute. This is not per-maintenance-lane attribution.
Docker block-I/O totals were cumulative and cannot be read as current throughput.

The observed cgroup path was:

```text
/sys/fs/cgroup/system.slice/docker-a45370c324c20036509cae9211e2fb6b5da56f87bf13297f6a6e50c4d2160f8b.scope
```

Re-resolve the container, image, service, and cgroup before any new measurement or deployment. These identifiers become stale after a restart.

### 10.2 Profile interpretation

The six evening profiles cover approximately 18:21:54–18:27:23 UTC and contain 277,520 samples.
The analyzer attributed about 92.0% to maintenance threads, 46.1% to sort operators, 37.0% to sort batch assembly, and 20.8% to aggregates.
Rollup callers appeared in 16.4%, dedup callers in 7.4%, Parquet in 8.5%, and partition statistics in 1.9%.
These categories overlap. Do not add them or treat them as disjoint CPU-seconds.

The earlier 57-profile sample spans about 06:35–07:35 UTC and contains 2,562,127 samples.
Maintenance threads represented 76.88%, sort operators 38.45%, batch assembly 32.27%, aggregates 29.76%, rollup callers 19.24%, and dedup callers 11.91%.
Again, these are overlapping sampled categories, not a complete per-tier cost allocation.

An earlier publication inventory recorded 471 publications and 192.923 GB of estimated input.
The shard-weighted estimate was 512.486 GB, or 2.6564 times the unweighted estimate.
Sessions represented 236 publications and 82.28% of estimated input, but that is not their CPU share or proof of uselessness.
These findings justify investigating repeated scans and unused materialization. They do not establish a production capacity multiplier.

## 11. Deployment and rollback boundaries

There is no spare isolated replica in the observed topology. A process-wide dependency change affects the entire service.
Do not call a project-scoped switch an isolated canary for a process-wide patch.
Resolve the service-wide handoff scope before activation. The user requested deployment, but materially different blast radius still needs explicit direction.

The existing `scripts/deploy/run.py` mechanism requires merged current master, an immutable image digest, and the deployment lease.
Recorded limits include 30 seconds of client-visible unavailability, a 120-second soak, and probes every two seconds.
Recovery discovery has a 720-second deadline. A confirmed drained handoff also requires WAL recovery within 5,000 milliseconds.
Read the actual scripts again before use. These numbers do not replace resource and rollback gates.

The captured image is a rollback reference, not proof that rollback works with data written by the candidate.
Run the persisted-data/WAL rollback drill before deployment. Do not delete data or metadata to make the old binary start.
Keep optional optimizations off and existing resource limits unchanged for the resource-safety release.

Stop conditions in the deployment checklist include correctness failures, OOM, scratch-limit violations, stalled flush, increasing backlog, and sustained latency/resource-error regressions.
Compare matched input and query mix. Do not interpret a quieter workload as an optimization.
The next operator must own deployment observation and rollback decisions, not only the build command.

## 12. Preserve and transfer the work safely

This guide is a map, not a portable backup of the implementation. A Git clone of remote master will not contain most of the unfinished work.
Do not reboot, remove temporary directories, or run broad cleanup until the important candidates are preserved.

Minimum preservation set:

1. The main repository and its Git object store, including local release commits and worktree metadata.
2. The large rollup worktree, including its untracked scripts, fixture, benchmark, plans, and patch artifacts.
3. The staged DataFusion 54 patch and its base identity. The repository patch file provides a reproducible source backup for this diff.
4. The four active dependency-upgrade candidate directories, including untracked test files and relevant lockfiles.
5. The final validation logs and draft PR text.
6. Restricted CPU captures in an approved local/private evidence location, not a public source repository.

For each repository, record `git status --short`, `git rev-parse HEAD`, branch, remotes, tracked diffs, and untracked-file inventory.
A plain `git diff` misses staged changes. A patch misses untracked files. A branch push misses uncommitted changes.
Use Git bundles or another approved backup for unpublished commits when transferring to another machine.
Keep generated build caches separate from source backups. Do not include credentials or raw customer data by default.

For registered worktrees, use `git worktree move` if relocation is approved. Do not use a plain directory move without repairing Git metadata.
Do not move a worktree while its build is active. Dependency candidates can also contain path references that require review after relocation.
No directory was moved or deleted as part of this handover.

Useful read-only inventory commands:

```sh
git -C /Users/tonyalaribe/Projects/apitoolkit/timefusion worktree list
git -C /tmp/timefusion-release.MSCafN status --short --branch
git -C /tmp/timefusion-rollups.XCvHu4 status --short
git -C /tmp/timefusion-datafusion.u4aBXK diff --cached --stat
git -C /tmp/timefusion-datafusion.u4aBXK diff --cached --no-ext-diff | shasum -a 256
```

Avoid broad `git add -A`, `git reset --hard`, `git clean`, or recursive deletion in these directories.
The main worktree contains user-owned changes whose provenance is not fully attributable to this task.

## 13. First message for the successor

Suggested takeover brief:

> Read `docs/plans/2026-09-25-rollup-handover.md` and inspect current Git/process state before changing anything. Preserve the temporary worktrees. Continue the frozen DataFusion 54 resource patch through its unresolved checks, publish an immutable fork revision, pin the 33 Timefusion references together in the isolated release branch, and perform one combined release validation. Keep broader rollup changes separate from this first resource-safety release, but retain the full implementation and dependency-upgrade objectives. Do not repeat checks whose exact inputs remain valid. Do not deploy before resource, rollback, and service-wide handoff gates are satisfied.

## 14. Closing process status

At 19:48 UTC, the assistant stopped only the unfinished Rust documentation process to honor the user's pause request.
The exact process was PID 57690, checked immediately before termination. It ran `cargo doc --document-private-items --no-deps --workspace`.
The wrapper finished with `VALIDATION_EXIT=143`. This was an intentional stop, not a compiler or test failure.
Tool session `15637` is now terminal. A process inspection found no remaining wrapper, Cargo documentation, or matching rustdoc process.
No new build, commit, push, image publication, or deployment will start after handover.

The durable log is `/tmp/timefusion-spill-quota-lint-20260925.log`. Its evidence is:

- Strict all-target/all-feature Clippy passed in 15m22s: `CHECK_EXIT strict_clippy=0`.
- Formatting passed.
- The separate suite Clippy configuration passed in 5m02s.
- TOML formatting, Hawkeye 6.2.0 license headers, spelling, and Markdown formatting passed.
- Rust documentation did not finish. This is the only remaining step in that dependency lint suite for the unchanged patch.

Resume only the unresolved documentation step if the staged patch and validation inputs are unchanged:

```sh
cd /tmp/timefusion-datafusion.u4aBXK
git diff --cached --no-ext-diff | shasum -a 256
CARGO_TARGET_DIR=/Users/tonyalaribe/Projects/apitoolkit/timefusion/target \
CARGO_BUILD_JOBS=2 \
bash -c 'set -o pipefail; ci/scripts/rust_docs.sh 2>&1 | tee /tmp/timefusion-spill-docs-resume.log'
```

Compare the checksum with the value in section 4.2 before reusing earlier checks.
Save the resumed command's terminal exit status. A partial log does not establish a passing result.
Do not repeat the 114-test selection or either Clippy configuration merely because the assistant paused.
If source, dependencies, toolchain, or relevant configuration changes, rerun affected checks and record the reason.

The handover itself is an untracked Markdown file in the main repository. It is not committed or a remote backup.
No code or diagnostic directory was deleted or moved. Existing local services and unrelated user processes were left alone.
The full goal remains incomplete and is paused at the user's request.
