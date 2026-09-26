# First production deployment: rollup resource fixes

Updated: 2026-09-26. Status: **deployed** to production on 2026-09-25 at 21:49 UTC (PR #322, master `68d39a1c`).

## Outcome

| Item | Result |
| --- | --- |
| Dependency | `tonyalaribe/datafusion` `be42f6a9` = `155b68e1` + spill-reader ownership + quota-rejection cleanup (patch sha256 `d7b19920…`). Docs step resumed and passed (`DOCS_EXIT=0`), completing `dev/rust_lint.sh` |
| Pin | All 33 DataFusion crates moved together; no other lockfile entry changed |
| `make ci-signoff` on `f417aa75` | test 2,074 passed (1 flaky retry: `cache_performance_test`), pg-smoke, e2e 65/65, fmt, clippy — all attested; image smoke passed |
| Image | `ghcr.io/monoscope-tech/timefusion@sha256:28bd488256bdca7de8aafdb85803bc77096fa61bc13ca6fe6bfa0b754ffcac0a`, promoted unchanged by `deploy.yml` (tree `60d4994d` identical to the attested tree) |
| Rollback drill | Candidate wrote Delta (1 Parquet file) + WAL-only rows, SIGKILL; production `2f2df90c` replayed the WAL (`entries=1`) with exact count/distinct/sum, wrote more; candidate rolled forward exactly. Native macOS binaries: the amd64 image cannot run io_uring under qemu-user and segfaults under OrbStack Rosetta |
| Constrained comparison | 3 GiB budget, 100k-group sketch aggregate over 400k rows, arms A B B A A B: identical result digests, no failures, peak RSS 232–249 MB (candidate) vs 240–277 MB; no spill occurred at this size |
| Rollout | Soak 46 probes, 0 failures; WAL recovery 0 ms. Run marked failed only because the client-visible unready interval was 32.8 s vs the 30 s budget: the new process listened 1.7 s after start, the gap was the old 24 h-uptime process draining its buffer |

Post-deploy rates, 15 min uptime vs the 24 h pre-deploy baseline: `flush_failed_total` 0 (was ~115/h), `flush_stalled_total` 0, `backpressure_rejected_total` 0, BaseRollup `resource_exhausted` retries ~356/h (was ~2,190/h), no panic/OOM. `backlog_bytes` and CPU rose with the usual restart re-inflation. No stop condition fired.

The user directed fix-forward rather than rollback for this plan (2026-09-25).

The requested [dependency upgrade](2026-09-25-dependency-upgrade.md) remains in scope as a separate commit.
It does not require a separate production deployment.
When the release includes upgraded dependencies, our fork changes must pass checks on those exact revisions.
Earlier checks on DataFusion 54 do not approve the upgraded artifact.

## Deployment priority

The user requested fewer deployments and faster completion on 2026-09-25.
The release unit is a checked artifact, not an individual commit or plan stage.
Compatible, ready changes share one candidate build, CI signoff, rollback procedure, and rollout.
Unfinished optional stages do not delay a ready release.
An incomplete dependency upgrade does not automatically block independently releasable fixes on the current stack.
Any such release still requires a reproducible dependency pin and all applicable safety checks.

The deployment request authorizes release preparation and rollout after those gates pass.
Routine commits and stages do not require repeated approval requests.
A materially different blast radius, destructive migration, or missing operational choice still requires user direction.
The canary and expansion use the same artifact where the topology permits.
Observation follows measured stability and stop rules, not an arbitrary multi-day waiting period.

The existing `scripts/deploy/run.py` requires the current merged master revision, an immutable image digest, and the deployment lease.
Its limits are 30 seconds of client-visible unavailability and a 120-second soak with probes every two seconds.
Recovery discovery has a 720-second deadline. A confirmed drained handoff additionally requires WAL recovery within 5,000 milliseconds.
These existing checks do not replace resource measurements or the persisted-data rollback drill.

The first deployment does not require completion of the [full rollup plan](2026-09-24-rollups-on-a-fixed-server.md).
The full implementation goal remains unchanged. This checklist defines the first release, not completion of that goal.

## Release scope

The minimum first-release candidate contains:

- DataFusion spill-reader ownership and disk-accounting correction.
- Disk-charge cleanup after quota rejection.
- T-digest retained-capacity accounting.
- Sketch-accumulator object-size accounting.
- Regression tests for these corrections.

Other ready changes can share this release after their own correctness and resource gates pass.
Coverage reconciliation, logical-only routing, journal-format changes, single-pass activation, publication batching, and flush-time aggregation are not currently approved for activation.
Optional optimizations remain disabled. Existing memory, scratch, and concurrency limits remain unchanged.
Unrelated worktree changes do not enter this release.

These corrections make resource limits more accurate. They do not establish a CPU saving or a capacity multiplier.
Accurate accounting can increase spilling or expose capacity errors that old counters concealed.

## When the first deployment happens

The deployment trigger is a reviewed release artifact, passing checks, a tested rollback, and an established deployment scope.
The current user request supplies deployment direction, but does not waive those gates.
There is no defensible calendar ETA yet: dependency packaging, final checks, and rollback validation remain incomplete.

The next milestone is one isolated release candidate with its evidence attached.
Unfinished optimizations do not extend the release schedule. Newly discovered safety defects can block it and require an explicit explanation.

## 1. Finish and package the release

- [ ] Extract only the release-scope changes into an isolated branch.
- [x] Finish the current memory-accounting tests and lint.
- [ ] Review the DataFusion patch against its exact base revision.
- [ ] Replace temporary local dependency paths with a reproducible, reviewed dependency revision.
- [ ] Update all affected DataFusion workspace references consistently.
- [ ] Build the exact release artifact from its committed manifest and lockfile.
- [ ] Record the artifact identity, configuration, dependency revision, and source commit.

The current dependency base is `155b68e1bdb7b8eff460f2a0b102da62a4e6357f`.
The local patched checkout is test evidence, not a deployable dependency reference.

## 2. Establish correctness for the enabled changes

- [ ] Preserve counts, sums, extrema, percentiles, and distinct counts across ordinary and spilling execution.
- [ ] Exercise multiple projects, late versions, tombstones, empty inputs, and high group counts.
- [x] Retain disk charges while readers own unread files.
- [ ] Release memory reservations and physical spill files after EOF, errors, and cancellation.
- [x] Account for retained centroid capacity and each accumulator object.
- [x] Preserve small-state charges and release obsolete allocation charges.
- [ ] Exercise restart recovery and flush handoff through the release artifact.
- [ ] Check query parity across historical holes, empty intervals, late writes, and deletes.

This release changes no coverage or routing policy.
The checked accounting items refer to the isolated source tests and lint recorded later in this document.
They do not establish validation of a combined release artifact or approval for deployment.
Coverage-expanding releases additionally require the full historical, source-view, and hybrid-routing race gates from the main plan.
Those redesign gates do not silently become prerequisites for this resource-only release.

## 3. Establish resource safety, then measure production effects

Before the canary:

- [ ] Compare the release candidate with the current production revision under the same representative workload and limits.
- [ ] Repeat counterbalanced before/after runs rather than relying on one timing.
- [ ] Record CPU time, bytes read, spill usage, peak RSS, maintenance progress, and insert/query latency.
- [ ] Include constrained pools, larger groups, cancellation, and foreground contention.
- [ ] Check scratch-limit rejection followed by cleanup and a successful later spill.
- [ ] Investigate increased failures, stalled work, or material regressions.
- [ ] Record a production baseline without additional heavy source scans.

During the canary:

- [ ] Compare the same metrics over matched workload windows.
- [ ] Normalize work by accepted rows and completed maintenance units.
- [ ] Separate warmup, restart effects, changing traffic, and cache effects.
- [ ] Record at least two counterbalanced comparison cycles before expansion.
- [ ] Use longer windows when variance prevents a meaningful comparison.

Production benefit is measured during the first bounded deployment, not claimed before it exists.
A resource-correctness release can succeed without a CPU saving.
Performance optimizations require demonstrated net savings before broader activation.
The synthetic single-pass savings are not acceptance evidence for this release.

Source review identified an existing DataFusion 54 quota-error risk that requires a focused regression.
`update_disk_usage` updates the global charge before the quota check, but records the file charge only after that check succeeds.
The error path can therefore leave a charge that file cleanup does not subtract. The current spill-reader patch does not change this path.
DataFusion 55 uses a different writer API that reverses its reservation on quota rejection. Its behavior does not prove safety of the version-54 release.
The real-file regression reproduced the leak: two rejected growth checks left 10 bytes charged after file deletion.
Run `38b9e176-4af7-45e0-b1a7-0ad5667cbce5` failed at that assertion after a 44.41-second build.
The candidate now records the file charge before the quota check returns an error.
The case table covers new and previously charged files, repeated rejection, cleanup, and a subsequent successful spill.
All 114 selected execution and spill tests passed in 1.257 seconds after a 45.43-second build.
Run `c1904d77-96e7-4168-8e20-2cd3feb19bf2` excluded 1,396 other tests.
The log at `/tmp/timefusion-spill-quota-tests-20260925.log` records exit zero.
Strict Clippy and the full lint suite are running against the expanded, frozen patch with the CI-pinned license tool.
The log at `/tmp/timefusion-spill-quota-lint-20260925.log` records individual check exits and the final suite exit.
The combined Timefusion artifact, resource comparisons, and rollback drill remain unverified.

## 4. Complete review and CI

- [ ] Run `rs-distill`, `rs-evasion-review`, and `rs-minimal-tests` on the final release diff.
- [ ] Resolve introduced findings without suppression or weakened assertions.
- [ ] Record existing dependency lint failures and resolve the applicable release gate.
- [ ] Run targeted tests, relevant full suites, formatting, and `cargo lint`.
- [ ] Run `make ci-signoff` with the relevant checks from `ci/checks.tsv`.
- [ ] Record local commands, results, and outstanding GitHub checks in the PR.
- [ ] Require every mandatory check to pass before deployment.

No passing attestation covers a failed, skipped, or unrun check.
An unexplained process-leak warning remains a recorded limitation, even after a clean rerun.

## 5. Prepare the canary and rollback

- [ ] Identify the actual deployment unit and its blast radius.
- [ ] Select one existing isolated replica or service instance, if the topology supports it.
- [ ] Do not claim project isolation for a process-wide dependency change.
- [ ] Document a separately approved service-wide window if no isolated deployment unit exists.
- [ ] Record the previous artifact, configuration, restart procedure, and rollback command.
- [ ] Exercise rollback with persisted data and WAL produced by the candidate.
- [ ] Check that rollback requires no data conversion, deletion, or historical rebuild.
- [ ] Name the operator and monitoring owner for the deployment window.

No server scaling is part of this rollout.
The exact topology, commands, and baseline thresholds belong in the deployment PR before approval.

Read-only topology check on 2026-09-25: `srv-captain--timefusion` has one replica and uses `stop-first` updates.
The service specification names `ghcr.io/monoscope-tech/timefusion@sha256:84504e8dc915bca8d8ff1e00102c1a64f246165b713d26033c55e7ad3989387b`.
This is a rollback-image reference captured from the service specification, not a completed rollback drill.
The image and live task must be checked again immediately before deployment.
There is no spare replica for an isolated canary in this topology.
A process-wide dependency change affects the whole service, even if optional project optimizations remain disabled.
The rollout therefore requires a service-wide handoff, readiness checks, observation, and a rollback procedure.
The operator must resolve that deployment scope before activation. No extra replica or server scaling is assumed.

Proposed stop conditions:

- Any stale result, omitted row, duplicate row, corruption, or recovery failure.
- Any OOM, scratch-limit violation, or stalled flush.
- Maintenance backlog growth across three consecutive five-minute windows under comparable input load, when the baseline was stable.
- Insert or query p99 breaches the existing SLO.
- Insert or query p99 exceeds the matched baseline by more than 20% across three consecutive five-minute windows.
- A sustained increase in resource-exhaustion failures relative to the matched baseline.

These are operational stop rules, not proof of statistical significance.
The final thresholds require review against baseline noise before deployment.
A stop condition ends expansion and triggers the approved rollback or incident procedure.

## 6. Deploy, observe, then expand

- [ ] Present the release evidence, remaining limitations, canary scope, and rollback procedure.
- [ ] Record the deployment authorization and resolve any scope choices that remain open.
- [ ] Deploy only the approved artifact and configuration.
- [ ] Observe the canary against the baseline and stop rules.
- [ ] Expand only after the agreed observation period and a reviewed result.
- [ ] Combine later ready optimizations into fewer releases, without removing their individual safety gates.

## Current evidence and remaining work

The boxed spill-reader candidate passed 45 native dependency tests and 18 Timefusion integration tests.
One integration run reported a process-leak warning. A repeat passed without that warning, but its cause remains unknown.
The centroid-capacity fix passed 62 selected function and scan tests.
Both accumulator-object regressions reproduced an eight-byte undercount.
The corrected candidate passed 86 selected tests in 52.087 seconds, including the accumulator and expanded scan cases.
This run reported no process-leak warning.

Timefusion `cargo lint` passed in 5m04s against the patched dependency.
These selected tests and lint do not replace the final release checks or dependency lint.
The original Cargo configuration and lockfile are restored, with no residual diff.
The [dependency review artifact](../../patches/datafusion/README.md) preserves the patch, base revision, checksum, and reproduction commands.
The release branch described below now exists. No final spill dependency revision, deployment artifact, CI signoff, or rollback drill exists yet.

## Release extraction: 2026-09-25

The isolated release branch is `codex/rollup-resource-release-20260925`.
Its checkout is `/tmp/timefusion-release.MSCafN`, based on `2f2df90c275089a4af85f77d3655d41d5bb541d8`.
The initial extraction changes only `src/read/functions.rs`: 55 added lines and 13 removed lines.
It contains the retained-centroid and accumulator-object accounting fixes with their existing regression tests.
The original worktrees remain unchanged by this extraction.

The candidate retains the committed manifest and lockfile, including the existing DataFusion pin.
The DataFusion spill correction is not included yet.
Its dependency revision remains a separate packaging requirement.
No coverage, routing, journal, scheduler, or optional activation change enters this extraction.

`cargo fmt --all -- --check` and `git diff --check` passed.
The selected function tests started with this command:
```sh
CARGO_TARGET_DIR=/Users/tonyalaribe/Projects/apitoolkit/timefusion/target cargo nextest run --locked --lib -E 'test(read::functions::tests::)' --no-fail-fast --status-level fail
```
All 57 selected tests passed in 0.466 seconds after a 4m57s build.
The run ID was `9d1a2704-a002-4ebe-b4c8-f928b90ba7a3`. The filter excluded 1,767 tests.
The build reported existing vendor dead-code, linker, and dependency future-compatibility warnings.
The repository-wide `cargo lint` check passed in 1m57s.
It used the existing all-target, all-feature, locked alias with warnings denied for the application.
Existing dependency warnings remain recorded. No lint configuration changed.
The full standard CI test check started next against local MinIO with `CI_NO_ATTEST=true` and `CI_FORCE=true`.
The command uses `make ci CHECKS=test` with explicit loopback storage endpoints and local credentials.
The standard CI test check passed: 2,074 tests in 770.515 seconds, followed by all 16 doctests.
Nextest excluded 16 tests and reported 24 slow tests. There were no test failures.
E2E and PGWire smoke checks remain separate requirements.
The first attempt stopped before tests: the mirrored MinIO image lacks the health check's `curl` binary.
The host received HTTP 200, but the container probe repeatedly exited 127.
The release branch now uses the image's Bash to request the same endpoint and require HTTP 200.
The replacement probe passed against the health endpoint and rejected a non-health route.
Docker Compose parsed the configuration, recreated the local CI service, and reported it healthy.
The second attempt completed successfully. No application failure or pass is established by the first attempt.
This adds `ci/compose.yml` to the release diff. It changes no production configuration.
`make ci-selftest` also passed its fingerprint, capability, and declared-input checks.
This infrastructure check does not replace application tests.
The existing deployment harness checks also passed:

- `PYTHONDONTWRITEBYTECODE=1 python3 scripts/deploy/test_prepare.py`: 3 tests, 0.725 seconds.
- `PYTHONDONTWRITEBYTECODE=1 python3 scripts/deploy/test_lease.py`: 13 tests, 35.031 seconds.
- `PYTHONDONTWRITEBYTECODE=1 python3 scripts/deploy/test_run.py`: 15 tests, 17.363 seconds.
- `PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-production-image.py`: 2 tests, 4.185 seconds.

These tests exercise local fixtures and include simulated remote state.
Their lease messages describe fixtures, not production actions.
They do not establish persisted-data rollback compatibility or a production handoff.
The image helper checks cover frozen build inputs and promoted-image identity, not a built release image.
The standard application run is `75b36d22-5330-469e-bc0d-d83201a8a4e1`: 2,074 tests across three binaries, with 16 excluded.
The runner reported all 2,074 tests passed, and the enclosing CI command exited successfully after doctests.
The next command started PGWire smoke and E2E checks through `./scripts/ci/ci.sh run pg-smoke e2e`.
It uses the same explicit local storage endpoints with `CI_NO_ATTEST=true` and `CI_FORCE=true`.
PGWire smoke passed through the PostgreSQL 18.4 client against the standalone local server.
It completed readiness and the table, table-description, database, role, and schema catalog commands.
E2E compilation completed in 3m04s.
Run `9cb3e693-20f3-4824-a145-73f851f06bad` started 65 E2E tests with two test processes.
The filter excluded two tests and three other binaries.
All 65 tests passed in 198.566 seconds, with three slow cases and one flaky case.
`ordering_pushdown::one_unsorted_file_does_not_cost_the_majority_its_ordering` failed its first attempt and passed its automatic retry.
The first attempt took 3.006 seconds. The retry took 6.362 seconds.
The failure output exceeded the captured output limit, so the exact first-failure cause remains unresolved.
A focused run without retries subsequently passed. The earlier retry pass remains a flaky result.
The source-checked nextest run queued behind the strict dependency lint build.
While that build ran, the cached E2E executable passed six separate runs of the exact test without retries.
The durations were 5.70, 6.00, 5.79, 5.49, 5.15, and 6.20 seconds.
Each run selected one test and excluded 66 tests, with local MinIO and application logs disabled.
The executable was `target/debug/deps/e2e-7cdc8d03a9a10815`.
These diagnostic runs did not reproduce the failure. They do not establish its cause or replace the queued source-checked run.
The source-checked run `5921aee9-8527-4c19-9a87-6f14fea60c50` passed its one selected test in 6.340 seconds, with 66 excluded.
It used `--retries 0` and the default nextest profile. The original failure remains unexplained.
The enclosing PGWire/E2E command exited successfully.
No passing attestation was published.
Those test runs preceded local packaging. No CI signoff or deployment is claimed.
The extraction is the first release component, not a reduced definition of the full rollup goal.

The source review checked the capacity calculation against locked `tdigests 1.0.1`.
Its compressor preserves the original vector below the threshold and allocates the configured capacity above it.
Its merge method uses the same centroid concatenation as the candidate.
The accounting wrapper retains that algorithm and records capacity without adding a second centroid buffer.
The HLL size method already includes its object size, so the accumulator adds only the enclosing storage difference.
Source inspection shows unchanged centroid serialization: `Vec<(f64, f64)>` with the same bincode configuration.
The scoped `rs-distill` review retained the wrapper because the pinned dependency exposes centroid slices, not retained capacity.
The `rs-evasion-review` found no added suppression, unsafe block, weakened error handling, or serialized-field change in this accounting diff.
The `rs-minimal-tests` review retained the shared HLL/t-digest case table and the real compression-and-merge scenario.
A future t-digest dependency upgrade must recheck the allocation behavior behind `centroid_capacity`.
These findings cover `src/read/functions.rs`, not the final combined release or the unfinished rollup redesign.
The added capacity field is in-memory only. This supports compatibility but does not replace a persisted-data rollback drill.

The spill regression covers cancellation after a returned batch, EOF, and schema mismatch.
Additional cases now cover queued reads and a started blocking read.
The existing `SpawnedTask::drop` aborts a blocking task only before that task starts.
A started read must retain the file and its disk charge until the blocking task releases its reader.
Immediate release on query cancellation is therefore not the expected result.
The regression must establish that the task started, cancel the stream, and check that the file remains charged.
After the read finishes, the regression must check file deletion and zero remaining charge.
Queued cancellation needs a separate case because the blocking task does not start.
The Unix active-read case connects a real FIFO writer, withholds valid IPC bytes, cancels the stream, then releases the bytes.
Its assertions require retained disk charge before release and file cleanup after the read finishes.
It requires `mkfifo` and does not establish Windows coverage. No production test hook, suppression, or unsafe code was added.
The spill dependency checkout pins Rust 1.95.0, while the earlier unsuccessful strict lint attempt used Rust 1.98.
`cargo +1.95.0 fmt --all -- --check` passed on the spill candidate.
Strict lint under the dependency's pinned toolchain started with:

```sh
CARGO_TARGET_DIR=/Users/tonyalaribe/Projects/apitoolkit/timefusion/target CARGO_BUILD_JOBS=1 cargo +1.95.0 clippy --all-targets --all-features --locked -- -D warnings
```

The process passed after 46m41s, including native dependency compilation.
This result covers the spill candidate before the additional queued-cancellation regression.
That regression covers cancellation before the first read and after one returned batch through the real spill stream.
The expanded spill run passed all 46 tests in 0.160 seconds after a 21.73-second build on Rust 1.98.
Run `220fd82b-dd99-4663-9513-97a8bca3855a` excluded 1,396 tests.
Rust 1.95 formatting completed, and the updated diff passed the whitespace check.
Strict lint passed on the queued-cancellation diff in 5m09s.
The later active-read case passed with all 47 spill tests in 0.158 seconds after a 25.06-second build.
Run `ab889807-5d9e-43e2-97dd-68b771084682` excluded 1,396 tests.
Formatting completed. Strict all-target, all-feature lint passed on this updated diff in 5m42s.
The resumed suite passed formatting, its separate Clippy configuration, and TOML formatting. The cached Clippy check took 3.69 seconds.
The license check rejected local Hawkeye 7.0.1 because the fork uses the version-6 configuration format.
The CI workflow pins Hawkeye 6.2.0. Its checksum-checked release binary passed the license check without source changes.
The spelling and Markdown checks also passed.
The Rust documentation check was deliberately stopped with exit 143 before completion, to run the quota-error regression without a competing Cargo build.
It remains required after the resource-error check. Passing checks retain their recorded input scope.
Logs and exit markers are retained outside git at `/tmp/timefusion-spill-lint-final-20260925.log` and `/tmp/timefusion-spill-lint-remainder-20260925.log`.
This does not replace Timefusion's Rust 1.98 checks or justify suppressing warnings.

## Local release commits

The isolated release branch now contains three local commits:

- `584565f0`: the MinIO healthcheck fix.
- `3040f5fd`: retained sketch-memory accounting.
- `beffeacc`: the requested batched-validation rules in `AGENTS.md`, without unrelated main-worktree edits.

The release worktree is clean. These commits contain the previously tested source, without additional implementation changes.
None of these commits is pushed or deployed. The spill dependency commit, immutable pin, combined artifact, and final CI signoff remain incomplete.

## Batched release validation

Keep the release source frozen until a required check identifies a defect. Defer optional cleanup to the next implementation batch.
Record each check's source revision, dependency graph, toolchain, command, result, and log location.
Reuse a passing result only while its inputs remain unchanged. Missing terminal output does not establish a passing result.

Publish the spill revision, then update all 33 DataFusion references together. Regenerate the lockfile before the combined release checks.
Run one integrated `make ci-signoff` against that frozen artifact. Do not run concurrent Cargo jobs against the shared target directory.
Keep correctness, resource limits, persisted-data rollback, and deployment observation as separate acceptance gates.

## Release 2 (rollup pause policy + Stage 0 coverage): 2026-09-26

Merged as PR #323 (`1824a7ca`), deployed 02:11 UTC via `deploy.yml` (CI-built image
`sha256:79aa71e5…`; GitHub CI green on both shards, E2E, clippy — run locally only in part
because the laptop was on the DataFusion 55 port).

- `ROLLUP PAUSE otel_logs_and_spans otel_logs_and_spans_rollup_sessions_1h_v1` at 02:27:59 UTC
  (no direct readers in monoscope; client session queries unroutable — see plan §8.3).
- **Incident:** OOM kills at 02:30:40, 02:43:27 and 02:58:00 (anon ≈ 124 GB of 128.8 GB;
  release 1 held ≈ 35 GB). Cause: release 2 moved compaction admission after file selection
  and priced it by the selected files' decoded estimate, so ~25 SealedConsolidation/HotPacking
  sorts were admitted in the same second (55 running units). `dashboard_1m_v3` was also paused
  at 02:33:57 as a diagnostic; memory kept spiking, which ruled out the rollup tiers.
- **Fix forward:** `96b093f5` restores release 1's admission (queued estimate, before
  selection). Pushed straight to master because prod was crash-looping; lint green, targeted
  tests green except MinIO-storage-full failures on the laptop (disk, not code).
- After recovery: `ROLLUP RESUME otel_logs_and_spans otel_logs_and_spans_rollup_dashboard_1m_v3
  FROM '<hour boundary>'` to restore the dashboard tier; sessions stays paused.
