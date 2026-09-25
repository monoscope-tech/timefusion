# DataFusion spill-reader disk accounting

Status: review artifact, not a production dependency revision.

## Source identity

- Repository: `https://github.com/tonyalaribe/datafusion.git`
- Base: `155b68e1bdb7b8eff460f2a0b102da62a4e6357f`
- Local branch: `codex/spill-reader-disk-accounting`
- Patch: [spill-reader-disk-accounting.patch](spill-reader-disk-accounting.patch)
- SHA-256: `d7b1992034397d8037a7f8fae1926c4a2612b2394cf341e8718429d318a93306`

The patch retains the tracked temporary-file owner while the spill reader remains open.
Without this owner, disk accounting reaches zero while the reader still holds unread file data.
The patch allocates one boxed reader-owner pair per file, not per batch.
The regression covers partial-consumption cancellation, cleanup at EOF, and cleanup after a schema-mismatch error.
It also covers queued cancellation before the first read and after one returned batch.
The Unix active-read case uses a real FIFO and IPC bytes to check retained charges and cleanup after cancellation.
This case requires the `mkfifo` utility and does not establish Windows coverage.

The spill-pool fixture now rotates after each batch.
Its previous threshold created two batches per file despite its one-batch assumption.
The fixture still requires partial disk release and complete final cleanup.

The quota-error fix records each file's new charge before the limit check returns an error.
Without this change, rejected growth leaves a global charge that file cleanup cannot release.
The real-file regression failed with 10 bytes still charged after two rejections and file deletion.
It covers new and previously charged files, repeated rejection, cleanup, and a subsequent successful spill.

## Reproduce

In a clean checkout at the base revision, run:

```sh
git apply --check /absolute/path/to/spill-reader-disk-accounting.patch
git apply /absolute/path/to/spill-reader-disk-accounting.patch
cargo +1.98 nextest run --config 'profile.dev.debug="line-tables-only"' -p datafusion-physical-plan --lib --no-fail-fast -E 'test(spill::)'
cargo +1.95.0 clippy --all-targets --all-features --locked -- -D warnings
```

The expanded native spill selection passed all 47 tests in 0.158 seconds after a 25.06-second build.
Strict all-target, all-feature lint passed under pinned Rust 1.95 with the queued-cancellation test in 5m09s.
Formatting completed after the active-read test addition.
Strict all-target, all-feature lint passed on the reader-only diff in 5m42s.
The suite's separate Clippy configuration passed in 3.69 seconds. TOML formatting, license, spelling, and Markdown checks passed.
The license check requires CI-pinned Hawkeye 6.2.0, not local version 7.0.1.
The Rust documentation check was stopped before completion to avoid competing builds during the quota-error regression.
The expanded patch passed all 114 selected execution and spill tests in 1.257 seconds after a 45.43-second build.
Run `c1904d77-96e7-4168-8e20-2cd3feb19bf2` excluded 1,396 other tests.
Strict all-target/all-feature Clippy passed on the expanded patch in 15m22s.
The separate suite Clippy configuration passed in 5m02s. Formatting, TOML, license, spelling, and Markdown checks passed.
Rust documentation was intentionally stopped for the user's handover request at 19:48 UTC. The wrapper exit was 143, not a source failure.
Only that documentation step remains in the dependency lint suite for this unchanged patch.
Earlier Rust 1.98 lint attempts reported existing dependency findings. No lint suppression was added.
These results do not establish full release signoff.

Both forward application against the unchanged index and reverse application against the candidate passed their read-only checks.
No dependency commit, push, or production manifest change accompanied this artifact.
The checkout's `origin` is a local Cargo cache. Its separate `fork` remote points to the owned GitHub repository.

## Scoped Rust review

The production change keeps the existing stream states and moves the tracked file owner with the reader.
The reader drops before its owner because the tuple stores the reader first.
No new production suppression, unsafe block, error fallback, or public API enters the patch.
Test assertions use real spill files, Arrow IPC, disk counters, and Tokio tasks.
The Unix-only case exercises a real FIFO. The platform condition does not remove the portable cancellation, EOF, or error cases.

The distillation review identifies optional reuse in the three test fixtures: one fixture helper can remove repeated manager setup.
That discretionary cleanup is proposed only, not applied or required for release.
The spill-pool change corrects its file-rotation fixture while retaining partial-release and final-cleanup assertions.
The quota fix changes no limit, error type, or public API. It preserves the charge for bytes that already exist until file cleanup.
Its test uses real temporary files and one case table. It adds no mock, sleep, suppression, or production test hook.

Final scope review uses `rs-distill`, `rs-evasion-review`, `rs-minimal-tests`, and the ownership and cancellation rules from `rust-skills`.

| File | Review result | Optional reduction |
| --- | --- | --- |
| `execution/src/disk_manager.rs` | Existing fields and error type remain intact. The effectful case table covers rejection, cleanup, and later admission. | None |
| `physical-plan/src/spill/mod.rs` | The existing state machine owns one reader-file pair. Real-file tests cover completion, errors, queued cancellation, and active cancellation. | A shared fixture can remove about 15 repeated lines. Deferred, not applied. |
| `physical-plan/src/spill/spill_pool.rs` | The fixture matches the existing strict rotation threshold. Partial-release and final-cleanup assertions remain. | None |

The review found no introduced evasion, weakened limit, or missing ownership transfer in this scope.
The quota case preserves its imperative form because each step performs file I/O or checks an observable resource transition.
This review does not establish behavior outside the selected patch or replace the combined release checks.

## Release requirements

- Complete the final Rust reviews.
- Resolve the applicable dependency lint gate.
- Publish a reviewed dependency revision.
- Update every affected workspace dependency reference consistently.
- Build and check the release from its committed manifest and lockfile.

The [deployment checklist](../../docs/plans/2026-09-25-first-rollup-deployment.md) defines the remaining release gates.
