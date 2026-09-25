# Dependency upgrade for the fixed-server rollup plan

Updated: 2026-09-25. Status: implementation started, not ready for deployment.

The upgrade preserves Timefusion fork behavior on the latest compatible upstream bases.
The full [rollup plan](2026-09-24-rollups-on-a-fixed-server.md) remains in scope.
Existing results for DataFusion 54 do not establish correctness or performance for the upgraded stack.

Deployment priority: keep this upgrade in a separate commit, but combine compatible ready changes into one checked release artifact.
The user requested fewer production deployments, not weaker checks.
The [deployment checklist](2026-09-25-first-rollup-deployment.md) defines the release gates and authorization.
An unfinished upgrade does not automatically delay independently releasable fixes.

## Candidate stack

The version-54 resource release now includes a reproduced quota-error correction.
Its old `update_disk_usage` path must record the file charge before returning a quota error, so cleanup can release the global charge.
The version-55 candidate has no equivalent public update method. Its `FileSpillWriter::write` reserves bytes before writing and reverses the reservation on quota rejection.
Do not port the version-54 implementation mechanically. Adapt the rejection, cleanup, and later-admission regression to the version-55 writer API instead.
This source comparison covers quota rejection, not all physical write failures or concurrent writer behavior.

| Component | Current pin | Candidate base | Remaining decision |
| --- | --- | --- | --- |
| DataFusion | Fork `155b68e1`, version 54.1 | Release 55.1.0, `7d3835c71f30cbd3c3ae4041732267f1f453097a` | Port or retire each fork patch after regression checks |
| Arrow and Parquet | 58.x | Compatible 59.x family | Resolve one coherent version family |
| Delta | Fork `12847eb6` | Upstream `29db75879be8df185fae765cee4c801f344093c2`, which uses DataFusion 55 and Arrow 59 | Audit release alternatives and port custom APIs |
| datafusion-postgres | Fork `2fb6a2fb`, branch `timefusion-df54` | Upstream `eda0da032ed8d6003b5041fce67c1e5b2f101876`, which uses DataFusion 55 and Arrow 59 | Preserve hooks, catalogs, cursors, and memory fixes |
| Delta kernel | Fork `e035e5db` | Audited source base `ab665890`, kernel 0.28.1 and engine 0.28.0 | Review the reconstructed version and integrate the tested cache ports |
| datafusion-variant | Upstream `9e1c8469` | Audit latest `1a68d5686707d454a75fec8acae44b458bd0bfa9` | Its manifest still requires DataFusion 54 with Arrow 59 and a root patch |
| JSON functions | 0.54.2 | Tagged 0.55.4, `f4f0a6e3d4b369c17b1137c1be302f1678aca4e6` | Check APIs and resolve the release |
| Tracing | 54.0.0 | Tagged 55.0.0, `d8f205bf52ef6db4f2c8ba62ccb5dbf11ce16d84` | Check instrumentation with new execution plans |

These are candidate bases, not a verified dependency graph.
The [JSON release manifest](https://github.com/datafusion-contrib/datafusion-functions-json/blob/v0.55.4/Cargo.toml) requires DataFusion 55.
The [tracing release manifest](https://github.com/datafusion-contrib/datafusion-tracing/blob/55.0.0/Cargo.toml) requires DataFusion 55.0.0.
DataFusion 55.1 uses Arrow 59.2. Its development branch already uses Arrow 60.
The latest datafusion-postgres release, 0.18.0, still uses DataFusion 54 and Arrow 58.
Thus, the latest release and the latest compatible source revision are not interchangeable.

Sources: [DataFusion release manifest](https://github.com/apache/datafusion/blob/55.1.0/Cargo.toml),
[Delta candidate manifest](https://github.com/delta-io/delta-rs/blob/29db75879be8df185fae765cee4c801f344093c2/Cargo.toml),
[Postgres candidate manifest](https://github.com/datafusion-contrib/datafusion-postgres/blob/eda0da032ed8d6003b5041fce67c1e5b2f101876/Cargo.toml),
[Postgres release manifest](https://github.com/datafusion-contrib/datafusion-postgres/blob/datafusion-postgres-v0.18.0/Cargo.toml).

## Delta upstream-reuse review: 2026-09-25

This review compares old fork `12847eb6`, candidate `29db7587`, and published kernel 0.28.1.
It uses `rs-distill` for reuse and `rs-evasion-review` for contract differences.
This is a focused source review, not a full lint or correctness signoff. No Rust changes result from this review.

The [release notes](https://github.com/delta-io/delta-rs/releases) include snapshot discovery and replay-safety changes.
The [Rust 0.32.4 release](https://github.com/delta-io/delta-rs/releases/tag/rust-v0.32.4) is a backport line, not the candidate stack.
[PR 4660](https://github.com/delta-io/delta-rs/pull/4660) preserves replay-safe statistics fields and rejects unsuitable cache seeds.
[PR 4661](https://github.com/delta-io/delta-rs/pull/4661) moves file discovery into Snapshot.
Release names alone do not establish API or behavioral equivalence.

| Area | Source finding | Disposition |
| --- | --- | --- |
| Variant normalization | Four existing real-storage cases pass without old normalization patches | Already omitted for covered paths. Merge and full application checks remain |
| Kernel executor delivery | Upstream already avoids the nested blocking task | Old patch replaced by upstream behavior. Eight executor tests passed previously |
| Tag decoding | Upstream `file_constant_map` and `to_add` preserve tags | Candidate accessor already reuses upstream. Do not port the old decoder |
| Selected-file scans | Upstream `SelectedFileScanFactory` carries scan configuration and operation identity | Candidate sort implementation already reuses it |
| Snapshot advancement | Upstream `Snapshot::update` passes materialized metadata into replay | Compare against the new custom fast path before retaining that extra implementation |
| DV path decoding | Kernel descriptor exposes public `absolute_path`; `relative_path` is crate-private | Propose replacement through the public API, with an object-store path adapter |
| Existing DV reads | Kernel `DeletionVectorDescriptor::read` checks version, size, magic, and CRC | Propose replacement of manual frame decoding after async and corruption checks |
| DV descriptor creation | Kernel `DeletionVectorWriteResult::to_descriptor` uses `DeletionVectorPath` | Propose replacement of manual descriptor construction after type conversion checks |
| DV bitmap input | `KernelDeletionVector::add_deleted_row_indexes` accepts an iterator | Propose removal of the intermediate `HashSet` |
| Remove actions | Upstream `LogicalFileView::remove_action` omits row-tracking values | Not an equivalent replacement for our metadata-preserving helper |

### Snapshot implementation: justify the additional path

Upstream `Snapshot::update` builds the target kernel snapshot and passes the existing materialized files into `materialize_files_with_engine`.
The candidate retains those identity and policy checks, but adds a second implementation for append and removal advancement.
Passing correctness tests does not prove that this extra implementation saves work.

The next comparison must measure ordinary upstream updates against the custom path on the same histories.
The cases include append, removal, intervening commits, checkpoint boundaries, metadata changes, and retained snapshots.
The comparison must record CPU, metadata reads, allocations, and exact file metadata.
If upstream meets the resource requirement, the proposal is to delete the custom advancement implementation and retain only necessary public adapters.
No new cache or replay implementation is justified solely by the existence of the old fork API.

### Snapshot follow-up: cached I/O is not zero replay work

The kernel `scan_metadata_from` reads commits newer than the seed version.
It then transforms cached file batches back into action batches and passes them through scan replay.
A newer checkpoint causes a full scan instead.
Thus, upstream cache reuse avoids old log reads, but still processes cached file entries.

The custom append path carries existing Arrow batches forward and replays only new commits.
Removal handling additionally filters carried paths.
This is a concrete algorithmic difference, not a measured production saving.
The snapshot implementation must not be deleted solely because upstream accepts a cache seed.

A fresh focused run passed all 11 selected tests in 4.221 seconds after a 0.79-second cached build.
The selection covers upstream cache reuse, failed-update preservation, append equivalence, removal handling, and catch-up cases.
The command was:
```sh
CARGO_TARGET_DIR=/Users/tonyalaribe/Projects/apitoolkit/timefusion/target cargo +1.98 nextest run --locked -p deltalake-core --features datafusion --lib --no-fail-fast --status-level fail -E 'test(test_eager_file_views_reuses_materialized_files) | test(test_failed_eager_update_preserves_materialized_files) | test(advance_append_matches_full_update) | test(incremental_advance_applies_removes) | test(advance_catchup)'
```
The run ID was `87336670-7933-4606-a0a9-045fa3a573df`.
The filter excluded 1,264 tests. Existing unused-code, deprecation, linker, and future-compatibility warnings remain.
This run is correctness evidence only. It does not measure CPU, allocations, or comparative metadata I/O.
No Rust implementation changed, and no new CI attestation or deployment occurred.

### Deletion-vector reuse: preserve contracts

The old `read_existing_dv` decodes bitmap bytes without checking the frame checksum.
The kernel reader supplies those checks and supports additional storage forms.
However, its synchronous `StorageHandler` interface requires an appropriate blocking boundary in the async write path.
The replacement must preserve operation-scoped storage, error propagation, repeated-delete unions, and cancellation behavior.

Source inspection also found unchecked slicing in the kernel inline reader.
Malformed inline payloads therefore need explicit regressions before this reader becomes a replacement across all storage forms.
Reuse does not establish safety for untested corrupt input.

The old `dv_relative_path` slices a string at a byte offset.
The kernel implementation uses byte slices and reports invalid encodings.
This is a stronger reuse candidate than a cosmetic refactor.
The kernel's `relative_path` method is crate-private, so Delta cannot call it directly.
The public `absolute_path` API requires an adapter that preserves table-root and object-store path semantics.
The replacement must reject invalid paths without silently excluding live DV files from vacuum protection.

The old writer already uses `StreamingDeletionVectorWriter`.
That writer is not a newly discovered replacement.
The remaining opportunities are path handling, read-side framing, descriptor construction, and the unnecessary bitmap-to-set allocation.

### Review boundaries and proposed savings

The DV helper changes can remove roughly 60–90 handwritten production lines before adapters and regression tests.
This is an estimate from the old helper bodies, not a measured final diff or CPU saving.
The snapshot opportunity can remove a larger implementation, but its resource comparison remains open.

The evasion review rejects a direct `remove_action` substitution.
That API sets `base_row_id` and `default_row_commit_version` to `None`, while our existing helper preserves them.
A similar name does not permit a weaker metadata contract.

These proposals do not remove DV write orchestration, conflict handling, physical-row safeguards, or live-DV vacuum protection.
The current upstream APIs do not establish replacement of those requirements.
The review proposes removals before implementation, as required by `rs-distill`.

## Preserve fork behavior

Each existing patch needs one recorded disposition: ported, replaced by upstream behavior, or deliberately retired with supporting evidence.
A matching symbol or clean patch application is not proof of equivalent behavior.

The DataFusion inventory includes prepared-statement deallocation, UPDATE FROM support, positional scans, sort pushdown restrictions, shared-buffer reservations, output metrics, and correlated UNNEST.
The local spill-owner correction is an additional candidate, outside the current fork pin.

| Existing DataFusion patch | Inspection against 55.1 | Required action |
| --- | --- | --- |
| `3fb603e95`: DEALLOCATE ALL | Enum, session behavior, and regression ported locally | Run SQL and Postgres integration checks |
| `a442435c7`: UPDATE FROM capability | Capability wired through the relocated planner trait | Run default rejection and Delta planner checks |
| `f903e2114`: positional file scans | Repartitioning control ported locally | Run scan-contract and Delta deletion-mask regressions |
| `158e8405f`: shared sort buffers | Regression fails on 55.1. Port reuses the upstream counter | Complete native and Timefusion resource checks |
| `2c2974878`: sort pushdown boundary | Capability and optimizer guard ported locally | Run the optimizer regression and Delta integration |
| `41162dd13`: output-byte metric | Ported locally | Complete review and broader checks |
| `155b68e1b`: correlated UNNEST | Baseline regression fails on 55.1. Planner port applied locally | Complete SQL regressions and Timefusion query checks |
| Local spill lifecycle | Upstream retains its owner until stream drop | Run EOF, cancellation, and read-error regression |

The shared-buffer helper alone does not replace the sort patch.
In 55.1, `ReservationStream` still shrinks by each batch's full size.
The port must preserve correct reservation behavior when multiple batches retain the same buffers.

The original shared-view sort regression failed on 55.1 in 0.045 seconds after a 48.98-second build.
The 24 MiB pool rejected an additional 59.7 MB reservation while the sorter already held 20.3 MB.
The sort counted the same view buffers for multiple output batches.
The candidate uses the upstream counter in reverse batch order to assign each buffer to its last queued use.
The stream releases those charges as it emits batches. Stream destruction releases the remaining reservation.
The retained tests cover sorted values, cancellation, completion, nullable shared slices, and failed admission.
All 102 selected memory, sort, stream, and spill tests passed in 0.385 seconds after a 1m32s build.
The original failure now passes under the same 24 MiB limit.
The command was `cargo +1.98 nextest run --locked -p datafusion-common -p datafusion-physical-plan --lib --no-fail-fast -E 'test(record_batch_tests) | test(sorts::sort::tests) | test(stream::test) | test(spill::)'`.
The build reported three unused imports in unchanged upstream test code.
These results do not establish process RSS, production savings, full-stack compatibility, or a warning-free build.

The Rust review retained the original real-sort scenario and its cancellation and completion cases.
The helper reuses `RecordBatchMemoryCounter` rather than adding a second buffer traversal implementation.
The reverse loop writes release amounts in place and avoids an additional allocation.
The stream retains typed batch ownership and places batches before the reservation in field drop order.
No new unsafe operation, lint suppression, or production panic was added.

The Delta inventory includes Variant handling, incremental snapshots, ordered compaction, deletion-vector writes and scans, transaction conflicts, scan pruning, and footer ordering.
The fork contains many dependent changes. A wholesale replacement with upstream would remove required Timefusion APIs.

The candidate Delta scan assigns row indexes after deletion-vector filtering.
Timefusion requires physical file positions, including gaps for deleted rows.
The port must preserve that contract through filters, sort pushdown, scan resets, and multiple files.
The upgraded regression reproduced the position error in three cases.
Deletion gaps and a short mask reported physical row 2 as row 1.
After a fully deleted batch, the next batch reported positions 1 and 2 instead of 3 and 4.
The unmasked case passed. The run finished in 0.404 seconds after a 3m28s build.
The regression extends the existing stream scenario with four parameterized masks.

The candidate now advances its counter by the raw batch size.
It applies the same mask to the data and the physical positions.
The mask converts to an Arrow bitmap once, without another vector copy.
The range excludes its upper bound before conversion to one-based positions, so an empty batch cannot overflow the starting position.
All 49 scan-execution tests passed in 1.125 seconds after a 3m40s build.
The command was `cargo +1.98 nextest run --locked -p deltalake-core --features datafusion --lib --no-fail-fast --status-level fail -E 'test(table_provider::next::scan::exec::tests)'`.
The selection includes the four mask cases and existing real-file deletion-vector, topology, and execution tests.
This port does not yet establish full deletion-vector write or optimizer correctness.
The Rust review found no new suppression, unsafe operation, production panic, or widened API.
The local fixture setup used the upstream `make setup-dat` target.

The candidate also adds topology checks for sequential deletion-vector scans.
Those checks need reconciliation with the fork's immutable masks and per-execution offsets, not wholesale replacement.
The immutable-mask port now retains the upstream topology checks.
Each execution shares the plan's masks through `Arc` and owns a per-file cursor map.
The port removes full-mask copies at execution start and repeated `Vec::drain` shifts between batches.
It still allocates a batch-sized selection and converts that selection to an Arrow bitmap.
Unmasked scans do not allocate a shared empty mask map.
The port adds no shared mutation, lock, background task, or durable record.

Five cursor cases cover empty masks, short masks, exact boundaries, zero-row batches, and exhaustion across multiple batches.
Each case runs twice against the same immutable mask.
The real-file deletion-vector scenario now runs after two plan resets and requires the same complete result.
The existing concurrent multi-group scan test remains unchanged.
All 50 native scan tests passed in 0.882 seconds after a 3m05s build.
The run used the same scan-execution command recorded for the physical-position port.
Formatting and whitespace checks passed. Existing compiler and dependency warnings remain.
CPU and memory savings still require measurement in the final stack.

The upstream candidate lacks the fork's `SortBy` and `with_max_files_per_bin` APIs.
The local port now connects `with_max_files_per_bin` to compact planning through `Option<NonZeroUsize>`.
It preserves byte-only binning by default and does not change Z-order behavior.
Six parameterized cases cover no cap, caps of one and two, a partial final bin, and exact or oversized caps.
The assertions require the expected bin count, bounded fan-in, complete ordered file identities, and consistent span metrics.
Existing planner tests retain their uncapped expectations. Four integration callers now pass the optional cap explicitly.
Formatting and whitespace checks pass. The new cases passed in the combined run recorded below.
A real-file regression also compacts seven tiny files repeatedly with a two-file cap.
It requires active file counts of four, two, one, and one, with exact row identities after every pass.
The final pass must publish no additional file.
All 13 selected planner and real-file tests passed in 0.879 seconds after a 2m28s build.
The selection includes the capped convergence scenario and the existing uncapped non-partitioned optimize scenario.
The command was `cargo +1.98 nextest run --locked -p deltalake-core --features datafusion --lib --test it_datafusion --no-fail-fast --status-level fail -E 'test(compact_planner_tests) | test(test_capped_compaction_converges_without_losing_rows) | test(test_optimize_non_partitioned_table)'`.
Existing unused-code, deprecated-reader, linker, and dependency future-compatibility warnings remain.
This result establishes the compact cap behavior, not sorted-rewrite behavior or cancellation cleanup under a failed attempt.
Sorted-rewrite support must propagate the same cap when that remaining port lands.
Ordered compaction, exact-file deduplication, cancellation, and idle-timeout behavior remain explicit port requirements.
The cancellation port now uses DataFusion's existing `SpawnedTask` for compact and Z-order rewrites.
That type aborts its task on drop and preserves the join-result type expected by Delta's error adapter.
This replaces the custom wrapper from fork commit `c5d23524`, without another dependency or helper implementation.
The upstream candidate used bare Tokio handles, which detach running rewrites when an attempt drops its stream.
DataFusion includes regressions for cancellation before and after task startup. Those tests have not run in this upgrade pass yet.
Formatting, whitespace, and compilation checks pass for the Delta change. Real compaction cancellation checks remain open.
The local sorted-rewrite branches now use the same ownership rule.
The initial sort port failed compilation because it also called the fork's unported incremental-snapshot API.
The initial isolated sort check retained upstream post-commit snapshot updates while the API port was incomplete.
Timefusion directly calls the incremental-snapshot API, so this remains a required port before final-stack integration.
The newer snapshot implementation also checks cache identity and statistics policy. The port must preserve both checks.
Three restored real-file regressions cover sorted-compaction convergence, greatest-tiebreak selection, and selected-file deduplication.
The current candidate preserves upstream scan configuration, operation-scoped stores, and upload budgets.
Additional checks remain for unsorted singleton files and accurate Parquet ordering metadata.
The selected-path regression reproduced a suffix-matching defect against a real Delta table.
An unrelated path with the same suffix rewrote one live file instead of zero.
Valid relative paths and table URIs passed. The baseline finished with two passes and one failure in 0.188 seconds.
Its build took 11.13 seconds.
The port now uses exact hash-set membership for the stored path or the table's file URI.
This removes the per-candidate scan through the selection set. Resource savings remain unmeasured.
The regression matrix checks publication counts and complete row identities.
All 23 selected tests passed in 1.394 seconds after a 2m28s build, with the same combined command recorded below.
This includes all three path cases and the earlier 20 selected tests.
The change adds no warning suppression, unsafe operation, or production panic.
Singleton and footer-ordering checks must inspect physical Parquet output, not query results sorted after the scan.
The new ordering regression reads the output file directly and compares ascending or descending values across two row groups.
It also checks each footer's sort declaration and complete row identities.
The first build failed because the test used the removed `parquet::format` module.
The fixture now uses `parquet::file::metadata::SortingColumn`, without a compatibility shim.
Both physical-order cases and six existing sort tests passed in 0.226 seconds after a 9.46-second build.
The command was `cargo +1.98 nextest run --locked -p deltalake-core --features datafusion --test it_datafusion --no-fail-fast --status-level fail -E 'test(optimize_sortby)'`.
The expanded matrix covers ascending and descending output on partitioned and unpartitioned tables.
All ten selected sort tests passed in 0.383 seconds after a 1m07s build, with the same command.
All four physical-order cases read two row groups directly. Formatting and whitespace checks passed.
The API documentation now states that callers supply footer ordering through writer properties.
`SortBy` neither infers nor validates that declaration. Ordering is local to each bin, not global across bins.
Null ordering, nested leaf indices, and caller-declaration mismatch remain separate checks.
All 20 selected tests passed in 1.160 seconds after a 2m36s build.
The selection includes the restored sort regressions and the existing cap, timeout, and non-partitioned checks.
The command was `cargo +1.98 nextest run --locked -p deltalake-core --features datafusion --lib --test it_datafusion --no-fail-fast --status-level fail -E 'test(compact_planner_tests) | test(capped_compaction) | test(non_partitioned) | test(optimize_sortby)'`.
Formatting and whitespace checks passed. Existing compiler, linker, and dependency warnings remain.
This result does not establish full-stack compatibility, bounded resource use, or complete ordering correctness.
The batch-read idle-timeout port now has a pending regression against the real rewrite function and in-memory object store.
Two cases stall before the first batch and after one valid batch.
Virtual time bounds the test without a real 20-minute wait. Existing `tokio-test` dependencies already enable this facility.
The outer deadline is one second beyond the fork's 20-minute limit.
The regression requires an internal timeout with a preserved source error and no published object.
Both baseline cases failed because the rewrite remained pending until the outer deadline.
The run finished in 0.111 seconds after a 2m31s build.
The baseline command selected `test(stalled_rewrite_expires_without_publishing)` in the native Delta library suite with `--locked --features datafusion`.
The port now restores the 20-minute timeout around each batch read.
A private error retains the configured duration and the underlying Tokio timeout through Delta's existing source-error wrapper.
This limit covers batch arrival, not scan initialization or writer operations.
All 15 tests in the combined planner and real-file selection passed in 1.164 seconds after a 2m44s build.
This selection includes both timeout regressions and the earlier 13 cap and compaction checks.
The timeout cases preserved the source error and left the test object store empty.
Existing compiler, linker, and dependency warnings remain. Failed-attempt cancellation and the full Timefusion workload still need checks.
The read-only three-way merge preview reports textual conflicts in nine files.
These include the workspace manifest, scan execution and planning, snapshot iterators, delete, merge, optimize, and vacuum.
Clean textual merges still require API and behavior checks.

The Postgres inventory includes parameter hooks, catalog compatibility, cursors, and prepared-statement memory reductions.
Its moving branch reference must become an immutable revision in the final Timefusion manifest.
The isolated Postgres worktree is `/tmp/timefusion-dependency-upgrade.VYaIwA/datafusion-postgres-55`, on branch `timefusion-upgrade-55`.
It starts at the candidate revision in the stack table.
The upstream candidate retains both the AST and logical plan in each parsed statement.
The local port now retains compact description metadata for bulk statements and rebuilds their plans through the parse hooks at execution.
Deferred metadata includes both Arrow parameter types and PostgreSQL wire types.
This preserves upstream semantic wire-type overrides after the original plan is released.
The port also adapts the hook signatures and cursor statement type.

The old fork skips execute-time hooks for deferred statements.
The permissions hook can handle bulk statements, so that shortcut is not valid for every hook configuration.
The local port instead rebuilds a transient AST with the plan and runs execute-time hooks before execution.
It does not retain the rebuilt AST for the connection lifetime.
This is a source-level finding. No production authorization failure is established.
The new real-protocol permission and prepared-statement retention regressions pass, as detailed below.

The initial `cargo +1.98 check -p datafusion-postgres --features pgvector` failed on DataFusion's non-exhaustive `WriteOp` enum.
The port now returns SQLSTATE `0A000` for an unsupported operation before execution, without a panic or a fabricated completion tag.
The Postgres workspace now declares Rust 1.94.0, the minimum version required by DataFusion 55.1.
Three regression tests are present. Their first native build failed because the permission fixture requested an unavailable Tokio runtime feature.
The fixture now uses the existing current-thread runtime. Its synchronous client remains on `spawn_blocking` alongside the asynchronous server.
The corrected run passed retention and TCP permission checks and failed the Unicode regression in 0.371 seconds after a 3m24s build.
The linker reported an oversized unwind-section warning.
The retention test uses the real parser and compares parameter descriptions before and after plan reconstruction.
The TCP permission test executes one prepared insert twice for denied and allowed users, then checks the stored row count.
These tests replace two mock-based retention fixtures. Existing unrelated upstream tests remain unchanged.
The command is `cargo +1.98 nextest run --locked -p datafusion-postgres --lib --features pgvector --no-fail-fast --status-level fail -E 'test(deferred_tests)'`.
The Unicode regression reproduced a panic on a byte boundary inside `é`.
The helper now uses `split_at_checked(5)`, which leaves short strings and non-ASCII prefixes unchanged without allocation.
All 32 PostgreSQL library tests passed with `--features pgvector` in 1.484 seconds after a 1m38s build.
This broader run used the same command without the test filter and included the existing ASCII synonym cases.
The linker repeated its unwind-section warning. This result does not cover the later listener port or full-stack integration.
Listener controls, catalog compatibility, planner changes, complete review, and final-stack integration remain open.

The listener port now preserves upstream `serve` delegation through `serve_with_hooks`, including optional pgvector initialization.
The old fork's direct factory construction cannot replace that entry point without losing the newer initialization.
Backlog control, pre-bound listeners, and accept-loop shutdown are now ported locally.
New real-socket tests cover IPv4, IPv6, hostname binding, and shutdown with an accepted PostgreSQL connection.
The shutdown scenario requires an existing connection to remain usable and a new connection to fail after the accept loop stops.
Its options deliberately name a different host and port, so the test also checks use of the supplied listener.
The first expanded build rejected a removed factory constructor in the new fixture.
The fixture now uses `new_with_hooks` with the default hooks.
All 34 library tests passed in 0.850 seconds after a 1m47s build.
The run included IPv4, unbracketed IPv6, hostname binding, and shutdown with an accepted connection.
The inherited IPv6 construction passed on this host, so no speculative address-parsing fix was applied.
The linker repeated its unwind-section warning. Cross-platform behavior and full-stack integration remain open.
The planner's `Cast.field.data_type()` adaptation already exists upstream.
Upstream also replaces the old regclass-only rewrite with a broader catalog-cast rule, including direct numeric OID literals.
That behavior can remove the need for the fork's numeric partition-ancestor workaround, but client-query regressions must establish equivalence.
The implicit namespace-name comparison remains separate: upstream still replaces one known client query with an empty result.
That replacement does not preserve the fork's namespace lookup behavior.
The existing current-user regression now includes `USER`, `SESSION_USER`, and a quoted `"user"` identifier.
The baseline failed on uppercase `SESSION_USER`, before it reached the `USER` case.
The port now maps both names to the existing canonical `session_user` spelling.
The quoted identifier remains unchanged.

Upstream now registers `array_upper` and `array_lower` and includes native and client-query regressions.
That implementation is a candidate replacement for the fork's standalone `array_upper` patch, not a verified replacement yet.
Its Int32 return type differs from the old fork's Int64 result. Client compatibility and oversized-list conversion need checks.
UUID and JSON bind decoding still call `portal.parameter::<String>()` upstream.
The nearby upstream tests explicitly omit those types because their string decoder does not accept the wire types.
The fork's protocol decoding and JSON output encoding therefore remain port requirements.
Binary JSONB checks must include its version byte and malformed payloads, not only valid UTF-8 bodies.
Two bind-protocol regressions now replace the upstream comment that excluded UUID and JSON tests.
They construct real pgwire Bind messages and portals, without mocks or a live external database.
The valid cases cover binary and text UUID, JSON, and JSONB, plus null values for every combination.
The malformed cases cover a short UUID, missing or unsupported JSONB version, and invalid UTF-8.
The baseline failed the valid UUID case with `InvalidRustTypeForParameter("uuid")`.
It passed the malformed-input case by rejecting the types, which did not establish correct malformed-payload handling.
The run finished in 0.065 seconds after a 3m58s build.
The baseline command is `cargo +1.98 nextest run --locked -p arrow-pg --lib --features datafusion --no-fail-fast --status-level fail -E 'test(uuid_and_json_bind_payloads)'`.
The decoder port reads the raw parameter bytes for these three wire types.
It converts binary UUIDs with the existing UUID library and requires binary JSONB version 1 before decoding UTF-8.
Text payloads retain the fork's string representation. This is wire framing and UTF-8 validation, not full JSON grammar validation.
The port adds no JSON parse tree or re-serialization step.
All 68 Arrow/PostgreSQL library tests passed in 0.177 seconds after a 5.59-second build.
That run omitted `--locked` to add the fork's UUID dependency. The only lockfile change adds `uuid` to `arrow-pg`'s dependency list.
The malformed-input assertion now requires `FailedToParseParameter`, rather than accepting any rejection.
The stronger 68-test selection passed with `--locked` in 0.194 seconds after a 5.29-second build.
The full Timefusion protocol suite remains open.
The output port includes a real-encoder regression for `tf.pg_type` metadata.
It covers UTF-8 and UTF-8 view arrays, JSON and JSONB, text and binary output, and null rows.
The assertions check the reported PostgreSQL type and exact wire bytes, including JSONB's version byte and null framing.
The unchanged output implementation failed because it reported JSON metadata as PostgreSQL text.
The combined baseline selects `test(json_metadata_and_wire_output) | test(test_current_user)` across `arrow-pg` and `datafusion-pg-catalog`.
It used `--locked --lib --features arrow-pg/datafusion`. Both baseline tests failed in 0.081 seconds after a 4m09s build.
The output port restores JSON type metadata and borrows serialized strings for wire encoding.
Binary JSONB adds its version byte. Text and binary JSON preserve the original bytes.
The existing `Option<T>` codecs handle null values without a second null representation.
All 120 Arrow/PostgreSQL and catalog library tests passed in 0.685 seconds after a 17.50-second build.
The command was `cargo +1.98 nextest run --locked -p arrow-pg -p datafusion-pg-catalog --lib --features arrow-pg/datafusion --no-fail-fast --status-level fail`.
Formatting and whitespace checks passed. The catalog linker repeated its unwind-section warning.
This result does not establish full application compatibility, resource savings, or release readiness.

## Implementation and checks

### Broader Delta check

The broader local run includes the complete core library, DataFusion integration target, and Variant target.
It limited execution to four test processes.
All 1,439 executed tests passed in 56.270 seconds after a 2m27s build. Nine tests remained skipped.
The command is `cargo +1.98 nextest run --locked -p deltalake-core --features datafusion --lib --test it_datafusion --test variant --test-threads 4 --no-fail-fast --status-level fail`.
This is not an all-feature or complete Timefusion check.
Existing compiler, linker, and dependency warnings remain. No CI attestation was published.

The source inventory confirms that Timefusion still requires unported deletion-vector write APIs.
These include `UpdateBuilder::with_deletion_vectors`, `DeleteBuilder::with_deletion_vectors`, and `operations::merge_dv`.
The read-side deletion-vector results do not replace these APIs.
Their ports must retain physical row positions, existing deletion-vector unions, transaction conflicts, and live-file protection during vacuum.

### Incremental snapshot port

The local candidate now includes the fork's snapshot materialization, append advance, removal-aware advance, and bounded catch-up APIs.
The table-state wrappers preserve Timefusion's existing API surface.
The port retains upstream cache identity and statistics-policy checks.
New cache records use the target snapshot identity, rather than a version number alone.
Metadata or protocol changes take the full update path. Newer checkpoints also take the full path to prevent duplicate carried files.
The existing string decoder is shared with its parent module instead of duplicated.
Four restored real-storage regressions compare incremental results with full updates for appends, removals, checkpoint crossings, and metadata changes.
All 113 selected snapshot tests passed in 1.402 seconds after a 2m34s build.
The selection includes the four restored regressions and existing snapshot checks.
The local candidate now connects the commit option to the post-commit hook and forwards it through compaction commits.
The option remains disabled by default.
The fast path requires an adjacent snapshot version because the successful commit's actions cannot describe intervening removals.
Other version gaps use the full update.
The overwrite regression now runs with the option enabled and disabled, against a full-update oracle.
All 164 selected snapshot and transaction tests passed in 1.557 seconds after a 2m30s build.
This includes both overwrite-option cases and the existing snapshot and transaction checks.
The selected suite does not establish compaction resource savings or final Timefusion integration.
The restored snapshot comparisons now inspect file records, rather than paths alone.
The append case also requires non-empty statistics fields.
The first stronger run passed all 164 selected tests in 2.202 seconds after a 3m10s build.
Review then found that the production `Add` equality omits row-tracking and clustering fields.
The tests now compare serialized records, including those fields, without changing production equality.
All 164 selected tests passed with serialized-record comparisons in 1.968 seconds after a 3m08s build.
The run used the same combined snapshot and transaction command. Existing warnings remain.
The initial fixtures did not establish preservation of non-empty deletion vectors, custom tags, or row-tracking values.
The new lifecycle matrix uses the existing real deletion-vector table with the incremental option enabled and disabled.
It requires non-empty deletion vectors, unchanged descriptors after append, exact live rows, and matching results after reload.
Compaction must merge three files into one without resurrecting deleted rows.
Both lifecycle cases and two existing deletion-vector compaction checks passed in 0.294 seconds after an 11.00-second build.
The command was `cargo +1.98 nextest run --locked -p deltalake-core --features datafusion --test it_datafusion --no-fail-fast --status-level fail -E 'test(incremental_snapshot_preserves_deletion_vectors) | test(command_optimize::test_optimize_compaction_)'`.
The fork's public `LogicalFileView::tags` accessor now reuses the upstream tag decoder.
Timefusion's sorted-run selection calls this API directly.
The physical-order matrix now includes an incremental append after sorted compaction and checks the persisted sort tag before and after reload.
All 14 selected sort, tag, and deletion-vector tests passed in 0.445 seconds after a 1m15s build.
The command used the same package, feature, and target with `-E 'test(optimize_sortby) | test(incremental_snapshot_preserves_deletion_vectors) | test(command_optimize::test_optimize_compaction_)'`.
The result includes persisted sort tags through incremental append and reload in all four physical-order cases.
Non-empty row-tracking values still require a separate fixture. Existing warnings and full-stack resource gates remain open.
Its command is `cargo +1.98 nextest run --locked -p deltalake-core --features datafusion --lib --no-fail-fast --status-level fail -E 'test(kernel::snapshot::) | test(kernel::transaction::)'`.
The command is `cargo +1.98 nextest run --locked -p deltalake-core --features datafusion --lib --no-fail-fast --status-level fail -E 'test(kernel::snapshot::)'`.
Formatting and whitespace checks passed. No warning suppression or unsafe operation was added.
Existing compiler, linker, and dependency warnings remain.
Resource measurements, failure recovery, concurrent-commit integration, and final-stack integration remain required.

1. Inventory fork-only changes against exact upstream bases.
2. Port retained changes in isolated checkouts.
3. Run the original regressions against each port.
4. Resolve the complete dependency graph, including sibling workspace crates.
5. Adapt Timefusion callers without weakening correctness contracts.
6. Run Rust reviews, formatting, lint, and relevant integration suites.
7. Compare representative resource use and latency against the current stack.
8. Publish reviewed fork revisions and pin them consistently.
9. Build the release from the committed manifest and lockfile.
10. Apply the existing canary and rollback gates before production deployment.

No force-push or rewrite of an existing shared branch is required.

The kernel release requires a separate source audit.
The published 0.28.1 crate supports Arrow 59 and records source revision `b7cedb67edf8fad95603117a7bc7b613d6656454`.
The Buoyant repository API did not resolve that revision.
The Delta upstream API also did not resolve it, and a direct Git fetch returned `not our ref`.
Its `buoyant/dev` revision `ab66589067127b81c87690370e9b2c6e5129bcdf` instead declares version 0.28.0.
That unmodified revision cannot satisfy Delta's 0.28.1 requirement.
The current Timefusion kernel fork also contains executor-delivery and schema-cache fixes that need preservation.

The source audit compared that revision with the downloaded registry packages.
Kernel, engine, and derive source files match their published 0.28.1, 0.28.0, and 1.2.0 packages.
The original package manifests also match, as does the kernel build script.
The published kernel includes extra backup and diagnostic files. The candidate does not copy those files.
The Git tree retains test data that the published package excludes.

The candidate worktree is `/tmp/timefusion-dependency-upgrade.VYaIwA/delta-kernel-028`.
Its initial manifest change sets the kernel package version explicitly to 0.28.1.
The workspace version remains 0.28.0, so the engine keeps its published version.
The engine's existing kernel requirement accepts 0.28.1.
`cargo +1.98 metadata --format-version 1 --no-deps --offline` confirms the three package versions.
This is a documented reconstruction from matching source, not recovery of the unavailable release commit.
Dependency resolution, native regressions, cache ports, and full integration remain required before any final fork pin.

The initial schema-cache port now has a regression for target replacement within the same boxed allocation.
The old cache records `last_schema_ptr` without retaining the target object.
A matching address bypasses the exact schema comparison, even though the caller can replace that schema in place.
The regression warms an identity entry, replaces the target with a renamed schema, and requires the new column name with unchanged values.
The regression reproduced the defect: the output column remained `b` instead of `renamed`.
The run passed the three existing cache tests and failed the new case in 0.063 seconds after a 3m03s build.
The correction removes the unowned target-address shortcut and compares the exact cached target schema on every hit.
The input allocation remains pinned. The cache remains thread-local and bounded, with no new lock or per-hit allocation.
Exact comparison adds schema-walk work that requires measurement. The evaluator-level cache uses the separate guard described later in this document.
All 28 tests in the broader `test(apply_schema)` selection passed in 0.101 seconds after a 55.25-second build.
The selection includes cache reuse, field-ID conflicts, nested metadata, null handling, Variant representations, and schema-evolution cases.
Existing unused-code warnings remain. This result is not full-suite, lint, performance, or final-graph signoff.
These targeted checks pass. Full dependency integration remains open.
The Delta baseline uses registry packages and does not include this separate local cache candidate.
The original cache regressions are retained alongside the new replacement case.
The changed file passes its nightly formatter. Workspace formatting also reports differences in unchanged Parquet and test-helper files.
The kernel guide still lists the old package names. Commands must use the actual `buoyant_kernel` package name.
The native cache regression command is `cargo +1.98 nextest run -p buoyant_kernel --lib --features arrow-59,default-engine-base --no-fail-fast -E 'test(identity_cache_tests) | test(cached_schema_replacement_at_same_address_preserves_renaming)'`.
The kernel tests used the workspace lockfile, which selects Arrow 59.0.0.
The kernel test helpers also build Arrow 58 dependencies.
These native results will not replace integration with the final Arrow 59.3 dependency graph.
Cargo changed only the local kernel and derive package versions in this workspace lockfile.
The derive change reconciles the previously stale lock entry with its existing 1.2.0 manifest.

Local paths are permitted only for integration experiments, not the release artifact.
No server scaling or production activation accompanies this upgrade.

## Current work and findings

Isolated upgrade checkouts are under `/tmp/timefusion-dependency-upgrade.VYaIwA`.
The DataFusion branch is `timefusion-upgrade-55.1`, based on the 55.1.0 release.
Delta and Postgres source histories are available for the patch inventory.

The output-metric patch from `41162dd13` applies to DataFusion 55.1.
The first filter selected no tests, so that invocation provides no test evidence.
The complete `datafusion-physical-expr-common` library selection then passed all 75 tests in 4.186 seconds.
The command was `cargo +1.98 nextest run --locked -p datafusion-physical-expr-common --lib`.
This package result does not establish full-stack compatibility or a performance benefit.

Upstream added `RecordBatchMemoryCounter` and replaced the old blocking spill reader with an asynchronous decoder.
The newer reader retains an `Arc<dyn SpillFile>`, so the old ownership patch cannot be replayed mechanically.
It retains that owner as a field even after EOF or an error.
The upgraded regression must establish release timing and disk charges through those paths.

The ported real-file regression failed at EOF on 55.1: disk usage remained 2,536 bytes instead of zero.
The cancellation case passed before the EOF assertion failed.
The run finished in 0.032 seconds after a 1m39s build.
This proves retained disk charges after completion, not the earlier uncharged-open-file defect.

The candidate replaces the completion flag with an optional active reader.
The active reader owns the decoder, byte stream, buffered bytes, and spill-file reference.
EOF and errors release that state together. Pending reads and successful batches retain it.
The byte stream drops before the file owner. No extra allocation, lock, or file handle is added.
All 45 native spill tests passed in 0.300 seconds after a 35.31-second build.
The command selected `test(spill::)` in `datafusion-physical-plan` with `cargo +1.98 nextest run --locked --lib --no-fail-fast`.
The lifecycle regression passed cancellation, EOF cleanup, and schema-mismatch cleanup.
Strict all-target, all-feature Clippy stopped in unchanged upstream code under Rust 1.98.
The finding is `chunks_exact_to_as_chunks` at `datafusion/common/src/utils/hex.rs:156`.
No suppression was added. The dependency lint gate remains open.

The Parquet source now retains the fork's explicit file-repartitioning control.
Ordinary scans allow splitting by default. Positional consumers can disable splitting without disabling parallel reads of separate files.
A new regression exercises the real file-group planner with splitting disabled and enabled.
It requires no split for positional mode, four groups for ordinary mode, and an unchanged original configuration.
The first Parquet test build rejected an unqualified `Result` in the new fixture.
The fixture now uses `datafusion_common::Result`.
All 25 Parquet source tests passed in 0.144 seconds after a 1m02s build.
The selection includes the new positional-splitting regression and existing ordering and predicate tests.

The sort-pushdown capability now connects to the relocated 55.1 optimizer.
The port preserves the default behavior for ordinary operators.
An operator that maintains row order can independently forbid sorting its input.
The original optimizer snapshot regression is ported with its upstream test fixture.
The fixture preserves the capability when 55.1 replaces children and recomputes properties.
This plan-level regression does not replace real Delta row-mask tests.
All 136 selected `core_integration` sort-ordering tests passed in 2.987 seconds after a 5m04s build.
The selection includes the ported boundary regression and upstream monotonicity cases.
The linker reported an oversized unwind-section warning. This is not a warning-free build or full-suite signoff.
DataFusion 55.1 moved `QueryPlanner` into `datafusion-session/src/planner.rs`.
The `UPDATE FROM` port extends that trait and retains the session-provider capability check.
Default planners still reject the joined update. A custom planner must explicitly advertise support.

The deallocation port distinguishes named statements from `ALL` with an enum.
It clears prepared plans without resetting session configuration.
The retained regression covers a quoted statement named `all`, repeated deallocation, session isolation, and preserved batch-size configuration.
All 33 selected SQL API and custom DML tests passed in 0.222 seconds after a 4m02s build.
The linker repeated the unwind-section warning.
Successful joined updates through the upgraded Delta planner and Postgres protocol checks remain required.

The correlated-UNNEST check now includes a real SQL regression before the planner port.
It requires repeated parent rows for list elements, preserved null elements, and no rows for empty or null lists.
The baseline failed with `OuterReferenceColumn` unsupported during physical planning.
The run finished in 0.067 seconds after a 4m49s build.
The planner port now maps the supported cross-join form to native row expansion.
The original SQL logic regression remains alongside the upstream unsupported-query cases.
All 31 selected SQL API and UNNEST core tests passed in 0.444 seconds after a 2m06s build.
The selection includes the new correlated-query regression and existing array-expansion cases.
The linker repeated the unwind-section warning.
The standalone SQL logic file and full Timefusion query suite remain unchecked.

The isolated Delta worktree is `/tmp/timefusion-dependency-upgrade.VYaIwA/delta-rs-55`, on branch `timefusion-upgrade-55`.
It starts at candidate revision `29db75879be8df185fae765cee4c801f344093c2`.
The existing Variant fixture now covers updates and deletes with view types enabled and disabled.
It checks affected-row metrics, surviving identities, Variant payloads, and retained snapshot reads.
The baseline run uses the candidate upstream dependencies before any fork behavior changes.
Its generated lockfile resolves DataFusion 55.1.0, Arrow 59.3.0, kernel 0.28.1, and kernel engine 0.28.0.
The lockfile SHA-256 is `2abd55a7d1ea7c6e96dbbf41cd8bf662b488c41b98b0df782d25ef6f74cf227f`.
The command is `cargo +1.98 nextest run -p deltalake-core --features datafusion --test variant --no-fail-fast -E 'test(test_write_variant_data_end_to_end)'`.
The first run failed both cases before Variant checks because the fixture supplied a session without Delta's custom planner.
The corrected fixture uses `DeltaSessionContext::new_with_session_overrides` and retains both view-type cases and all result assertions.
The second run completed mutations but failed on duplicate table registration in the snapshot-check loop.
The fixture now deregisters the previous snapshot before registering the next one.
Both cases then passed in 0.201 seconds after a 5.91-second build.
The candidate preserves checked identities and Variant payloads through writes, updates, deletes, and retained snapshot reads without the old normalization patches.
This supports replacement by upstream behavior for these cases, not retirement across every write path.
Merge rewrites and the full Timefusion workload still require checks.
The fixture now includes partitioned and unpartitioned tables as a second independent test axis.
Its update changes the partition key, so the expanded cases also exercise movement between partitions.
All four cases passed in 0.526 seconds after a cached 0.72-second build.
The run used the same command with `--locked`.
The result covers partitioned and unpartitioned tables, including updates that move rows between partitions.
It does not establish deletion-vector correctness or replacement of every fork-specific Variant path.
The build reported existing unused-code and deprecated-reader warnings, plus a linker warning and a dependency future-compatibility warning.
No warning suppression was added.

The published `buoyant_kernel_engine 0.28.0` already sends executor results directly through the standard unbounded channel.
Both executor implementations avoid the nested blocking task from our old deadlock fix.
The published source also contains the small-blocking-pool regression.
All eight native executor tests passed in 0.076 seconds after a 2m08s build.
The selection includes the small-blocking-pool deadlock regression.
The command was `cargo +1.98 nextest run --locked -p buoyant_kernel_engine --lib --features arrow-59 --no-fail-fast --status-level fail -E 'test(executor::)'`.
The old executor-delivery patch is replaced by tested upstream behavior, rather than replayed.
Final-stack integration remains required.
The package records source revision `63258a5e311c8cf56a6a209848716b429f19da02`.
The kernel evaluator cache port now compares the actual evaluated output fields.
The old evaluator cache reuses its verdict for batches with the same input-Fields allocation.
Its output type and expression remain fixed, but opaque expressions supply their own Arrow output.
The opaque-expression interface does not establish an identical output schema across batches with different values.
This is an unresolved assumption in the old shortcut, not a reproduced production failure.
The port retains a single verdict in `OnceLock` and compares output fields before it bypasses the transform.
Changed output fields take the full transform. The fixed target type does not need a separate address guard.
This preserves the optimization without an input-schema determinism assumption or a new shared lock.

The regression compares cached results with the full transform through real Arrow expressions.
Four cases combine struct projections and column expressions with identity and metadata-changing transforms.
Each case covers repeated batches, changed source metadata, null values, and an empty batch.
All 250 selected expression, schema-application, and type-validation tests passed in 0.638 seconds after a 31.01-second build.
The command was `cargo +1.98 nextest run --locked -p buoyant_kernel --lib --features arrow-59,default-engine-base --no-fail-fast -E 'test(arrow_expression) | test(apply_schema) | test(ensure_data_types)'`.
The Rust review found no new suppression, unsafe operation, production panic, or public API change in the evaluator port.
The tests reuse real expressions and a parameterized case matrix, without mocks or a duplicate fixture.
Exact field comparison has an unmeasured CPU cost. Full-stack resource checks and strict lint remain open.

The new schema transform also checks field-ID conflicts and translates nested field-ID metadata.
Cache ports must preserve those checks, Variant handling, and null propagation.
A pointer cache or matching function name alone does not establish equivalent behavior.

The Rust review found no added suppression, unsafe operation, production panic, or widened test-only API in this port.
The existing unsafe decoder configuration is unchanged.
The test uses real spill files and checks data, disk charges, file deletion, and terminal behavior.
Cancellation during an outstanding filesystem operation and full Timefusion integration remain separate gates.

Before this upgrade, the expanded DataFusion 54 spill suite passed all 45 tests in 0.164 seconds.
That run includes schema-mismatch cleanup before stream drop.
The saved [old-stack patch](../../patches/datafusion/README.md) remains evidence and a porting reference, not the final dependency revision.
