//! Single integration-test binary.
//!
//! Every `tests/suite/*.rs` file is a module here rather than its own Cargo
//! target. Each extra target is a separate full link of a ~100 MB binary
//! against 1200+ deps, so 26 targets cost 26 links on every source edit —
//! the dominant term in the edit→test loop. One target links once.
//!
//! Run everything: `cargo nextest run`. Run one file: `cargo nextest run
//! -E 'binary(suite)' <substring>`, e.g. `cargo nextest run dedup_compaction`.

mod buffer_consistency_test;
mod cache_performance_test;
mod connection_pressure_test;
mod dedup_compaction_test;
mod delta_checkpoint_cache_test;
mod delta_rs_api_test;
mod integration_test;
mod jsonb_oid_test;
mod kill_recovery;
mod listen_backlog_test;
mod membuffer_concurrency_bench;
mod merge_date_prune_test;
mod pg_client_compat;
mod pgwire_dml_tag_test;
mod plan_cache_shape_repro;
mod proptest_invariants;
mod sqllogictest;
mod statistics_test;
mod tantivy_e2e_test;
mod tantivy_index_test;
mod tantivy_search_test;
mod tantivy_storage_test;
mod tantivy_transparent_test;
mod test_custom_functions;
mod test_dml_operations;
mod test_postgres_json_functions;
mod unnest_optimizer_regression_test;
