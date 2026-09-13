//! Single integration-test binary: every `tests/suite/*.rs` file is a module
//! here rather than its own Cargo target, so the large binary links once.

mod bloom_prune_test;
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
mod pgwire_harness;
mod plan_cache_shape_repro;
mod proptest_invariants;
mod query_pool_insert_test;
mod range_split_test;
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
