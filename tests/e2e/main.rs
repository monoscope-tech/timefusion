//! E2E test binary. One Cargo target so testcontainers MinIO startup can be
//! amortized across scenarios and so we can share a single harness module.
//!
//! Run with: `cargo test --test e2e` (requires Docker for MinIO).

mod harness;

mod bulk_load;
mod cache_warmth;
mod deletion_vectors;
mod dml_compression;
mod eviction;
mod flush_lifecycle;
mod flush_sort_cost;
mod flush_warm;
mod hash_enrichment;
mod hot_tail_sorted_footer;
mod hot_tier;
mod insert_unnest_variant;
mod merge_on_read;
mod multi_tenant_isolation;
mod or_utf8view_delta;
mod ordering_pushdown;
mod partition_pruning;
mod postcommit_hooks;
mod pressure_flush;
mod recent_window_pruning;
mod restart_recovery;
mod smoke;
mod staged_commit;
mod wide_scan_gate;
mod zorder_idempotence;
