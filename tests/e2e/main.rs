//! E2E test binary: one Cargo target so all scenarios share a single MinIO harness.

mod harness;

mod bulk_load;
mod cache_warmth;
mod deletion_vectors;
mod dml_compression;
mod eviction;
mod flush_lifecycle;
mod flush_sort_cost;
mod flush_sort_escalation;
mod flush_warm;
mod hash_enrichment;
mod heavy_query_admission;
mod hot_tail_sorted_footer;
mod insert_unnest_scaling;
mod insert_unnest_variant;
mod merge_on_read;
mod multi_tenant_isolation;
mod or_utf8view_delta;
mod ordering_pushdown;
mod partition_pruning;
mod postcommit_hooks;
mod pressure_flush;
mod recent_window_pruning;
mod repair_resume;
mod restart_recovery;
mod smoke;
mod staged_commit;
mod wide_scan_gate;
mod zorder_idempotence;
