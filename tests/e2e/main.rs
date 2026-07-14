//! E2E test binary. One Cargo target so testcontainers MinIO startup can be
//! amortized across scenarios and so we can share a single harness module.
//!
//! Run with: `cargo test --test e2e` (requires Docker for MinIO).

mod harness;

mod cache_warmth;
mod eviction;
mod flush_lifecycle;
mod insert_unnest_variant;
mod multi_tenant_isolation;
mod or_utf8view_delta;
mod ordering_pushdown;
mod postcommit_hooks;
mod pressure_flush;
mod restart_recovery;
mod smoke;
mod staged_commit;
mod zorder_idempotence;
