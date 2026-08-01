//! Process-wide clock used by eviction/flush.
//!
//! Two modes, selected at runtime:
//!   - **Wall** (default): `now_micros()` returns `chrono::Utc::now()`.
//!   - **Frozen**: a fixed micros value is stored in an `AtomicI64`; tests
//!     can step it forward to simulate long time windows in seconds.
//!
//! Backwards-compat: the previous env-only `TIMEFUSION_FROZEN_TIME` knob
//! still works via `init_from_env()` — it just installs the initial frozen
//! value. Runtime mutators (`set_micros`, `advance_micros`, `unfreeze`)
//! are wired into SQL UDFs in `functions.rs` so test harnesses can drive
//! the clock over a normal PGWire connection.

use std::sync::atomic::{AtomicI64, Ordering};

/// Sentinel meaning "no frozen value installed; use wall clock". We pick
/// `i64::MIN` because no realistic micros-since-epoch value can collide.
const WALL_SENTINEL: i64 = i64::MIN;

static FROZEN_NOW: AtomicI64 = AtomicI64::new(WALL_SENTINEL);

fn frozen_micros() -> Option<i64> {
    Some(FROZEN_NOW.load(Ordering::Acquire)).filter(|&v| v != WALL_SENTINEL)
}

pub fn init_from_env() {
    let Ok(s) = std::env::var("TIMEFUSION_FROZEN_TIME") else { return };
    let t = chrono::DateTime::parse_from_rfc3339(&s).unwrap_or_else(|e| panic!("TIMEFUSION_FROZEN_TIME must be RFC3339 ({s:?}): {e}")).timestamp_micros();
    set_micros(t);
    tracing::warn!(frozen_at = %s, "TIMEFUSION_FROZEN_TIME set; clock is frozen (test mode)");
}

#[inline]
pub fn now_micros() -> i64 {
    frozen_micros().unwrap_or_else(|| chrono::Utc::now().timestamp_micros())
}

/// Today's UTC date on the (possibly frozen) clock. Maintenance that decides
/// which partitions are sealed must read this rather than `Utc::now`, or a
/// frozen-clock test sees a date its fixture data never lands in.
pub fn today_utc() -> chrono::NaiveDate {
    chrono::DateTime::from_timestamp_micros(now_micros()).unwrap_or_default().date_naive()
}

/// True when the clock is currently pinned (test mode).
pub fn is_frozen() -> bool {
    frozen_micros().is_some()
}

/// Install or replace the frozen time (test mode). Returns the new value.
pub fn set_micros(t: i64) -> i64 {
    FROZEN_NOW.store(t, Ordering::Release);
    t
}

/// Advance the frozen time by `delta_micros`. If the clock is *not* frozen,
/// this freezes it at `wall_now + delta_micros` so the first call from an
/// unprimed test harness has predictable behavior. Returns new value.
pub fn advance_micros(delta_micros: i64) -> i64 {
    set_micros(now_micros().saturating_add(delta_micros))
}

/// Switch back to wall-clock mode.
pub fn unfreeze() {
    FROZEN_NOW.store(WALL_SENTINEL, Ordering::Release);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_advance() {
        // Use a far-future timestamp so we never collide with wall-clock.
        let t0 = 4_000_000_000_000_000_i64;
        set_micros(t0);
        assert_eq!(now_micros(), t0);
        let t1 = advance_micros(60_000_000);
        assert_eq!(t1, t0 + 60_000_000);
        assert_eq!(now_micros(), t1);
        unfreeze();
        assert!(!is_frozen());
    }
}
