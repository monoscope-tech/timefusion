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

pub fn init_from_env() {
    if let Ok(s) = std::env::var("TIMEFUSION_FROZEN_TIME") {
        let t = chrono::DateTime::parse_from_rfc3339(&s).unwrap_or_else(|e| panic!("TIMEFUSION_FROZEN_TIME must be RFC3339 ({s:?}): {e}")).timestamp_micros();
        FROZEN_NOW.store(t, Ordering::Release);
        tracing::warn!(
            frozen_at = %chrono::DateTime::from_timestamp_micros(t).unwrap(),
            "TIMEFUSION_FROZEN_TIME set; clock is frozen (test mode)"
        );
    }
}

#[inline]
pub fn now_micros() -> i64 {
    let v = FROZEN_NOW.load(Ordering::Acquire);
    if v == WALL_SENTINEL { chrono::Utc::now().timestamp_micros() } else { v }
}

/// True when the clock is currently pinned (test mode).
pub fn is_frozen() -> bool {
    FROZEN_NOW.load(Ordering::Acquire) != WALL_SENTINEL
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
    let cur = FROZEN_NOW.load(Ordering::Acquire);
    let base = if cur == WALL_SENTINEL { chrono::Utc::now().timestamp_micros() } else { cur };
    let next = base.saturating_add(delta_micros);
    FROZEN_NOW.store(next, Ordering::Release);
    next
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
