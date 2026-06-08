//! Property-based tests over MemBuffer + clock interactions. Pure in-process
//! (no MinIO) so a property loop is fast. The goal is to shake out
//! invariant violations under sequences a hand-rolled test wouldn't try.
//!
//! Invariants asserted:
//!   - Row count is conserved across insert/query for a fixed (project, table).
//!   - `compute_bucket_id(ts)` is monotonic in `ts` (sanity check).
//!   - `current_bucket_id()` strictly increases when the clock is advanced
//!     by at least one bucket duration.

use proptest::prelude::*;
use timefusion::{clock, mem_buffer::MemBuffer, test_utils::test_helpers};

// Invariants exercised below: row count conservation across inserts,
// monotonic bucket id, strict advancement when the frozen clock jumps.
proptest! {
    #![proptest_config(ProptestConfig { cases: 32, ..ProptestConfig::default() })]

    #[test]
    fn row_count_conserved_across_inserts(
        rows in proptest::collection::vec(1usize..50, 1..10),
    ) {
        // Use a frozen clock so bucket boundaries are deterministic.
        clock::set_micros(1_900_000_000_000_000);

        let buf = MemBuffer::default();
        let ts = clock::now_micros();
        let mut expected: usize = 0;
        for n in &rows {
            let records: Vec<_> = (0..*n)
                .map(|i| serde_json::to_value(test_helpers::test_span_ts(&format!("p-{i}"), "n", "pp", ts)).unwrap())
                .collect();
            let batch = test_helpers::json_to_batch(records).unwrap();
            buf.insert("pp", "otel_logs_and_spans", batch, ts).unwrap();
            expected += n;
        }
        let stats = buf.get_stats();
        prop_assert_eq!(stats.total_rows, expected);

        clock::unfreeze();
    }

    #[test]
    fn compute_bucket_id_monotonic(a in 0i64..i64::MAX / 2, b in 0i64..i64::MAX / 2) {
        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
        prop_assert!(MemBuffer::compute_bucket_id(lo) <= MemBuffer::compute_bucket_id(hi));
    }

    #[test]
    fn current_bucket_id_strictly_advances_when_clock_jumps(
        jumps in proptest::collection::vec(1u32..1000u32, 1..20),
    ) {
        clock::set_micros(1_900_000_000_000_000);
        let bucket_micros: i64 = timefusion::mem_buffer::bucket_duration_micros();
        let mut prev = MemBuffer::current_bucket_id();
        for j in jumps {
            // Jump at least one bucket forward to guarantee a strict increase.
            clock::advance_micros(bucket_micros * (j as i64 + 1));
            let cur = MemBuffer::current_bucket_id();
            prop_assert!(cur > prev, "expected {} > {}", cur, prev);
            prev = cur;
        }
        clock::unfreeze();
    }
}
