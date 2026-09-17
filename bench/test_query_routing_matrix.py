import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from bench.query_routing_matrix import Run, ambient_miss_reasons, attributed_route, explain, explain_route_hint, load_manifest, result_digest, stats_delta, stop_reason, validate_sql, window_bounds


class ExplainCursor:
    def execute(self, sql, params):
        self.sql, self.params = sql, params

    def fetchall(self):
        return [("logical_plan", "Projection"), ("physical_plan", "DataSourceExec table=otel_logs_and_spans")]


class QueryRoutingMatrixTest(unittest.TestCase):
    def test_explain_uses_plan_text_from_timefusion_two_column_rows(self):
        cursor = ExplainCursor()
        self.assertEqual(explain(cursor, "SELECT 1", {}), ["Projection", "DataSourceExec table=otel_logs_and_spans"])

    def test_only_one_read_statement_is_accepted(self):
        self.assertEqual(validate_sql(" SELECT 1; "), "SELECT 1")
        self.assertEqual(validate_sql("WITH x AS (SELECT 1) SELECT * FROM x"), "WITH x AS (SELECT 1) SELECT * FROM x")
        self.assertEqual(validate_sql("SELECT 'update'"), "SELECT 'update'")
        for sql in ("DELETE FROM t", "SELECT 1; DROP TABLE t", "WITH gone AS (DELETE FROM t RETURNING *) SELECT * FROM gone"):
            with self.assertRaises(ValueError):
                validate_sql(sql)

    def test_result_digest_is_stable_for_pg_values(self):
        rows = [(datetime(2026, 1, 1, tzinfo=timezone.utc), Decimal("1.20"), b"a")]
        self.assertEqual(result_digest(rows), result_digest(list(rows)))
        self.assertEqual(result_digest(rows), result_digest([(datetime(2026, 1, 1, tzinfo=timezone.utc), Decimal("1.2"), b"a")]))
        self.assertEqual(result_digest([(2,), (1,)], ordered=False), result_digest([(1,), (2,)], ordered=False))

    def test_stats_delta_and_route_keep_miss_reason(self):
        before = {"maintenance.rollup_hits_full_total": "4", "maintenance.rollup_miss_unknown_filter_total": "2", "parquet.bytes_read": "10"}
        after = {"maintenance.rollup_hits_full_total": "4", "maintenance.rollup_miss_unknown_filter_total": "3", "parquet.bytes_read": "42"}
        delta = stats_delta(before, after)
        self.assertEqual(ambient_miss_reasons(delta), ["unknown_filter"])
        self.assertEqual(explain_route_hint(["logical raw", "DataSourceExec table=otel_logs_and_spans"]), "raw")
        self.assertEqual(explain_route_hint(["logical raw", "DataSourceExec table=otel_logs_and_spans_rollup_dashboard_1h_v2"]), "full")
        self.assertEqual(
            explain_route_hint(["logical raw", "otel_logs_and_spans otel_logs_and_spans_rollup_dashboard_1h_v2"]),
            "hybrid",
        )
        self.assertIsNone(attributed_route({"maintenance.rollup_hits_full_total": 1}, isolated=False))
        self.assertEqual(attributed_route({"maintenance.rollup_hits_full_total": 1}, isolated=True), "full")
        self.assertIsNone(attributed_route({"maintenance.rollup_hits_full_total": 1, "maintenance.rollup_misses_total": 1}, isolated=True))
        self.assertEqual(delta["parquet.bytes_read"], 32)

    def test_bounds_distinguish_live_and_sealed_windows(self):
        now = datetime(2026, 9, 17, 13, tzinfo=timezone.utc)
        live = window_bounds(now, "1h", "today", 2)
        sealed = window_bounds(now, "1d", "sealed", 2)
        self.assertEqual(live[1], now)
        self.assertEqual(sealed[1], datetime(2026, 9, 15, tzinfo=timezone.utc))

    def test_stop_conditions_are_bounded(self):
        self.assertEqual(stop_reason(Run("q", "7d", "sealed", "first", "", "", explain_route_hint="raw"), 100), "long_window_route_unproven")
        self.assertEqual(
            stop_reason(Run("q", "1d", "sealed", "first", "", "", ambient_physical_bytes_read_upper_bound=101), 100),
            "ambient_scan_budget_exceeded",
        )
        self.assertEqual(stop_reason(Run("q", "1h", "sealed", "first", "", "", result_equal=False), 100), "oracle_mismatch")

    def test_manifest_rejects_duplicate_names_and_unknown_windows(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_text('{"queries":[{"name":"q","project_id":"p","sql":"SELECT 1","windows":["2d"]}]}')
            with self.assertRaisesRegex(ValueError, "unknown windows"):
                load_manifest(path)


if __name__ == "__main__":
    unittest.main()
