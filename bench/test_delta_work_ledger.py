import json
import unittest

from bench.delta_work_ledger import ledger, pack_trace


def commit(info, *actions):
    return "\n".join(json.dumps(action) for action in ({"commitInfo": info}, *actions))


class DeltaWorkLedgerTest(unittest.TestCase):
    def test_separates_parquet_rewrites_from_same_path_dv_updates(self):
        physical = commit(
            {"timestamp": 1_000, "operation": "OPTIMIZE", "timefusion.lane": "wave_commit"},
            {"remove": {"path": "old.parquet", "size": 80, "partitionValues": {"project_id": "p", "date": "2026-09-17"}}},
            {
                "add": {
                    "path": "new.parquet",
                    "size": 100,
                    "stats": json.dumps({"numRecords": 12}),
                    "partitionValues": {"project_id": "p", "date": "2026-09-17"},
                }
            },
        )
        dv_update = commit(
            {"timestamp": 2_000, "operation": "WRITE", "timefusion.lane": "dv_dedup"},
            {
                "remove": {
                    "path": "same.parquet",
                    "size": 1_000_000,
                    "partitionValues": {"project_id": "p", "date": "2026-09-17"},
                    "deletionVector": {"pathOrInlineDv": "old", "sizeInBytes": 7},
                }
            },
            {
                "add": {
                    "path": "same.parquet",
                    "size": 1_000_000,
                    "stats": json.dumps({"numRecords": 50_000}),
                    "partitionValues": {"project_id": "p", "date": "2026-09-17"},
                    "deletionVector": {"pathOrInlineDv": "new", "sizeInBytes": 11},
                }
            },
        )

        rows = {row["lane"]: row for row in ledger([(1, physical), (2, dv_update)], ("lane",))}

        self.assertEqual(rows["wave_commit"]["parquet_bytes_written"], 100)
        self.assertEqual(rows["wave_commit"]["parquet_rows_written"], 12)
        self.assertEqual(rows["dv_dedup"]["parquet_bytes_written"], 0)
        self.assertEqual(rows["dv_dedup"]["parquet_rows_written"], 0)
        self.assertEqual(rows["dv_dedup"]["same_path_readds"], 1)
        self.assertEqual(rows["dv_dedup"]["dv_bytes_written"], 11)
        self.assertEqual(rows["dv_dedup"]["dv_bytes_removed"], 7)

    def test_groups_by_lane_project_and_date_and_applies_half_open_time(self):
        before = commit(
            {"timestamp": 999, "operation": "WRITE"},
            {"add": {"path": "before", "size": 1, "stats": "{\"numRecords\":1}", "partitionValues": {}}},
        )
        inside = commit(
            {"timestamp": 1_000, "operation": "WRITE"},
            {
                "add": {
                    "path": "inside",
                    "size": 20,
                    "stats": "{\"numRecords\":2}",
                    "partitionValues": {"project_id": "p1", "date": "2026-09-17"},
                }
            },
        )
        at_end = commit(
            {"timestamp": 2_000, "operation": "WRITE"},
            {"add": {"path": "end", "size": 1, "stats": "{\"numRecords\":1}", "partitionValues": {}}},
        )

        rows = ledger([(1, before), (2, inside), (3, at_end)], ("lane", "project", "date"), 1_000, 2_000)

        self.assertEqual(
            rows,
            [
                {
                    "lane": "unattributed:WRITE",
                    "project": "p1",
                    "date": "2026-09-17",
                    "commits": 1,
                    "add_actions": 1,
                    "remove_actions": 0,
                    "parquet_files_written": 1,
                    "parquet_rows_written": 2,
                    "parquet_bytes_written": 20,
                    "parquet_files_removed": 0,
                    "parquet_bytes_removed": 0,
                    "same_path_readds": 0,
                    "dv_sidecars_written": 0,
                    "dv_bytes_written": 0,
                    "dv_sidecars_removed": 0,
                    "dv_bytes_removed": 0,
                }
            ],
        )

    def test_recognizes_flush_metadata_as_a_lane(self):
        flush = commit(
            {"timestamp": 1_000, "operation": "WRITE", "timefusion.wal_watermark": {}},
            {"add": {"path": "flush", "size": 20, "stats": "{\"numRecords\":2}", "partitionValues": {}}},
        )

        self.assertEqual(ledger([(1, flush)], ("lane",))[0]["lane"], "flush_commit")

    def test_pack_trace_contains_only_sanitized_flush_arrivals(self):
        flush = commit(
            {"timestamp": 1_000, "operation": "WRITE", "timefusion.wal_watermark": {}},
            {
                "add": {
                    "path": "customer-path.parquet",
                    "size": 20,
                    "stats": json.dumps(
                        {
                            "numRecords": 2,
                            "minValues": {"timestamp": "2026-09-17T01:00:00Z"},
                            "maxValues": {"timestamp": "2026-09-17T01:01:00Z"},
                        }
                    ),
                    "partitionValues": {"project_id": "customer-id", "date": "2026-09-17"},
                }
            },
        )
        maintenance = commit(
            {"timestamp": 2_000, "operation": "OPTIMIZE", "timefusion.lane": "wave_commit"},
            {"add": {"path": "output", "size": 1, "stats": "{}", "partitionValues": {}}},
        )

        rows = pack_trace([(1, flush), (2, maintenance)])

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["path"], "v1-a0")
        self.assertNotEqual(rows[0]["project"], "customer-id")
        self.assertNotIn("customer", json.dumps(rows[0]))


if __name__ == "__main__":
    unittest.main()
