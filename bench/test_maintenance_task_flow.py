import json
import tempfile
import unittest
from pathlib import Path

from bench.maintenance_task_flow import add_live_remainder, build_report, compare_journals, ledger_totals, load_journal, load_objects


def event(kind, **attrs):
    return {"timestamp": attrs.pop("timestamp", "2026-09-17T00:00:00Z"), "attributes": {"event": kind, **attrs}}


TASK = dict(operation="Dedup", table="otel_logs_and_spans", project_id="p", slice_start=0, slice_end=100)


class TaskFlowTest(unittest.TestCase):
    def test_retry_reprices_then_completion_explains_snapshot_swing(self):
        events = [
            event("maintenance_queue_snapshot", backlog_bytes=100),
            event("maintenance_task_started", **TASK, estimated_decoded_bytes=100, attempts=0),
            event("maintenance_task_finished", **TASK, outcome="Retry"),
            event("maintenance_task_started", **TASK, estimated_decoded_bytes=300, attempts=1),
            event("maintenance_task_finished", **TASK, outcome="Complete"),
            event("maintenance_queue_snapshot", backlog_bytes=0, timestamp="2026-09-17T00:01:00Z"),
        ]
        report = build_report(events)
        self.assertEqual((report.estimate_replacement_bytes, report.retired_bytes), (200, 300))
        self.assertEqual((report.snapshot_delta, report.accounted_delta, report.unexplained_delta), (-100, -100, 0))

    def test_split_replaces_parent_with_two_open_children_without_inventing_completion(self):
        report = build_report(
            [
                event("maintenance_task_started", **TASK, estimated_decoded_bytes=100),
                event("maintenance_dedup_task_split", **TASK, estimated_decoded_bytes=400),
                event("maintenance_task_finished", **TASK, outcome="Superseded"),
            ]
        )
        self.assertEqual((report.splits, report.estimate_replacement_bytes, report.retired_bytes), (1, 300, 0))
        self.assertEqual(sorted(task["estimate"] for task in report.open_tasks), [200, 200])
        self.assertEqual(report.missing_estimate_events, 0)

    def test_aggregate_transition_and_missing_identity_stay_coverage_gaps(self):
        report = build_report(
            [
                event("maintenance_hygiene_tasks_retired", retired=7),
                event("maintenance_task_finished", operation="Repair", outcome="Complete"),
            ]
        )
        self.assertEqual(report.aggregate_gaps, {"maintenance_hygiene_tasks_retired": 7})
        self.assertEqual(report.missing_identity_events, 1)
        self.assertEqual(report.retired_bytes, 0)

    def test_partial_observable_identity_is_reported(self):
        report = build_report([event("maintenance_task_finished", **TASK, estimated_decoded_bytes=1, outcome="Some(Complete)")])
        self.assertEqual((report.partial_identity_events, report.terminal_tasks), (1, 1))

    def test_loads_search_envelope_and_keeps_physical_ledger_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = root / "events.json"
            events.write_text(json.dumps({"events": [event("maintenance_queue_snapshot", backlog_bytes=1)]}))
            ledger = root / "ledger.json"
            ledger.write_text(
                json.dumps(
                    {
                        "records": [
                            {"lane": "wave_commit", "commits": 2, "parquet_rows_written": 5, "parquet_bytes_written": 7},
                            {"lane": "wave_commit", "commits": 3, "parquet_rows_written": 11, "parquet_bytes_written": 13},
                        ]
                    }
                )
            )
            self.assertEqual(len(load_objects(events)), 1)
            self.assertEqual(ledger_totals(ledger)["wave_commit"]["parquet_bytes_written"], 20)

    def test_journal_diff_balances_add_reprice_and_retirement(self):
        def task(project, estimate, state="pending"):
            return {
                "key": {
                    "operation": "dedup",
                    "physical_table": "otel_logs_and_spans",
                    "source": "otel_logs_and_spans",
                    "project_id": project,
                    "slice": {"start_micros": 0, "end_micros": 1},
                },
                "state": state,
                "estimated_decoded_bytes": estimate,
            }

        before = {("dedup", "otel_logs_and_spans", "otel_logs_and_spans", key, 0, 1): task(key, value) for key, value in (("done", 100), ("repriced", 20))}
        after = {
            ("dedup", "otel_logs_and_spans", "otel_logs_and_spans", "done", 0, 1): task("done", 100, "complete"),
            ("dedup", "otel_logs_and_spans", "otel_logs_and_spans", "repriced", 0, 1): task("repriced", 50),
            ("dedup", "otel_logs_and_spans", "otel_logs_and_spans", "new", 0, 1): task("new", 200),
        }
        diff = compare_journals(before, after)
        self.assertEqual((diff["added_bytes"], diff["repriced_bytes"], diff["retired_bytes"]), (200, 30, 100))
        self.assertEqual((diff["backlog_delta"], diff["explained_delta"], diff["residual"]), (130, 130, 0))

        add_live_remainder(diff, 1_000, 1_080)
        self.assertEqual(
            (
                diff["outside_durable_journal_before_bytes"],
                diff["outside_durable_journal_after_bytes"],
                diff["outside_durable_journal_delta"],
                diff["combined_delta_residual"],
            ),
            (880, 830, -50, 0),
        )

    def test_journal_wal_replay_ignores_only_a_torn_final_record(self):
        task = {
            "key": {"operation": "dedup", "physical_table": "t", "source": "t", "project_id": "p", "slice": {"start_micros": 0, "end_micros": 1}},
            "state": "pending",
            "estimated_decoded_bytes": 3,
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot, wal = root / "snapshot.json", root / "journal.wal"
            snapshot.write_text(json.dumps({"tasks": []}))
            wal.write_bytes((json.dumps({"task": task}) + "\n" + '{"task":').encode())
            loaded = load_journal(snapshot, wal)
            self.assertEqual(list(loaded.values()), [task])


if __name__ == "__main__":
    unittest.main()
