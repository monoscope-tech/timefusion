#!/usr/bin/env python3
"""Explain maintenance queue movement without treating estimates as physical work.

Input is a bounded JSON/JSONL export of structured TimeFusion events.  Event
objects may be Monoscope detail records (fields under ``attributes``) or flat
objects.  Queue snapshots use ``event=maintenance_queue_snapshot`` and a
``backlog_bytes`` field.  The optional ledger is the JSON output of
``delta_work_ledger.py``.

Examples:

    bench/maintenance_task_flow.py events.jsonl --ledger ledger.json
    bench/maintenance_task_flow.py events.json --json

The report never converts estimated decoded bytes into committed bytes.  It
accounts for observable estimate replacement, splitting, enqueue, completion,
and supersession.  Aggregate events that omit task identity remain explicit
coverage gaps.  Two durable task-journal snapshots can also be compared;
optional WAL files are replayed so the comparison includes changes after each
snapshot.  Derived work is intentionally absent from that journal, so queue
snapshots expose the in-memory remainder instead of silently attributing it to
durable tasks.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable


TERMINAL_OUTCOMES = {"Complete", "Superseded"}
AGGREGATE_EVENTS = {
    "maintenance_compaction_debt_planned",
    "maintenance_rollup_backfill_planned",
    "maintenance_hygiene_tasks_retired",
    "maintenance_undeclared_tier_tasks_retired",
    "maintenance_sealed_slices_coarsened",
    "rollup_moved_slices_requeued",
    "rollup_unverifiable_rebuild_queued",
}


@dataclass(frozen=True, order=True)
class TaskId:
    operation: str
    table: str
    project_id: str
    slice_start: int
    slice_end: int
    source: str = "?"
    hash_shard: int = 0
    hash_shards: int = 1

    @classmethod
    def from_attributes(cls, attrs: dict[str, Any]) -> "TaskId | None":
        required = ("operation", "table", "project_id", "slice_start", "slice_end")
        if any(attrs.get(name) is None for name in required):
            return None
        return cls(
            operation=str(attrs["operation"]),
            table=str(attrs["table"]),
            project_id=str(attrs["project_id"]),
            slice_start=int(attrs["slice_start"]),
            slice_end=int(attrs["slice_end"]),
            source=str(attrs.get("source") or "?"),
            hash_shard=int(attrs.get("hash_shard") or 0),
            hash_shards=int(attrs.get("hash_shards") or 1),
        )

    def label(self) -> str:
        shard = f"#{self.hash_shard}/{self.hash_shards}" if self.hash_shards > 1 else ""
        return f"{self.operation}:{self.table}:{self.project_id}:{self.slice_start}..{self.slice_end}{shard}"


@dataclass
class TaskState:
    estimate: int
    attempts: int = 0
    starts: int = 0
    finishes: int = 0
    outcome: str = "Open"
    synthetic: bool = False


@dataclass
class FlowReport:
    events: int = 0
    task_events: int = 0
    enqueued_bytes: int = 0
    estimate_replacement_bytes: int = 0
    retired_bytes: int = 0
    opening_bytes_observed: int = 0
    opening_tasks_observed: int = 0
    terminal_tasks: int = 0
    retry_finishes: int = 0
    splits: int = 0
    missing_identity_events: int = 0
    partial_identity_events: int = 0
    missing_estimate_events: int = 0
    duplicate_starts: int = 0
    snapshot_first: int | None = None
    snapshot_last: int | None = None
    snapshot_delta: int | None = None
    accounted_delta: int = 0
    unexplained_delta: int | None = None
    aggregate_gaps: dict[str, int] = field(default_factory=dict)
    physical: dict[str, dict[str, int]] = field(default_factory=dict)
    journal: dict[str, Any] = field(default_factory=dict)
    groups: list[dict[str, Any]] = field(default_factory=list)
    open_tasks: list[dict[str, Any]] = field(default_factory=list)


def load_objects(path: Path) -> list[dict[str, Any]]:
    text = path.read_text()
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    if isinstance(value, list):
        return value
    if isinstance(value, dict) and isinstance(value.get("events"), list):
        return value["events"]
    if isinstance(value, dict):
        return [value]
    raise ValueError(f"{path}: expected a JSON object, array, or JSONL")


def event_attributes(event: dict[str, Any]) -> dict[str, Any]:
    attrs = event.get("attributes")
    if isinstance(attrs, dict):
        return attrs
    return event


def timestamp_key(event: dict[str, Any]) -> tuple[str, int]:
    return str(event.get("timestamp") or ""), int(event.get("start_time_ns") or 0)


def nonnegative_int(attrs: dict[str, Any], field_name: str) -> int | None:
    value = attrs.get(field_name)
    if value is None:
        return None
    value = int(value)
    return value if value >= 0 else None


def child_tasks(parent: TaskId, estimate: int) -> tuple[tuple[TaskId, int], tuple[TaskId, int]] | None:
    width = parent.slice_end - parent.slice_start
    if width <= 1:
        return None
    midpoint = parent.slice_start + width // 2
    left = estimate * (midpoint - parent.slice_start) // width
    make = lambda start, end: TaskId(parent.operation, parent.table, parent.project_id, start, end, parent.source, parent.hash_shard, parent.hash_shards)
    return (make(parent.slice_start, midpoint), left), (make(midpoint, parent.slice_end), estimate - left)


def ledger_totals(path: Path | None) -> dict[str, dict[str, int]]:
    if path is None:
        return {}
    data = json.loads(path.read_text())
    totals: dict[str, Counter[str]] = defaultdict(Counter)
    for row in data.get("records", []):
        lane = str(row.get("lane") or "unknown")
        for name in ("commits", "parquet_files_written", "parquet_rows_written", "parquet_bytes_written", "dv_sidecars_written", "dv_bytes_written"):
            totals[lane][name] += int(row.get(name) or 0)
    return {lane: dict(values) for lane, values in sorted(totals.items())}


def journal_key(task_or_key: dict[str, Any]) -> tuple[Any, ...]:
    key = task_or_key.get("key", task_or_key)
    slice_ = key.get("slice") or {}
    return (
        str(key.get("operation") or "?"),
        str(key.get("physical_table") or "?"),
        str(key.get("source") or "?"),
        str(key.get("project_id") or "?"),
        int(slice_.get("start_micros") or 0),
        int(slice_.get("end_micros") or 0),
    )


def load_journal(snapshot_path: Path, wal_path: Path | None = None) -> dict[tuple[Any, ...], dict[str, Any]]:
    snapshot = json.loads(snapshot_path.read_text())
    tasks = {journal_key(task): task for task in snapshot.get("tasks", [])}
    if wal_path is None:
        return tasks
    lines = wal_path.read_bytes().splitlines(keepends=True)
    for index, raw in enumerate(lines):
        if not raw.endswith(b"\n"):
            if index == len(lines) - 1:
                break
            raise ValueError(f"{wal_path}: torn record before final line")
        record = json.loads(raw)
        if "task" in record:
            tasks[journal_key(record["task"])] = record["task"]
        elif "removed" in record:
            tasks.pop(journal_key(record["removed"]), None)
    return tasks


def active(task: dict[str, Any] | None) -> bool:
    return task is not None and str(task.get("state") or "").lower() in {"pending", "running", "retry"}


def task_estimate(task: dict[str, Any] | None) -> int:
    return int((task or {}).get("estimated_decoded_bytes") or 0)


def compare_journals(before: dict[tuple[Any, ...], dict[str, Any]], after: dict[tuple[Any, ...], dict[str, Any]]) -> dict[str, Any]:
    first = sum(task_estimate(task) for task in before.values() if active(task))
    last = sum(task_estimate(task) for task in after.values() if active(task))
    added_bytes = retired_bytes = repriced_bytes = 0
    added_tasks = retired_tasks = repriced_tasks = 0
    transitions: Counter[str] = Counter()
    groups: dict[tuple[str, str, str], Counter[str]] = defaultdict(Counter)

    for key in before.keys() | after.keys():
        earlier, later = before.get(key), after.get(key)
        was_active, is_active = active(earlier), active(later)
        before_state = str((earlier or {}).get("state") or "absent").lower()
        after_state = str((later or {}).get("state") or "absent").lower()
        if before_state != after_state:
            transitions[f"{before_state}->{after_state}"] += 1
        group = groups[(key[0], key[1], key[3])]
        if not was_active and is_active:
            value = task_estimate(later)
            added_bytes += value
            added_tasks += 1
            group.update(added_tasks=1, added_bytes=value)
        elif was_active and not is_active:
            value = task_estimate(earlier)
            retired_bytes += value
            retired_tasks += 1
            group.update(retired_tasks=1, retired_bytes=value)
        elif was_active and is_active:
            delta = task_estimate(later) - task_estimate(earlier)
            if delta:
                repriced_bytes += delta
                repriced_tasks += 1
                group.update(repriced_tasks=1, repriced_bytes=delta)

    rows = []
    for (operation, table, project_id), values in groups.items():
        values["net_bytes"] = values["added_bytes"] + values["repriced_bytes"] - values["retired_bytes"]
        rows.append({"operation": operation, "table": table, "project_id": project_id, **dict(values)})
    rows.sort(key=lambda row: abs(row["net_bytes"]), reverse=True)
    explained = added_bytes + repriced_bytes - retired_bytes
    return {
        "before_tasks": len(before),
        "after_tasks": len(after),
        "before_active_tasks": sum(map(active, before.values())),
        "after_active_tasks": sum(map(active, after.values())),
        "before_backlog_bytes": first,
        "after_backlog_bytes": last,
        "backlog_delta": last - first,
        "added_tasks": added_tasks,
        "added_bytes": added_bytes,
        "retired_tasks": retired_tasks,
        "retired_bytes": retired_bytes,
        "repriced_tasks": repriced_tasks,
        "repriced_bytes": repriced_bytes,
        "explained_delta": explained,
        "residual": last - first - explained,
        "state_transitions": dict(sorted(transitions.items())),
        "groups": rows,
    }


def add_live_remainder(journal: dict[str, Any], live_before: int, live_after: int) -> None:
    """Reconcile the live gauge with the durable subset represented by the journal."""
    outside_before = live_before - journal["before_backlog_bytes"]
    outside_after = live_after - journal["after_backlog_bytes"]
    journal.update(
        live_before_backlog_bytes=live_before,
        live_after_backlog_bytes=live_after,
        outside_durable_journal_before_bytes=outside_before,
        outside_durable_journal_after_bytes=outside_after,
        outside_durable_journal_delta=outside_after - outside_before,
        combined_delta_residual=(live_after - live_before)
        - (journal["backlog_delta"] + outside_after - outside_before),
    )


def build_report(events: Iterable[dict[str, Any]], physical: dict[str, dict[str, int]] | None = None) -> FlowReport:
    report = FlowReport(physical=physical or {})
    tasks: dict[TaskId, TaskState] = {}
    groups: dict[tuple[str, str, str], Counter[str]] = defaultdict(Counter)
    snapshots: list[int] = []
    gaps: Counter[str] = Counter()

    for event in sorted(events, key=timestamp_key):
        report.events += 1
        attrs = event_attributes(event)
        kind = str(attrs.get("event") or event.get("event") or event.get("body") or "")
        if kind == "maintenance_queue_snapshot":
            value = nonnegative_int(attrs, "backlog_bytes")
            if value is None:
                report.missing_estimate_events += 1
            else:
                snapshots.append(value)
            continue
        if kind in AGGREGATE_EVENTS:
            gaps[kind] += int(attrs.get("planned") or attrs.get("retired") or attrs.get("queued") or 1)
            continue
        if kind not in {"maintenance_task_enqueued", "maintenance_task_started", "maintenance_task_finished", "maintenance_dedup_task_split"}:
            continue

        report.task_events += 1
        task_id = TaskId.from_attributes(attrs)
        if task_id is None:
            report.missing_identity_events += 1
            continue
        if any(attrs.get(name) is None for name in ("source", "hash_shard", "hash_shards")):
            report.partial_identity_events += 1
        group = groups[(task_id.operation, task_id.table, task_id.project_id)]
        group["events"] += 1

        if kind == "maintenance_task_enqueued":
            estimate = nonnegative_int(attrs, "estimated_decoded_bytes")
            if estimate is None:
                report.missing_estimate_events += 1
                continue
            previous = tasks.get(task_id)
            if previous is None:
                tasks[task_id] = TaskState(estimate=estimate)
                report.enqueued_bytes += estimate
                group["enqueued_bytes"] += estimate
            else:
                delta = estimate - previous.estimate
                previous.estimate = estimate
                report.estimate_replacement_bytes += delta
                group["estimate_replacement_bytes"] += delta
            continue

        if kind == "maintenance_task_started":
            estimate = nonnegative_int(attrs, "estimated_decoded_bytes")
            if estimate is None:
                report.missing_estimate_events += 1
                estimate = 0
            state = tasks.get(task_id)
            if state is None:
                tasks[task_id] = state = TaskState(estimate=estimate)
                report.opening_tasks_observed += 1
                report.opening_bytes_observed += estimate
                group["opening_tasks"] += 1
                group["opening_bytes"] += estimate
            else:
                if state.outcome == "Running":
                    report.duplicate_starts += 1
                delta = estimate - state.estimate
                if delta:
                    report.estimate_replacement_bytes += delta
                    group["estimate_replacement_bytes"] += delta
                    state.estimate = estimate
            state.starts += 1
            state.attempts = max(state.attempts, int(attrs.get("attempts") or 0))
            state.outcome = "Running"
            group["starts"] += 1
            continue

        state = tasks.get(task_id)
        if state is None:
            tasks[task_id] = state = TaskState(estimate=0)
            report.opening_tasks_observed += 1
            report.missing_estimate_events += 1
            group["opening_tasks"] += 1

        if kind == "maintenance_dedup_task_split":
            estimate = nonnegative_int(attrs, "estimated_decoded_bytes")
            if estimate is None:
                report.missing_estimate_events += 1
                continue
            children = child_tasks(task_id, estimate)
            if children is None:
                gaps["split_without_splittable_identity"] += 1
                continue
            delta = estimate - state.estimate
            report.estimate_replacement_bytes += delta
            group["estimate_replacement_bytes"] += delta
            state.outcome = "Superseded"
            state.estimate = 0
            for child_id, child_estimate in children:
                tasks[child_id] = TaskState(estimate=child_estimate, synthetic=True)
            report.splits += 1
            group["splits"] += 1
            continue

        previous_outcome = state.outcome
        outcome = str(attrs.get("outcome") or "Unknown").rsplit("::", 1)[-1]
        if outcome.startswith("Some(") and outcome.endswith(")"):
            outcome = outcome[5:-1]
        state.finishes += 1
        state.outcome = outcome
        group["finishes"] += 1
        if outcome in TERMINAL_OUTCOMES:
            if state.estimate:
                report.retired_bytes += state.estimate
                group["retired_bytes"] += state.estimate
            elif previous_outcome != outcome:
                report.missing_estimate_events += 1
            state.estimate = 0
            report.terminal_tasks += 1
            group["terminal_tasks"] += 1
        elif outcome in {"Pending", "Retry", "Running", "Unknown", "None"}:
            report.retry_finishes += 1
            group["retry_finishes"] += 1
        else:
            gaps[f"unknown_outcome:{outcome}"] += 1

    report.accounted_delta = report.enqueued_bytes + report.estimate_replacement_bytes - report.retired_bytes
    if snapshots:
        report.snapshot_first, report.snapshot_last = snapshots[0], snapshots[-1]
        report.snapshot_delta = snapshots[-1] - snapshots[0]
        report.unexplained_delta = report.snapshot_delta - report.accounted_delta
    report.aggregate_gaps = dict(sorted(gaps.items()))
    report.groups = [
        {"operation": key[0], "table": key[1], "project_id": key[2], **dict(values)}
        for key, values in sorted(groups.items())
    ]
    report.open_tasks = [
        {"task": task_id.label(), **asdict(state)}
        for task_id, state in sorted(tasks.items())
        if state.estimate or state.outcome not in TERMINAL_OUTCOMES
    ]
    return report


def format_bytes(value: int | None) -> str:
    if value is None:
        return "unknown"
    sign = "-" if value < 0 else ""
    value = abs(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{sign}{value:.1f} {unit}" if unit != "B" else f"{sign}{value} B"
        value /= 1024
    raise AssertionError("unreachable")


def render(report: FlowReport) -> str:
    lines = [
        "# Maintenance task-flow accounting",
        "",
        f"- Input events: {report.events} ({report.task_events} identity-bearing task transitions)",
        f"- Observable opening queue: {report.opening_tasks_observed} tasks / {format_bytes(report.opening_bytes_observed)}",
        f"- Enqueued: {format_bytes(report.enqueued_bytes)}",
        f"- Estimate replacement: {format_bytes(report.estimate_replacement_bytes)}",
        f"- Terminal retirement: {format_bytes(report.retired_bytes)}",
        f"- Accounted queue delta: {format_bytes(report.accounted_delta)}",
        f"- Snapshot queue delta: {format_bytes(report.snapshot_delta)}",
        f"- Unexplained residual: {format_bytes(report.unexplained_delta)}",
        f"- Coverage gaps: missing identity={report.missing_identity_events}, partial identity={report.partial_identity_events}, missing estimate={report.missing_estimate_events}, aggregate-only={sum(report.aggregate_gaps.values())}",
    ]
    if report.physical:
        lines += ["", "## Physical commits"]
        for lane, values in report.physical.items():
            lines.append(
                f"- {lane}: {values.get('commits', 0)} commits, {values.get('parquet_rows_written', 0)} rows, "
                f"{format_bytes(values.get('parquet_bytes_written', 0))} Parquet"
            )
    if report.journal:
        journal = report.journal
        lines += [
            "",
            "## Durable-journal subset",
            f"- Backlog: {format_bytes(journal['before_backlog_bytes'])} → {format_bytes(journal['after_backlog_bytes'])} ({format_bytes(journal['backlog_delta'])})",
            f"- Added: {journal['added_tasks']} tasks / {format_bytes(journal['added_bytes'])}",
            f"- Repriced: {journal['repriced_tasks']} tasks / {format_bytes(journal['repriced_bytes'])}",
            f"- Retired: {journal['retired_tasks']} tasks / {format_bytes(journal['retired_bytes'])}",
            f"- Accounting residual: {format_bytes(journal['residual'])}",
        ]
        if "outside_durable_journal_before_bytes" in journal:
            lines += [
                f"- Live queue outside durable journal: {format_bytes(journal['outside_durable_journal_before_bytes'])} → {format_bytes(journal['outside_durable_journal_after_bytes'])}",
                f"- Outside-journal delta: {format_bytes(journal['outside_durable_journal_delta'])}",
                f"- Combined reconciliation residual: {format_bytes(journal['combined_delta_residual'])}",
            ]
    if report.aggregate_gaps:
        lines += ["", "## Aggregate transitions without task identity"]
        lines += [f"- {name}: {count}" for name, count in report.aggregate_gaps.items()]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("events", nargs="?", type=Path, help="bounded JSON, JSON array, or JSONL structured-event export")
    parser.add_argument("--ledger", type=Path, help="JSON output from delta_work_ledger.py for the same half-open window")
    parser.add_argument("--journal-before", type=Path, help="maintenance_tasks.json at the beginning of the window")
    parser.add_argument("--journal-before-wal", type=Path, help="maintenance_tasks.wal captured with --journal-before")
    parser.add_argument("--journal-after", type=Path, help="maintenance_tasks.json at the end of the window")
    parser.add_argument("--journal-after-wal", type=Path, help="maintenance_tasks.wal captured with --journal-after")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    args = parser.parse_args()
    if bool(args.journal_before) != bool(args.journal_after):
        parser.error("--journal-before and --journal-after must be supplied together")
    if args.events is None and args.journal_before is None:
        parser.error("provide an event export or a journal pair")
    report = build_report(load_objects(args.events) if args.events else [], ledger_totals(args.ledger))
    if args.journal_before:
        before = load_journal(args.journal_before, args.journal_before_wal)
        after = load_journal(args.journal_after, args.journal_after_wal)
        report.journal = compare_journals(before, after)
        if report.snapshot_first is not None and report.snapshot_last is not None:
            add_live_remainder(report.journal, report.snapshot_first, report.snapshot_last)
    print(json.dumps(asdict(report), indent=2, sort_keys=True) if args.json else render(report), end="\n" if args.json else "")


if __name__ == "__main__":
    main()
