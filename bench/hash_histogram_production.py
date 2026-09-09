#!/usr/bin/env python3
"""Compare ordinary and native hash histograms on the saved production workload.

Usage: python3 bench/hash_histogram_production.py <image-tag-or-digest-reference> <output.json>
Requires psycopg, SSH access to the production host, and the sibling monoscope
.env TIMEFUSION_PG_URL. Credentials are neither logged nor saved. This script
runs read-only queries sequentially with a three-second statement timeout.
Global counter deltas are supporting evidence, not per-query attribution.
This is a smoke check, not the full release latency or workload acceptance.
Exit 0 requires all pairs to complete and agree, all native queries to finish
within three seconds, and the expected image to remain on running tasks.
"""
import datetime
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid

import psycopg

ROOT = Path(__file__).resolve().parents[1]

def image():
    return subprocess.check_output([
        'ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
        'ubuntu@captain.s.past3.tech',
        'docker service inspect srv-captain--timefusion --format "{{.Spec.TaskTemplate.ContainerSpec.Image}}"',
    ], text=True).strip()

def running_tasks():
    raw = subprocess.check_output([
        'ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
        'ubuntu@captain.s.past3.tech',
        'docker service ps srv-captain--timefusion --filter desired-state=running --no-trunc --format "{{json .}}"',
    ], text=True)
    return [json.loads(line) for line in raw.splitlines() if line.strip()]

def matches_image(actual, expected):
    return actual == expected if '@sha256:' in expected else actual.split('@')[0].rsplit(':', 1)[-1] == expected

def ready(tasks, expected):
    return bool(tasks) and all(
        matches_image(task['Image'], expected)
        and task['CurrentState'].startswith('Running ')
        for task in tasks
    )

def main():
    expected, destination = sys.argv[1:]
    report = {'at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'image_before': image(), 'tasks_before': running_tasks(), 'queries': [], 'completed_pairs': 0, 'mismatch': False}
    if not matches_image(report['image_before'], expected) or not ready(report['tasks_before'], expected):
        raise SystemExit('Expected image is not deployed; no queries run.')
    url = next(line.split('=', 1)[1].strip().strip('\"\'')
               for line in (ROOT.parent / 'monoscope/.env').read_text().splitlines()
               if line.startswith('TIMEFUSION_PG_URL='))
    windows = [
        ('narrow', '2026-09-02 08:04:30+00', '2026-09-02 08:04:31+00'),
        ('positive_day', '2026-09-02 00:00:00+00', '2026-09-03 00:00:00+00'),
        ('slow_day', '2026-09-06 00:00:00+00', '2026-09-07 00:00:00+00'),
        ('week', '2026-09-02 00:00:00+00', '2026-09-09 00:00:00+00'),
    ]
    tags = [('saved', 'err:e03848c6'), ('absent', 'probe:' + uuid.uuid4().hex)]
    try:
        with psycopg.connect(url, connect_timeout=5, autocommit=True) as connection:
            connection.execute("SET statement_timeout='3s'")
            def stats():
                return connection.execute("SELECT component,key,value FROM timefusion_stats WHERE key LIKE '%histogram%'").fetchall()
            report['stats_before'] = stats()
            for repeat in range(2):
                for label, start, end in windows:
                    for kind, tag in tags:
                        pair = {}
                        arms = ['count(timestamp)', 'count(*)']
                        if repeat % 2:
                            arms.reverse()
                        for aggregate in arms:
                            sql = (f"SELECT time_bucket('1 hour',timestamp), {aggregate} FROM otel_logs_and_spans "
                                   "WHERE project_id='00000000-0000-0000-0000-000000000000' "
                                   f"AND timestamp >= TIMESTAMPTZ '{start}' AND timestamp < TIMESTAMPTZ '{end}' "
                                   f"AND hashes @> ARRAY['{tag}'] GROUP BY 1 ORDER BY 1")
                            sample = {'repeat': repeat, 'window': label, 'tag_kind': kind, 'aggregate': aggregate, 'sql': sql}
                            sample['stats_before'] = stats()
                            began = time.monotonic()
                            try:
                                sample['rows'] = connection.execute(sql).fetchall()
                                pair[aggregate] = sample['rows']
                            except psycopg.Error as error:
                                sample['error'] = {'type': type(error).__name__, 'sqlstate': error.sqlstate}
                            sample['elapsed_ms'] = round((time.monotonic() - began) * 1000, 2)
                            sample['stats_after'] = stats()
                            report['queries'].append(sample)
                            print(repeat, label, kind, aggregate, sample['elapsed_ms'], sample.get('error', sample.get('rows')), flush=True)
                        if len(pair) == 2:
                            report['completed_pairs'] += 1
                            if pair['count(*)'] != pair['count(timestamp)']:
                                report['mismatch'] = True
                        if kind == 'absent' and any(pair.values()):
                            report['mismatch'] = True
                        if report['mismatch']:
                            break
                    if report['mismatch']:
                        break
                if report['mismatch']:
                    break
            report['stats_after'] = stats()
    except psycopg.Error as error:
        report['probe_error'] = {'type': type(error).__name__, 'sqlstate': error.sqlstate}
    report['image_after'] = image()
    report['tasks_after'] = running_tasks()
    report['same_image'] = report['image_before'] == report['image_after'] and ready(report['tasks_after'], expected)
    report['probe_passed'] = (
        report['same_image'] and not report['mismatch'] and 'probe_error' not in report
        and report['completed_pairs'] == 16
        and all('error' not in sample and sample['elapsed_ms'] <= 3000
                for sample in report['queries'] if sample['aggregate'] == 'count(*)')
    )
    Path(destination).write_text(json.dumps(report, indent=2, default=str) + '\n')
    print(destination, 'probe_passed=', report['probe_passed'], 'completed_pairs=', report['completed_pairs'])
    return 0 if report['probe_passed'] else 1

if __name__ == '__main__':
    raise SystemExit(main())
