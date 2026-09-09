#!/usr/bin/env python3
"""Local PostgreSQL array-index comparison. Creates a new, isolated schema.

Requires psycopg. Pass an explicitly local DSN and an output JSON path.
This is a synthetic mechanism benchmark, not a TimeFusion performance test.
"""
import argparse
import json
import time
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict


def run(dsn, n, output):
    host = conninfo_to_dict(dsn).get("host", "")
    if not (host.startswith("/tmp/") or host in ("127.0.0.1", "localhost")):
        raise ValueError("Use an explicit local host or /tmp Unix socket")
    schema = f"hash_research_{time.time_ns()}"
    results = {"rows": n, "schema": schema, "build": [], "queries": []}
    with psycopg.connect(dsn, autocommit=True) as c:
        results["version"] = c.execute("SELECT version()").fetchone()[0]
        c.execute(f"CREATE SCHEMA {schema}")
        c.execute(f"SET search_path={schema}")
        c.execute("SET timezone='UTC'")
        c.execute("SET statement_timeout='120s'")
        c.execute("SET work_mem='64MB'")
        c.execute("SET max_parallel_workers_per_gather=0")
        c.execute("SET jit=off")
        # Pin parameter-aware planning so EXPLAIN and timed execution use the
        # same policy even after psycopg starts preparing repeated statements.
        c.execute("SET plan_cache_mode=force_custom_plan")
        results["settings"] = {"plan_cache_mode": "force_custom_plan", "work_mem": "64MB",
                               "max_parallel_workers_per_gather": 0, "jit": "off"}

        def build(label, sql):
            start = time.perf_counter()
            c.execute(sql)
            elapsed = round(time.perf_counter() - start, 4)
            results["build"].append({"label": label, "seconds": elapsed})
            print(label, elapsed, flush=True)

        build("events", f"""
          CREATE TABLE events AS
          SELECT i AS id, 'p1'::text AS project_id,
            TIMESTAMPTZ '2026-08-09' + (i-1)*INTERVAL '30 days'/{n} AS ts,
            ARRAY['endpoint:' || CASE WHEN i%10<>0 THEN 'common' ELSE 'other' END]
              || CASE WHEN i%1000=0 THEN ARRAY['err:rare'] ELSE ARRAY[]::text[] END
              || CASE WHEN i%100=0 THEN ARRAY['err:medium'] ELSE ARRAY[]::text[] END
              || CASE WHEN i%200=0 THEN ARRAY['err:overlap'] ELSE ARRAY[]::text[] END AS hashes
          FROM generate_series(1,{n}) i
        """)
        build("event primary key", "ALTER TABLE events ADD PRIMARY KEY(id)")
        build("project time btree", "CREATE INDEX events_time ON events(project_id,ts)")
        c.execute("VACUUM ANALYZE events")

        def chart(table, where, count="count(*)"):
            return f"SELECT date_trunc('hour',ts),{count} FROM {table} WHERE project_id='p1' AND ts >= TIMESTAMPTZ '2026-09-08' - %s::interval AND ts < TIMESTAMPTZ '2026-09-08' AND {where} GROUP BY 1 ORDER BY 1"

        cases = {"absent": ["err:absent"], "rare": ["err:rare"],
                 "medium": ["err:medium"], "common": ["endpoint:common"],
                 "overlap": ["err:medium", "err:overlap"]}
        expected = {}

        def measure(label, query, case, days, check=True):
            params = (f"{days} days", cases[case])
            samples = []
            for _ in range(3):
                start = time.perf_counter()
                rows = c.execute(query, params).fetchall()
                samples.append(round((time.perf_counter()-start)*1000, 3))
            if label == "no_hash_index":
                expected[case, days] = rows
            elif check:
                assert rows == expected[case, days], (label, case, days)
            plan = c.execute("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + query, params).fetchone()[0][0]
            results["queries"].append({"method": label, "case": case, "days": days,
                "client_ms": samples, "count": sum(int(r[1]) for r in rows),
                "buckets": len(rows), "plan": plan})

        for days in (3,7,30):
            for case in cases:
                measure("no_hash_index", chart("events", "hashes && %s::text[]"), case, days)
        build("gin", "CREATE INDEX events_hashes ON events USING gin(hashes)")
        c.execute("ANALYZE events")
        for days in (3,7,30):
            for case in cases:
                measure("gin", chart("events", "hashes && %s::text[]"), case, days)
        # Also compare the application's JSONPath shape for a rare tag.
        sql = chart("events", "jsonb_path_exists(to_jsonb(hashes), '$[*] ? (@ == \"err:rare\")'::jsonpath) AND %s::text[] IS NOT NULL")
        measure("jsonpath_with_gin_present", sql, "rare", 30)
        build("membership table", "CREATE TABLE memberships AS SELECT project_id,ts,id,hash FROM events CROSS JOIN LATERAL (SELECT DISTINCT unnest(hashes) AS hash) h")
        build("membership covering btree", "CREATE UNIQUE INDEX membership_lookup ON memberships(project_id,hash,ts,id)")
        c.execute("VACUUM ANALYZE memberships")
        build("hourly table", "CREATE TABLE hourly AS SELECT project_id,hash,date_trunc('hour',ts) AS ts,count(*) AS n FROM memberships GROUP BY 1,2,3")
        build("hourly btree", "CREATE UNIQUE INDEX hourly_lookup ON hourly(project_id,hash,ts)")
        c.execute("VACUUM ANALYZE hourly")
        for days in (3,7,30):
            for case in cases:
                measure("covering_memberships", chart("memberships", "hash=ANY(%s)", "count(DISTINCT id)"), case, days)
                # Summing per-tag counts is intentionally invalid for overlapping ORs.
                measure("hourly", chart("hourly", "hash=ANY(%s)", "sum(n)"), case, days, check=case!="overlap")
        results["sizes"] = c.execute("SELECT relname,pg_relation_size(oid) FROM pg_class WHERE relnamespace=%s::regnamespace AND relkind IN ('r','i') ORDER BY relname", (schema,)).fetchall()
        # Physical PostgreSQL updates must not return a stale matching version.
        before = c.execute("SELECT count(*) FROM events WHERE hashes @> ARRAY['err:rare']").fetchone()[0]
        c.execute("UPDATE events SET hashes=array_remove(hashes,'err:rare') WHERE id=1000")
        after = c.execute("SELECT count(*) FROM events WHERE hashes @> ARRAY['err:rare']").fetchone()[0]
        assert before-after == 1
        results["gin_removal_check"] = {"before": before, "after": after}
    output.write_text(json.dumps(results, indent=2, default=str)+"\n")
    print("wrote", output, flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", required=True)
    p.add_argument("--rows", type=int, default=1_000_000)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    if args.rows < 1000:
        p.error("--rows must be at least 1000")
    run(args.dsn, args.rows, args.output)
