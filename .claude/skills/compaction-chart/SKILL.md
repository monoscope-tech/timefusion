---
name: compaction-chart
description: Refresh and republish the historical-compaction dashboard (per-project/per-day Delta file counts, before/after bars) at its permanent artifact URL. Use when asked for compaction status, file-count charts, or "update the compaction chart".
---

Refresh `docs/dashboards/compaction-chart.html` with live data and republish it
to the permanent artifact URL below (pass it as the `url` parameter of the
Artifact tool from any session — never mint a new URL).

**Artifact URL (stable):** https://claude.ai/code/artifact/896a7eb9-2c29-4ca0-98ab-4ca02fb8d671 · favicon `🗜️`

## 1. Pull live per-(project, date) active file counts

Counts must come from the Delta snapshot (S3 listing overcounts tombstones).
Python `deltalake` is installed; creds in `.env.prod`:

Two OVH quirks are load-bearing here — both are baked into the snippet below,
don't "simplify" them away:

- `AWS_REGION` must be **`de`**, not `auto`. OVH rejects `auto` with
  `AuthorizationHeaderMalformed … expecting 'de'`.
- Export `AWS_REQUEST_CHECKSUM_CALCULATION` / `AWS_RESPONSE_CHECKSUM_VALIDATION`
  as `when_required`. Otherwise the client sends `x-amz-checksum-mode`, OVH
  rejects it, and reads fail — the failure mode that once made a probe report
  shipbubble as 0/14 days sorted when it was 9/9.

```bash
set -a; source .env.prod; set +a
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
python3 - <<'EOF'
from deltalake import DeltaTable
import os, collections
st={"AWS_ACCESS_KEY_ID":os.environ["AWS_ACCESS_KEY_ID"],"AWS_SECRET_ACCESS_KEY":os.environ["AWS_SECRET_ACCESS_KEY"],
    "AWS_ENDPOINT_URL":os.environ["AWS_S3_ENDPOINT"],"AWS_REGION":"de"}
dt=DeltaTable(f"s3://{os.environ['AWS_S3_BUCKET']}/timefusion/otel_logs_and_spans", storage_options=st)
c=collections.Counter(); b=collections.Counter()
for a in dt.get_add_actions(flatten=True).to_struct_array().to_pylist():
    d=str(a.get("partition.date"))
    if d>="2026-07-20":   # adjust window to the question at hand
        k=((a.get("partition.project_id") or "NULL")[:8],d)
        c[k]+=1; b[k]+=a.get("size_bytes",0)
for p in sorted({p for p,_ in c}):
    print(p, {d:(n, round(b[(p,d)]/1e9,2)) for (q,d),n in sorted(c.items()) if q==p})
print("version:",dt.version())
EOF
```

Bytes matter as much as counts: the chart's yield column is
`(files − ceil(GB)) / GB`, so pull `size_bytes` in the same pass.

Rollup tables live at `timefusion/<table>`, **not** `timefusion/default/<table>`.

## 2. Update the template

Edit `docs/dashboards/compaction-chart.html`:
- the `const rows=[...]` array: `[proj8, tag, before29, now29, before28, now28, now30, status]`
  (status chips: `done` / `done29` / `run` / `q`); keep the historical "before"
  baselines unless the comparison period changes.
- the four `.tile` numbers, the `.sub` snapshot version/time, and the `.note`.
- Healthy = ~file count ≈ partition GB (1 GB target) on sealed days.

## 3. Republish

Artifact tool with `file_path: docs/dashboards/compaction-chart.html`,
`url: <the stable URL above>`, favicon `🗜️`.

## Context that stays true

- Project IDs: 87576849… is the whale tenant (10-100× everyone else). Full IDs
  via `aws s3api list-objects-v2 --prefix "timefusion/otel_logs_and_spans/project_id=" --delimiter "/"`.
- Off-box compaction recipes and the 2026-07-30 backlog story: see
  memory `tf_cli_offbox_2026-07-30` and `tf_compaction_binfanin_leak_2026-07-30`.
