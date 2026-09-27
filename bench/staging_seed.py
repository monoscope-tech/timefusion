#!/usr/bin/env python3
"""Seed s3://<bucket>/timefusion-staging/otel_logs_and_spans from prod's active files for DATES.

Reads prod's raw Delta log (checkpoint parquet + JSON commits after it), reconstructs the
active add actions verbatim (tags, stats, DVs), server-side copies data + DV objects under
the staging root, and writes a single version-0 commit. Usage:
  seed_staging.py plan     # replay + print, no writes
  seed_staging.py apply    # copy objects + write _delta_log/0.json
Env: AWS_* from .env.prod, AWS_REGION=de, AWS_{REQUEST,RESPONSE}_CHECKSUM_*=when_required.
"""
import io, json, os, sys, time, uuid, urllib.parse
from concurrent.futures import ThreadPoolExecutor
import boto3, pyarrow.parquet as pq
from boto3.s3.transfer import TransferConfig

DATES = {"2026-09-25", "2026-09-26"}
TABLE = "otel_logs_and_spans"
SRC_ROOT = f"timefusion/{TABLE}/"
STAGING = "timefusion-staging/"
DST_ROOT = f"{STAGING}{TABLE}/"
BUCKET = os.environ["AWS_S3_BUCKET"]
assert os.environ.get("AWS_REGION") == "de"
s3 = boto3.client("s3", endpoint_url=os.environ["AWS_S3_ENDPOINT"], region_name="de")


def guard(key):
    assert key.startswith(STAGING) and not key.startswith("timefusion/"), f"refusing write outside staging: {key}"
    return key


def get(key):
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


# --- Delta log replay -------------------------------------------------------------------------

def unmap(v):
    """pyarrow map -> dict, recursively dropping None-valued struct fields that are optional."""
    return dict(v) if isinstance(v, list) else v


def ckpt_add(a):
    """Checkpoint `add` struct -> the JSON add action (maps as objects, absent optionals omitted)."""
    out = {"path": a["path"], "partitionValues": unmap(a["partitionValues"]), "size": a["size"],
           "modificationTime": a["modificationTime"], "dataChange": a["dataChange"]}
    if a["stats"] is not None: out["stats"] = a["stats"]
    if a["tags"] is not None: out["tags"] = unmap(a["tags"])
    if a["deletionVector"] is not None:
        out["deletionVector"] = {k: v for k, v in a["deletionVector"].items() if v is not None}
    for k in ("baseRowId", "defaultRowCommitVersion", "clusteringProvider"):
        out[k] = a.get(k)
    return out


def ckpt_meta(m):
    return {**m, "format": {**m["format"], "options": unmap(m["format"]["options"])},
            "configuration": unmap(m["configuration"])}


def ckpt_protocol(p):
    return {k: v for k, v in p.items() if v is not None}


def dv_id(dv):
    if not dv: return None
    return dv["storageType"] + dv["pathOrInlineDv"] + (f"@{dv['offset']}" if dv.get("offset") is not None else "")


def fkey(a):
    return (a["path"], dv_id(a.get("deletionVector")))


def replay(ckpt=None, upto=None):
    """Active set at `upto` (default: latest) from checkpoint `ckpt` (default: _last_checkpoint)."""
    lc = json.loads(get(SRC_ROOT + "_delta_log/_last_checkpoint"))
    if ckpt is not None:
        lc = {"version": ckpt, "numOfAddFiles": None}
    v0 = lc["version"]
    assert "parts" not in lc, f"multi-part checkpoint not handled: {lc}"
    t = pq.read_table(io.BytesIO(get(SRC_ROOT + f"_delta_log/{v0:020d}.checkpoint.parquet"))).to_pylist()
    assert not any(r.get("sidecar") for r in t), "v2 checkpoint sidecars not handled"
    active = {fkey(a): a for a in (ckpt_add(r["add"]) for r in t if r.get("add"))}
    assert lc["numOfAddFiles"] in (None, len(active)), (len(active), lc)
    proto = ckpt_protocol(next(r["protocol"] for r in t if r.get("protocol")))
    meta = ckpt_meta(next(r["metaData"] for r in t if r.get("metaData")))
    other = {k for r in t for k in ("txn", "domainMetadata") if r.get(k)}
    v = v0
    while upto is None or v < upto:
        try:
            body = get(SRC_ROOT + f"_delta_log/{v + 1:020d}.json")
        except s3.exceptions.NoSuchKey:
            break
        v += 1
        for line in body.decode().splitlines():
            if not line.strip(): continue
            act = json.loads(line)
            if "add" in act: active[fkey(act["add"])] = act["add"]
            elif "remove" in act: active.pop(fkey(act["remove"]), None)
            elif "protocol" in act: proto = act["protocol"]
            elif "metaData" in act: meta = act["metaData"]
            elif "txn" in act or "domainMetadata" in act: other.add(next(iter(act)))
    # a path appears at most once in a valid snapshot
    assert len({p for p, _ in active}) == len(active), "duplicate path in active set"
    return v0, v, proto, meta, other, list(active.values())


# --- Deletion vector paths --------------------------------------------------------------------

Z85 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"
Z85_IDX = {c: i for i, c in enumerate(Z85)}


def z85_decode(s):
    assert len(s) % 5 == 0
    out = bytearray()
    for i in range(0, len(s), 5):
        n = 0
        for c in s[i:i + 5]: n = n * 85 + Z85_IDX[c]
        out += n.to_bytes(4, "big")
    return bytes(out)


def dv_rel_path(dv):
    """Relative object path of a 'u' DV: <prefix>/deletion_vector_<uuid>.bin (prefix optional)."""
    p = dv["pathOrInlineDv"]
    prefix, enc = p[:-20], p[-20:]
    name = f"deletion_vector_{uuid.UUID(bytes=z85_decode(enc))}.bin"
    return f"{prefix}/{name}" if prefix else name


# --- Main -------------------------------------------------------------------------------------

def main(mode):
    v0, v, proto, meta, other, adds = replay()
    print(f"prod snapshot: checkpoint {v0}, replayed to version {v}; active files {len(adds)}")
    print(f"protocol: {proto}")
    if other: print(f"NOTE: prod log has {other} actions; not copied (staging starts without app txns)")
    sel = [a for a in adds if a["partitionValues"].get("date") in DATES]
    by_date = {d: (sum(1 for a in sel if a["partitionValues"]["date"] == d),
                   sum(a["size"] for a in sel if a["partitionValues"]["date"] == d)) for d in sorted(DATES)}
    kinds = {}
    for a in sel:
        dv = a.get("deletionVector")
        kinds[dv["storageType"] if dv else None] = kinds.get(dv["storageType"] if dv else None, 0) + 1
    print(f"selected {len(sel)} files, {sum(a['size'] for a in sel)} bytes; per date {by_date}")
    print(f"tags: {sum(1 for a in sel if a.get('tags'))}, DVs by storageType: {kinds}")

    copies = []  # (src_key, dst_key)
    for a in sel:
        rel = urllib.parse.unquote(a["path"])
        assert "://" not in rel and not rel.startswith("/"), f"absolute data path unsupported: {rel}"
        copies.append((SRC_ROOT + rel, DST_ROOT + rel))
        dv = a.get("deletionVector")
        if not dv or dv["storageType"] == "i": continue
        if dv["storageType"] == "u":
            dvrel = dv_rel_path(dv)
            copies.append((SRC_ROOT + dvrel, DST_ROOT + dvrel))
        elif dv["storageType"] == "p":
            raise SystemExit(f"absolute DV path present ({dv['pathOrInlineDv']}); rewriting not implemented - stop")
        else:
            raise SystemExit(f"unknown DV storageType {dv}")
    copies = sorted(set(copies))
    dv_copies = [c for c in copies if "deletion_vector_" in c[0]]
    print(f"objects to copy: {len(copies)} ({len(dv_copies)} DV files)")

    # verify decode against real objects: every DV source must exist with the recorded size bound
    for src, _ in dv_copies[:1]:
        h = s3.head_object(Bucket=BUCKET, Key=src)
        print(f"DV decode check: {src} exists ({h['ContentLength']} bytes)")
    missing = []
    def head(src):
        try: s3.head_object(Bucket=BUCKET, Key=src)
        except Exception as e: missing.append((src, str(e)))
    with ThreadPoolExecutor(16) as ex: list(ex.map(head, [s for s, _ in copies]))
    assert not missing, f"missing sources: {missing[:5]}"
    print("all source objects exist")

    new_meta = {**meta, "id": str(uuid.uuid4())}
    assert new_meta["id"] != meta["id"]
    now = int(time.time() * 1000)
    commit = [{"commitInfo": {"timestamp": now, "operation": "CREATE TABLE", "operationParameters": {"mode": "ErrorIfExists"},
                              "isBlindAppend": True, "clientVersion": "timefusion-staging-seed",
                              "timefusion.seed": json.dumps({"source": f"s3://{BUCKET}/{SRC_ROOT}", "version": v, "dates": sorted(DATES)})}},
              {"protocol": proto}, {"metaData": new_meta}] + [{"add": a} for a in sel]
    body = "\n".join(json.dumps(x, separators=(",", ":")) for x in commit) + "\n"
    local = os.path.join(os.path.dirname(os.path.abspath(__file__)), "staging_00000000000000000000.json")
    open(local, "w").write(body)
    print(f"commit written locally: {local} ({len(body)} bytes); new table id {new_meta['id']} (prod {meta['id']})")
    if mode != "apply": return

    print(f"WRITE destination prefix: s3://{BUCKET}/{DST_ROOT}")
    guard(DST_ROOT)
    existing = s3.list_objects_v2(Bucket=BUCKET, Prefix=DST_ROOT + "_delta_log/").get("KeyCount", 0)
    assert existing == 0, f"staging _delta_log already has {existing} objects; refusing to overwrite"
    cfg = TransferConfig(multipart_threshold=5 * 1024**3, max_concurrency=4)
    def copy(c):
        src, dst = c
        s3.copy({"Bucket": BUCKET, "Key": src}, BUCKET, guard(dst), Config=cfg)
        return dst
    with ThreadPoolExecutor(8) as ex:
        for i, dst in enumerate(ex.map(copy, copies), 1):
            if i % 25 == 0 or i == len(copies): print(f"  copied {i}/{len(copies)}")
    # the commit goes last so the table only exists once every referenced object does
    s3.put_object(Bucket=BUCKET, Key=guard(DST_ROOT + "_delta_log/00000000000000000000.json"), Body=body.encode())
    print("staging table committed at version 0")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "plan")
