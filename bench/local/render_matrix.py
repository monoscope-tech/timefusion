import csv, pathlib, sys
rows=list(csv.DictReader(pathlib.Path(sys.argv[1]).open()))
W=["1h","3h","24h","3d","7d","14d","30d"]
SH=["throughput","group_by_service","p95_latency","error_rate","log_list","dcount_service","facet_service"]
NAMES={"p1":"87576849","p2":"edb04135","p3":"00000000","p4":"dcad860a","p5":"be87ebc1","p6":"28f62f01"}
def cell(r):
    if r["status"]=="ok":
        v=r["rep2_ms"] or r["rep1_ms"]
        try: return f"{round(float(v)):,}"+("*" if not r["rep2_ms"] else "")
        except Exception: return v
    if r["status"].startswith("skip"): return "fail'"
    s=r["status"]
    if "per-scan limit" in s: return "REFUSED"
    if "exhaust" in s.lower(): return "OOM"
    if "timeout" in s.lower() or "cancel" in s.lower(): return "fail"
    return "err"
by={(r["project"],r["window"],r["shape"]):r for r in rows}
for p in [k for k in NAMES if any(r["project"]==k for r in rows)]:
    print(f"\n### {p}  {NAMES[p]}\n")
    print("| shape | "+" | ".join(W)+" |")
    print("|---"*(len(W)+1)+"|")
    for s in SH:
        line=[cell(by[(p,w,s)]) if (p,w,s) in by else "—" for w in W]
        if all(x=="—" for x in line): continue
        print(f"| {s} | "+" | ".join(line)+" |")
