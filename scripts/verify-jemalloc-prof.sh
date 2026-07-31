#!/usr/bin/env bash
# Verify that the shipped image's jemalloc heap dumps are actually attributable
# — i.e. the profiler's unwinder produces deep, symbolizable Rust stacks.
#
# Both the 2026-07-30 and -31 profiling changes shipped unverified and were blind
# in prod (100% of an 88GB dump in one anonymous frame), hence this gate. Run it
# on a Linux docker host against the CI-built image:
#
#   scripts/verify-jemalloc-prof.sh ghcr.io/monoscope-tech/timefusion:<sha>
#
# Exits 0 only if the newest dump has a >3-frame stack AND jeprof resolves at
# least one Rust/DataFusion symbol.
set -euo pipefail

IMAGE="${1:?usage: verify-jemalloc-prof.sh <image-tag>}"
WORK="$(mktemp -d)"
NAME="tf-profverify-$$"
trap 'docker rm -f "$NAME" >/dev/null 2>&1 || true; rm -rf "$WORK"' EXIT
mkdir -p "$WORK/data/timefusion/profiles" "$WORK/data/wal"
chmod -R 777 "$WORK/data"   # distroless runs as uid 65532; dumps must be writable
chmod -R 777 "$WORK"   # container runs as nonroot (65532)

# prof_gdump: dump on every virtual-memory high-water mark, so boot allocations
# alone produce dumps; lg_prof_interval:30 (~1GiB) adds volume-triggered ones.
# MALLOC_CONF wins over the baked malloc_conf symbol; prof_prefix stays baked.
docker run -d --name "$NAME" \
  -e MALLOC_CONF=prof:true,prof_active:true,prof_gdump:true,lg_prof_interval:30,lg_prof_sample:16 \
  -e TIMEFUSION_ALLOW_INSECURE_AUTH=true \
  -e AWS_S3_BUCKET=profverify -e AWS_REGION=us-east-1 \
  -e AWS_ACCESS_KEY_ID=profverify -e AWS_SECRET_ACCESS_KEY=profverify \
  -e RUST_LOG=info \
  -v "$WORK/data:/app/data" "$IMAGE" >/dev/null

echo "booting + allocating for 60s..."
sleep 60
docker logs "$NAME" 2>&1 | tail -5
docker rm -f "$NAME" >/dev/null

DUMP="$(ls -t "$WORK"/data/timefusion/profiles/jeprof.*.heap 2>/dev/null | head -1 || true)"
[ -n "$DUMP" ] || { echo "FAIL: no heap dump written (profiling feature missing?)"; exit 1; }
echo "dump: $DUMP"

# Stack depth: dump lines are "  t*: N: B [..] @ 0xa 0xb 0xc ..." — count addresses.
DEPTH="$(awk -F'@' '/@ 0x/ {n=gsub(/0x/,"",$2); if (n>m) m=n} END {print m+0}' "$DUMP")"
echo "deepest stack: $DEPTH frames"

# Symbolize against the binary from the image. jeprof's own symbolizer is
# broken for this PIE layout (it picks the wrong base), so resolve manually:
# vaddr = runtime addr − load base (the first r--p file mapping in the dump).
docker create --name "$NAME-cp" "$IMAGE" >/dev/null
docker cp "$NAME-cp:/usr/local/bin/timefusion" "$WORK/timefusion" >/dev/null
docker rm "$NAME-cp" >/dev/null
cp "$DUMP" "$WORK/dump.heap"
docker run --rm -v "$WORK:/w" debian:bookworm-slim sh -c '
  apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq binutils >/dev/null 2>&1
  BASE=$(grep -a "r--p 00000000.*timefusion" /w/dump.heap | head -1 | cut -d- -f1)
  echo "load base: 0x$BASE"
  grep -aoE "0x[0-9a-f]{10,16}" /w/dump.heap | sort | uniq -c | sort -rn | head -12 | \
  while read -r _ A; do
    OFF=$(printf "0x%x" $(( A - 0x$BASE )) 2>/dev/null) || continue
    echo "$A -> $(addr2line -f -C -e /w/timefusion "$OFF" 2>/dev/null | head -1)"
  done' > "$WORK/text" 2>/dev/null
cat "$WORK/text"

grep -qE 'alloc::|datafusion|timefusion|arrow|tokio|core::|std::' "$WORK/text" \
  || { echo "FAIL: no Rust symbols resolved — unwinder still blind"; exit 1; }
[ "$DEPTH" -gt 3 ] \
  || { echo "FAIL: deepest stack is $DEPTH frames (<=3) — unwinder still blind"; exit 1; }
echo "OK: heap profiles are attributable ($DEPTH-frame stacks, Rust symbols resolved)"
