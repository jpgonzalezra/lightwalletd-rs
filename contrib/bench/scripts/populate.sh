#!/usr/bin/env bash
#
# Populate both proxies' caches for a profile from the mock, then report the
# on-disk footprint, prove the range is served from cache (mock refusing getblock),
# and check fairness (both proxies return identical blocks). Leaves the cache
# volumes warm and the stack running idle, ready for the load run.
#
# Usage: scripts/populate.sh <profile>

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REPO_DIR="$(cd "$BENCH_DIR/../.." && pwd)"
PROFILE="${1:?usage: populate.sh <profile>}"
ENV_FILE="$BENCH_DIR/profiles/$PROFILE.env"
[ -f "$ENV_FILE" ] || { echo "no profile env: $ENV_FILE" >&2; exit 1; }
# shellcheck disable=SC1090
set -a; . "$ENV_FILE"; set +a
: "${BASE:?profile must define BASE}" "${SPAN:?profile must define SPAN}"
[ -f "$BENCH_DIR/data/$PROFILE/index.json" ] || {
  echo "no dataset at data/$PROFILE — run extract-dataset.sh $PROFILE first" >&2; exit 1; }

COMPOSE=(docker compose -f "$BENCH_DIR/docker-compose.bench.yml" --env-file "$ENV_FILE")
NET="lwd-bench_bench"
GRPCURL_IMAGE="${GRPCURL_IMAGE:-fullstorydev/grpcurl:latest}"
GRPC_MAX_RANGE=10000
END=$((BASE + SPAN - 1))

echo "[populate:$PROFILE] starting mock-rpc + proxies ..." >&2
"${COMPOSE[@]}" up -d mock-rpc rust-lwd go-lwd >/dev/null

# Ingestion progress. Go's lengths file is exactly 4 bytes per cached block, an
# exact count; redb has no simple block count, so Rust is done when its cache size
# stops growing. Settled = Go has all SPAN blocks and redb held steady a few polls.
in_rust() { "${COMPOSE[@]}" exec -T rust-lwd sh -c "$1"; }
in_go() { "${COMPOSE[@]}" exec -T go-lwd sh -c "$1"; }
go_blocks() { echo $(( $(in_go '[ -f /data/db/main/lengths ] && wc -c < /data/db/main/lengths || echo 0' | tr -d ' ') / 4 )); }
rust_bytes() { in_rust '[ -f /data/main-blocks.redb ] && wc -c < /data/main-blocks.redb || echo 0' | tr -d ' '; }

stable=0; last_rust=-1
while :; do
  gc=$(go_blocks); rs=$(rust_bytes)
  printf '\r[populate:%s] go %d/%d blocks | rust redb %s bytes   ' "$PROFILE" "$gc" "$SPAN" "$rs" >&2
  if [ "$gc" -ge "$SPAN" ] && [ "$rs" = "$last_rust" ]; then
    stable=$((stable + 1)); [ "$stable" -ge 3 ] && break
  else
    stable=0
  fi
  last_rust=$rs
  sleep 3
done
echo "" >&2

# Footprint on disk (bytes), same range for both.
RUST_FOOTPRINT=$(in_rust 'du -sb /data/main-blocks.redb | cut -f1')
GO_FOOTPRINT=$(in_go 'du -sb /data/db/main | cut -f1')

# Prove the read path is served from cache AND that both proxies agree: with
# getblock denied by the mock, each must still return every block in [BASE, END]
# (a miss would fall back to the node and fail the request), and the two rendered
# GetBlockRange streams must hash the same (fairness — the benchmark only compares
# like for like). The mock stays up because the Go ingestor exits fatally if its
# tip poll fails; only getblock is cut off. GetBlockRange caps at GRPC_MAX_RANGE,
# so cover the span in chunks.
DENY_GETBLOCK_FLAG="$BENCH_DIR/data/$PROFILE/DENY_GETBLOCK"
RUST_STREAM=$(mktemp); GO_STREAM=$(mktemp)
trap 'rm -f "$DENY_GETBLOCK_FLAG" "$RUST_STREAM" "$GO_STREAM"' EXIT
echo "[populate:$PROFILE] denying mock getblock; verifying served-from-cache + fairness ..." >&2
touch "$DENY_GETBLOCK_FLAG"
stream_range() { # stream_range HOST -> raw GetBlockRange stream over [BASE, END] (chunked at the cap)
  local host=$1 s=$BASE e
  while [ "$s" -le "$END" ]; do
    e=$((s + GRPC_MAX_RANGE - 1)); [ "$e" -gt "$END" ] && e=$END
    docker run --rm --network "$NET" -v "$REPO_DIR/proto:/proto:ro" "$GRPCURL_IMAGE" \
      -plaintext -import-path /proto -proto service.proto \
      -d "{\"start\":{\"height\":$s},\"end\":{\"height\":$e}}" \
      "$host:9067" cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange 2>/dev/null \
      || { echo "grpcurl GetBlockRange [$s, $e] against $host failed" >&2; exit 1; }
    s=$((e + 1))
  done
}
stream_range rust-lwd > "$RUST_STREAM"
stream_range go-lwd > "$GO_STREAM"
RUST_SERVED=$(jq -s 'length' < "$RUST_STREAM")
GO_SERVED=$(jq -s 'length' < "$GO_STREAM")
# Content identity (the responses decode to the same messages), not a wire-byte claim.
RUST_HASH=$(shasum -a 256 < "$RUST_STREAM" | cut -d' ' -f1)
GO_HASH=$(shasum -a 256 < "$GO_STREAM" | cut -d' ' -f1)
FAIRNESS=differ; [ "$RUST_HASH" = "$GO_HASH" ] && FAIRNESS=identical
rm -f "$DENY_GETBLOCK_FLAG"  # back to idle, ready for the load run

RESULT_DIR="$BENCH_DIR/results/$PROFILE"
mkdir -p "$RESULT_DIR"
printf 'profile=%s\nblocks=%d\nrust_cache_bytes=%s\ngo_cache_bytes=%s\nfairness=%s\n' \
  "$PROFILE" "$SPAN" "$RUST_FOOTPRINT" "$GO_FOOTPRINT" "$FAIRNESS" > "$RESULT_DIR/footprint.txt"

echo "" >&2
echo "=== $PROFILE: $SPAN blocks [$BASE, $END] ===" >&2
echo "served from cache: rust $RUST_SERVED/$SPAN | go $GO_SERVED/$SPAN | GetBlockRange fairness: $FAIRNESS" >&2
echo "cache footprint: rust redb $RUST_FOOTPRINT bytes | go db/main $GO_FOOTPRINT bytes" >&2
if [ "$RUST_SERVED" -eq "$SPAN" ] && [ "$GO_SERVED" -eq "$SPAN" ] && [ "$FAIRNESS" = identical ]; then
  echo "OK: both caches fully populated, serving identical blocks without the node" >&2
else
  # Keep the two streams for diagnosis (results/ is git-ignored); a diff points at
  # the first divergent block.
  mv "$RUST_STREAM" "$RESULT_DIR/rust-getblockrange.stream" 2>/dev/null || true
  mv "$GO_STREAM" "$RESULT_DIR/go-getblockrange.stream" 2>/dev/null || true
  echo "WARN: served rust=$RUST_SERVED go=$GO_SERVED, fairness=$FAIRNESS (want $SPAN + identical)" >&2
  echo "  saved both streams under $RESULT_DIR/ — diff them to find the first divergent block" >&2
  exit 1
fi
