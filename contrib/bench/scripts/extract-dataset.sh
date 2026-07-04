#!/usr/bin/env bash
#
# One-off extraction of a height window from a live zebrad into a frozen dataset
# the mock-rpc serves. The node is used only here; it never takes part in a
# measurement.
#
# Usage:
#   RPC_URL=http://127.0.0.1:8232 scripts/extract-dataset.sh <profile>
#
# <profile> selects profiles/<profile>.env for BASE/SPAN. Writes
# data/<profile>/{index.json,raw.bin}. RPC auth is optional (zebrad exposes an
# unauthenticated endpoint); set RPC_USER/RPC_PASSWORD only if your node needs it.
#
# For each height h in [BASE, BASE+SPAN):
#   getblock [h, 1]     -> hash, tx[], sapling/orchard tree sizes
#   getblock [hash, 0]  -> raw block hex (appended to raw.bin, referenced by offset)
# The raw block is fetched by hash (from the verbose reply), matching how both
# proxies fetch, so the two calls always refer to the same block.
#
# Resilient to a flaky connection:
#   - each RPC retries with backoff (RPC_RETRIES=6, RPC_TIMEOUT=60s per call);
#   - progress is checkpointed to data/<profile>/blocks.ndjson, so re-running
#     resumes from where it stopped instead of starting over;
#   - FRESH=1 forces a clean restart.

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PROFILE="${1:?usage: extract-dataset.sh <profile>}"
ENV_FILE="$BENCH_DIR/profiles/$PROFILE.env"
[ -f "$ENV_FILE" ] || { echo "no profile env: $ENV_FILE" >&2; exit 1; }
# shellcheck disable=SC1090
set -a; . "$ENV_FILE"; set +a

: "${RPC_URL:?set RPC_URL}"
RPC_USER="${RPC_USER:-}"
RPC_PASSWORD="${RPC_PASSWORD:-}"
: "${BASE:?profile must define BASE}" "${SPAN:?profile must define SPAN}"

for tool in curl jq xxd; do
  command -v "$tool" >/dev/null || { echo "$tool not found" >&2; exit 1; }
done

OUT_DIR="$BENCH_DIR/data/$PROFILE"
mkdir -p "$OUT_DIR"
RAW_BIN="$OUT_DIR/raw.bin"
BLOCKS_NDJSON="$OUT_DIR/blocks.ndjson"
END=$((BASE + SPAN - 1))

rpc() { # rpc METHOD PARAMS_JSON  -> prints the JSON-RPC `result` (retries on transport failure)
  local body="{\"jsonrpc\":\"1.0\",\"id\":\"extract\",\"method\":\"$1\",\"params\":$2}"
  local attempt=1 max="${RPC_RETRIES:-6}" timeout="${RPC_TIMEOUT:-60}" out
  while :; do
    if [ -n "$RPC_USER" ]; then
      out=$(curl -fsS --max-time "$timeout" --user "$RPC_USER:$RPC_PASSWORD" -H 'Content-Type: application/json' --data "$body" "$RPC_URL") && break
    else
      out=$(curl -fsS --max-time "$timeout" -H 'Content-Type: application/json' --data "$body" "$RPC_URL") && break
    fi
    if [ "$attempt" -ge "$max" ]; then
      echo "rpc $1 failed after $max attempts; re-run to resume" >&2
      return 1
    fi
    echo "  rpc $1 failed (attempt $attempt/$max); retrying in $((attempt * 2))s ..." >&2
    sleep $((attempt * 2))
    attempt=$((attempt + 1))
  done
  printf '%s' "$out" | jq -e '.result'
}

# Fresh start on request, otherwise resume from the checkpoint.
if [ "${FRESH:-0}" = "1" ]; then
  : > "$RAW_BIN"; : > "$BLOCKS_NDJSON"
fi
[ -f "$RAW_BIN" ] || : > "$RAW_BIN"
[ -f "$BLOCKS_NDJSON" ] || : > "$BLOCKS_NDJSON"

# Drop a partial trailing line left by an interrupted run, then trim raw.bin to
# exactly the bytes those checkpointed blocks cover (blocks are appended to
# raw.bin before their checkpoint line, so raw.bin is never short).
if [ -s "$BLOCKS_NDJSON" ]; then
  jq -c . "$BLOCKS_NDJSON" > "$BLOCKS_NDJSON.tmp" 2>/dev/null || true
  mv "$BLOCKS_NDJSON.tmp" "$BLOCKS_NDJSON"
fi
done_count=$(wc -l < "$BLOCKS_NDJSON" | tr -d ' ')
offset=$(jq -s 'map(.raw_len) | add // 0' "$BLOCKS_NDJSON")
current_raw=$(wc -c < "$RAW_BIN" | tr -d ' ')
if [ "$current_raw" != "$offset" ]; then
  head -c "$offset" "$RAW_BIN" > "$RAW_BIN.tmp" && mv "$RAW_BIN.tmp" "$RAW_BIN"
fi
start_h=$((BASE + done_count))

echo "querying getblockchaininfo ..." >&2
CHAIN_INFO="$(rpc getblockchaininfo '[]')"
CHAIN="$(echo "$CHAIN_INFO" | jq -r '.chain')"
CONSENSUS="$(echo "$CHAIN_INFO" | jq -c '.consensus')"
UPGRADES="$(echo "$CHAIN_INFO" | jq -c '.upgrades // {}')"
# Sapling activation (branch id 76b809bb); 0 if the chain has no Sapling upgrade.
SAPLING="$(echo "$CHAIN_INFO" | jq -r '.upgrades["76b809bb"].activationheight // 0')"

if [ "$done_count" -gt 0 ]; then
  echo "resuming $CHAIN from height $start_h ($done_count/$SPAN already done)" >&2
else
  echo "extracting $CHAIN [$BASE, $END] ($SPAN blocks) from $RPC_URL ..." >&2
fi

for ((h = start_h; h <= END; h++)); do
  verbose="$(rpc getblock "[\"$h\", 1]")"
  hash="$(echo "$verbose" | jq -r '.hash')"
  txids="$(echo "$verbose" | jq -c '.tx // []')"
  sapling_size="$(echo "$verbose" | jq -r '.trees.sapling.size // 0')"
  orchard_size="$(echo "$verbose" | jq -r '.trees.orchard.size // 0')"

  raw_hex="$(rpc getblock "[\"$hash\", 0]" | jq -r '.')"
  raw_len=$(( ${#raw_hex} / 2 ))
  printf '%s' "$raw_hex" | xxd -r -p >> "$RAW_BIN"

  jq -cn \
    --argjson height "$h" --arg hash "$hash" --argjson txids "$txids" \
    --argjson sapling_size "$sapling_size" --argjson orchard_size "$orchard_size" \
    --argjson raw_offset "$offset" --argjson raw_len "$raw_len" \
    '{height:$height, hash:$hash, txids:$txids, sapling_size:$sapling_size,
      orchard_size:$orchard_size, raw_offset:$raw_offset, raw_len:$raw_len}' \
    >> "$BLOCKS_NDJSON"

  offset=$((offset + raw_len))
  completed=$((h - BASE + 1))
  if (( completed % ${PROGRESS_EVERY:-100} == 0 || h == END )); then
    pct10=$(( completed * 1000 / SPAN ))
    printf '  %d/%d (%d.%d%%)\n' "$completed" "$SPAN" $((pct10 / 10)) $((pct10 % 10)) >&2
  fi
done

# Assemble the final index.json from every checkpointed block.
TIP_HASH="$(jq -rs '.[-1].hash // ""' "$BLOCKS_NDJSON")"
jq -s \
  --arg chain "$CHAIN" --argjson base "$BASE" --argjson span "$SPAN" \
  --argjson tip_height "$END" --arg tip_hash "$TIP_HASH" \
  --argjson sapling_activation "$SAPLING" \
  --argjson consensus "$CONSENSUS" --argjson upgrades "$UPGRADES" \
  '{chain:$chain, base:$base, span:$span, tip_height:$tip_height, tip_hash:$tip_hash,
    sapling_activation:$sapling_activation, consensus:$consensus, upgrades:$upgrades, blocks:.}' \
  "$BLOCKS_NDJSON" > "$OUT_DIR/index.json"

raw_total=$(wc -c < "$RAW_BIN" | tr -d ' ')
echo "" >&2
echo "wrote $OUT_DIR/index.json and raw.bin ($raw_total bytes)" >&2
jq -rs 'if length == 0 then "density: (no blocks)" else
  "density: avg \(((map(.txids | length) | add) / length * 100 | round) / 100) txids/block, avg \(((map(.raw_len) | add) / length) | round) raw bytes/block"
  end' "$BLOCKS_NDJSON" >&2
