#!/usr/bin/env bash
#
# Load sweep with ghz against each proxy in series (never both at once), reading
# 100% from the warm cache with the mock idle. Per implementation, request type,
# and concurrency: one discarded warm-up plus N reps. Captures the ghz JSON, the
# proxy's CPU (from cgroup cpu.stat) per run, and — once per implementation — its
# peak RSS and a /metrics scrape of grpc_server_handling_seconds.
#
# Usage: scripts/run-bench.sh <profile>   (run populate.sh <profile> first)
#
# Tunables (env): REPS DURATION WARMUP CONCURRENCIES IMPLS REQUESTS
#   REQUESTS: getblock range100 range1000 range10000   (range<W> = GetBlockRange of W)

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REPO_DIR="$(cd "$BENCH_DIR/../.." && pwd)"
PROFILE="${1:?usage: run-bench.sh <profile>}"
ENV_FILE="$BENCH_DIR/profiles/$PROFILE.env"
[ -f "$ENV_FILE" ] || { echo "no profile env: $ENV_FILE" >&2; exit 1; }
# shellcheck disable=SC1090
set -a; . "$ENV_FILE"; set +a
: "${BASE:?}" "${SPAN:?}"

COMPOSE=(docker compose -f "$BENCH_DIR/docker-compose.bench.yml" --env-file "$ENV_FILE")
NET="lwd-bench_bench"
END=$((BASE + SPAN - 1))

REPS="${REPS:-5}"
DURATION="${DURATION:-8s}"
WARMUP="${WARMUP:-3s}"
read -r -a CONCURRENCIES <<< "${CONCURRENCIES:-1 2 4 8 16 32 64}"
read -r -a IMPLS <<< "${IMPLS:-rust go}"
read -r -a REQUESTS <<< "${REQUESTS:-getblock range100 range1000 range10000}"

# Map an implementation to its gRPC service and metrics port (no associative
# arrays — macOS ships bash 3.2).
svc_for() { case "$1" in rust) echo rust-lwd;; go) echo go-lwd;; *) echo "$1";; esac; }
mport_for() { case "$1" in rust) echo 9100;; go) echo 9068;; *) echo 9100;; esac; }

echo "[bench:$PROFILE] building ghz image (first build compiles ghz, takes a few minutes) ..." >&2
"${COMPOSE[@]}" --profile ghz build ghz >/dev/null

echo "[bench:$PROFILE] ensuring stack is up ..." >&2
"${COMPOSE[@]}" up -d mock-rpc rust-lwd go-lwd >/dev/null

in_svc() { "${COMPOSE[@]}" exec -T "$1" sh -c "$2"; }
# Readiness: the cache must already hold every block, so requests are served from
# cache (mock idle), not fetched on demand. populate.sh guarantees this.
go_blocks=$(( $(in_svc go-lwd '[ -f /data/db/main/lengths ] && wc -c < /data/db/main/lengths || echo 0' | tr -d ' ') / 4 ))
if [ "$go_blocks" -lt "$SPAN" ]; then
  echo "cache not fully populated (go has $go_blocks/$SPAN) — run: scripts/populate.sh $PROFILE" >&2
  exit 1
fi

cpu_usec() { in_svc "$1" "awk '/usage_usec/{print \$2}' /sys/fs/cgroup/cpu.stat" | tr -d ' '; }

call_for() { case "$1" in
  getblock) echo cash.z.wallet.sdk.rpc.CompactTxStreamer.GetBlock;;
  range*) echo cash.z.wallet.sdk.rpc.CompactTxStreamer.GetBlockRange;;
esac; }

# Build the request payload for a config at a given rep; spreads the target across
# the range so successive reps don't all hit the same block.
payload() { # payload REQ REP
  local req="$1" rep="$2"
  if [ "$req" = "getblock" ]; then
    echo "{\"height\":$(( BASE + rep * (SPAN / (REPS + 1)) ))}"
  else
    local w="${req#range}" a
    a=$(( BASE + rep * ((SPAN - w) / (REPS + 1)) ))
    echo "{\"start\":{\"height\":$a},\"end\":{\"height\":$((a + w - 1))}}"
  fi
}

ghz_run() { # ghz_run IMPL REQ C DURATION OUTFILE|-
  local impl="$1" req="$2" c="$3" dur="$4" out="$5"
  local data; data="$(payload "$req" "${REP:-0}")"
  local args=(--insecure --proto /proto/service.proto --import-paths /proto
    --call "$(call_for "$req")" -d "$data" -c "$c" --connections "$c" -z "$dur" -O json
    "$(svc_for "$impl"):9067")
  # stderr stays on the terminal: a failed docker/ghz invocation must be loud,
  # since set -e aborts the sweep on it.
  if [ "$out" = "-" ]; then
    docker run --rm --network "$NET" -v "$REPO_DIR/proto:/proto:ro" lwd-bench-ghz "${args[@]}" >/dev/null
  else
    docker run --rm --network "$NET" -v "$REPO_DIR/proto:/proto:ro" lwd-bench-ghz "${args[@]}" >"$out"
  fi
}

for impl in "${IMPLS[@]}"; do
  svc="$(svc_for "$impl")"
  out_dir="$BENCH_DIR/results/$PROFILE/$impl"
  mkdir -p "$out_dir"
  echo "[bench:$PROFILE] === $impl ===" >&2
  for req in "${REQUESTS[@]}"; do
    for c in "${CONCURRENCIES[@]}"; do
      REP=0 ghz_run "$impl" "$req" "$c" "$WARMUP" -   # warm-up, discarded
      for rep in $(seq 1 "$REPS"); do
        out="$out_dir/$req-c$c-r$rep.json"
        before=$(cpu_usec "$svc")
        REP=$rep ghz_run "$impl" "$req" "$c" "$DURATION" "$out"
        after=$(cpu_usec "$svc")
        # Denominator is ghz's own run duration (ns, from its report): BSD date on
        # macOS has no %N, and the proxy only accrues CPU during the load window.
        elapsed_ns=$(jq -r '.total' "$out")
        cores=$(awk -v a="$after" -v b="$before" -v ns="$elapsed_ns" \
          'BEGIN{ printf "%.3f", (ns>0)?((a-b)/1e6)/(ns/1e9):0 }')
        echo "{\"proxy_cpu_cores\":$cores}" > "$out_dir/$req-c$c-r$rep.res"
        printf '\r[bench:%s] %-4s %-11s c=%-3s rep=%s cpu=%s cores   ' \
          "$PROFILE" "$impl" "$req" "$c" "$rep" "$cores" >&2
      done
    done
  done
  echo "" >&2
  # Per-implementation: server-side histogram + peak RSS.
  docker run --rm --network "$NET" curlimages/curl:latest -s "http://$svc:$(mport_for "$impl")/metrics" \
    > "$out_dir/metrics-final.txt" 2>/dev/null || true
  in_svc "$svc" 'cat /sys/fs/cgroup/memory.peak' | tr -d ' ' > "$out_dir/peak-rss.txt"
  echo "[bench:$PROFILE] $impl peak RSS: $(cat "$out_dir/peak-rss.txt") bytes" >&2
done

echo "[bench:$PROFILE] done -> results/$PROFILE/" >&2
