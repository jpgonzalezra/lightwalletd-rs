#!/usr/bin/env bash
#
# Manual smoke test against a live darkside server, driven with grpcurl + jq over the real
# `basic-reorg` vector fetched from the internet. Not run in CI (needs network + tooling).
#
# The vector is downloaded here with curl and pushed in via the streaming control RPCs, because the
# server's reqwest client is built without TLS (the backend node is plain HTTP) and so the URL-based
# StageBlocks/StageTransactions RPCs cannot fetch from https. The .proto files are still passed to
# grpcurl explicitly below (rather than relying on the server's gRPC reflection) so this script keeps
# working unchanged against a server built before reflection was added.
#
# Requirements: grpcurl, jq. Usage: ./contrib/smoke-test.sh
#
# --nocache and --metrics-bind are left at their defaults here (the on-disk cache and Prometheus on
# 127.0.0.1:9068), since neither affects this script's assertions and the metrics port is otherwise
# unused in this process tree.

set -eo pipefail

cd "$(dirname "$0")/.." || exit 1

ADDR=127.0.0.1:9067
PROTO_DIR=proto
PROTO_ARGS=(-import-path "$PROTO_DIR" -proto service.proto -proto darkside.proto)

BASE=https://raw.githubusercontent.com/zcash-hackworks/darksidewalletd-test-data/master
BLOCK_URL="$BASE/basic-reorg/663150.txt"
RECV_URL="$BASE/transactions/recv/0821a89be7f2fc1311792c3fa1dd2171a8cdfb2effd98590cbd5ebcdcfcf491f.txt"

# The recv transaction's txid in wire (protocol) order, base64-encoded — how it appears in CompactTx
# and how GetTransaction expects it. Display order is 0821a8…491f.
TXID_WIRE_B64="H0nPz83r1cuQhdn/LvvNqHEh3aE/LHkRE/zy55uoIQg="
# Synthetic mainnet t-address (P2PKH hash160 of all zeros) — the literal form of the Rust tests'
# example_taddress(): ZcashAddress::from_transparent_p2pkh(Main, [0; 20]).
TADDR=t1Hsc1LR8yKnbbe3twRp88p6vFfC5t7DLbs
RESET='{"saplingActivation":663150,"branchID":"bad","chainName":"x"}'

command -v grpcurl >/dev/null || { echo "grpcurl not found (go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest)"; exit 1; }
command -v jq >/dev/null || { echo "jq not found"; exit 1; }

echo "downloading the basic-reorg vector..."
BLOCK_HEX=$(curl -fsS "$BLOCK_URL" | tr -d '\r\n ')
TX_B64=$(curl -fsS "$RECV_URL" | tr -d '\r\n ' | xxd -r -p | base64 | tr -d '\n')

DATADIR=$(mktemp -d)
SERVER_PID=

cleanup() {
  # Ask the server to stop cleanly; fall back to killing the process.
  grpcurl -plaintext "${PROTO_ARGS[@]}" "$ADDR" cash.z.wallet.sdk.rpc.DarksideStreamer/Stop >/dev/null 2>&1 || true
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$DATADIR"
}
trap cleanup EXIT

grpc() { # grpc <Service> <Method> [json]
  local service=$1 method=$2
  shift 2
  local data=()
  [ "$#" -gt 0 ] && data=(-d "$1")
  grpcurl -plaintext "${PROTO_ARGS[@]}" "${data[@]}" "$ADDR" "cash.z.wallet.sdk.rpc.$service/$method"
}
gp() { grpc CompactTxStreamer "$@"; } # production CompactTxStreamer
gt() { grpc DarksideStreamer "$@"; }  # darkside control plane

check() { # check <description> <expected> <actual>
  if [ "$2" != "$3" ]; then
    echo "FAIL: $1"
    echo "  expected: [$2]"
    echo "  actual:   [$3]"
    exit 1
  fi
  echo "ok: $1"
}

echo "building..."
cargo build --quiet

echo "starting darkside server (data dir $DATADIR)..."
cargo run --quiet -- --darkside-very-insecure --no-tls-very-insecure --data-dir "$DATADIR" --grpc-bind "$ADDR" &
SERVER_PID=$!

echo -n "waiting for server"
ready=
for _ in $(seq 1 60); do
  if gt Reset "$RESET" >/dev/null 2>&1; then ready=1; break; fi
  echo -n .
  sleep 1
done
echo
[ -n "$ready" ] || { echo "server did not become ready"; exit 1; }

echo "staging the basic-reorg vector..."
gt StageBlocksStream "{\"block\":\"$BLOCK_HEX\"}" >/dev/null
gt StageBlocksCreate '{"height":663151,"count":100}' >/dev/null
gt StageTransactionsStream "{\"data\":\"$TX_B64\",\"height\":663190}" >/dev/null
gt ApplyStaged '{"height":663210}' >/dev/null

info=$(gp GetLightdInfo)
check "GetLightdInfo chainName" "x" "$(echo "$info" | jq -r .chainName)"
check "GetLightdInfo saplingActivationHeight" "663150" "$(echo "$info" | jq -r .saplingActivationHeight)"
check "GetLightdInfo consensusBranchId" "bad" "$(echo "$info" | jq -r .consensusBranchId)"
check "GetLightdInfo blockHeight" "663210" "$(echo "$info" | jq -r .blockHeight)"
check "GetLightdInfo vendor" "lightwalletd-rs" "$(echo "$info" | jq -r .vendor)"

check "GetLatestBlock height" "663210" "$(gp GetLatestBlock | jq -r .height)"

block=$(gp GetBlock '{"height":663190}')
check "GetBlock 663190 height" "663190" "$(echo "$block" | jq -r .height)"
check "GetBlock 663190 contains recv tx" "true" \
  "$(echo "$block" | jq -r --arg t "$TXID_WIRE_B64" '[.vtx[].txid] | index($t) != null')"

range=$(gp GetBlockRange '{"start":{"height":663152},"end":{"height":663154}}')
check "GetBlockRange heights" "663152,663153,663154" \
  "$(echo "$range" | jq -s -r '[.[].height] | join(",")')"

check "GetTransaction height" "663190" \
  "$(gp GetTransaction "{\"hash\":\"$TXID_WIRE_B64\"}" | jq -r .height)"

echo "checking GetTaddressTransactions (fresh Reset)..."
gt Reset "$RESET" >/dev/null
gt AddAddressTransaction '{"address":"'"$TADDR"'","data":"'"$TX_B64"'","height":644337}' >/dev/null

in_range='{"range":{"start":{"height":644337},"end":{"height":650510}},"address":"'"$TADDR"'"}'
out_of_range='{"range":{"start":{"height":644338},"end":{"height":650510}},"address":"'"$TADDR"'"}'
check "GetTaddressTransactions in range" "644337" "$(gp GetTaddressTransactions "$in_range" | jq -r .height)"
check "GetTaddressTransactions out of range is empty" "" "$(gp GetTaddressTransactions "$out_of_range")"

echo
echo "smoke test passed."
