#!/usr/bin/env python3
"""Minimal JSON-RPC server that replays a frozen block dataset.

Serves exactly the RPCs both proxies issue while ingesting and starting up, from
a dataset extracted once by scripts/extract-dataset.sh:

  - getblockchaininfo   chain / tip / consensus / upgrades
  - getbestblockhash    display-order hash of the tip
  - getblock [h, 1]     verbose: hash, tx[], sapling/orchard tree sizes (by height)
  - getblock [hash, 0]  raw block hex (by hash)
  - getinfo             build / subversion
  - getrawmempool       empty (keeps the Rust mempool poll cheap and quiet)

Stateless and deterministic. During measurement it is idle: it keeps reporting
the dataset tip, so each proxy sees itself synced and only polls without fetching.
Stdlib only.

Dropping a DENY_GETBLOCK file into the dataset directory makes getblock fail while
every other RPC keeps answering. populate.sh uses this to prove both proxies serve
the whole range from cache: a miss falls back to the node and fails loudly, yet the
tip polls stay alive (the Go ingestor exits fatally if its tip poll fails, so the
mock must never go down while the proxies run).
"""

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DATASET = os.environ.get("DATASET_DIR", "/dataset")
DENY_GETBLOCK_FLAG = os.path.join(DATASET, "DENY_GETBLOCK")
BIND_HOST = os.environ.get("MOCK_BIND_HOST", "0.0.0.0")
BIND_PORT = int(os.environ.get("MOCK_BIND_PORT", "8232"))


class Dataset:
    """The frozen dataset: index metadata plus positional reads into raw.bin."""

    def __init__(self, directory):
        with open(os.path.join(directory, "index.json"), "r") as index_file:
            self.index = json.load(index_file)
        self.raw_fd = os.open(os.path.join(directory, "raw.bin"), os.O_RDONLY)
        self.by_height = {int(block["height"]): block for block in self.index["blocks"]}
        self.by_hash = {block["hash"]: block for block in self.index["blocks"]}

    def raw_hex(self, block):
        """Read a block's raw bytes from raw.bin and return them hex-encoded."""
        data = os.pread(self.raw_fd, int(block["raw_len"]), int(block["raw_offset"]))
        return data.hex()

    def blockchain_info(self):
        return {
            "chain": self.index["chain"],
            "blocks": self.index["tip_height"],
            "bestblockhash": self.index["tip_hash"],
            "estimatedheight": self.index["tip_height"],
            "consensus": self.index.get(
                "consensus", {"chaintip": "00000000", "nextblock": "00000000"}
            ),
            "upgrades": self.index.get("upgrades", {}),
        }


class RpcError(Exception):
    """A JSON-RPC error to return to the caller (never crashes the server)."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


def dispatch(dataset, method, params):
    if method == "getblockchaininfo":
        return dataset.blockchain_info()
    if method == "getbestblockhash":
        return dataset.index["tip_hash"]
    if method == "getrawmempool":
        # The Rust proxy polls the mempool on a background tick; an empty mempool
        # keeps that idle poll cheap and quiet, as a synced real node would.
        return []
    if method == "getinfo":
        # The subversion MUST contain "/Zebra:" — the Go proxy fatals on an
        # unrecognized backend subversion at startup.
        return {"build": "mock", "subversion": "/Zebra:mock/"}
    if method == "getblock":
        if os.path.exists(DENY_GETBLOCK_FLAG):
            raise RpcError(-32000, "getblock denied (DENY_GETBLOCK set): cache miss")
        if not isinstance(params, list) or len(params) < 2:
            raise RpcError(-1, "getblock expects [id, verbosity]")
        verbosity = int(params[1])
        if verbosity == 0:
            block = dataset.by_hash.get(str(params[0]))
            if block is None:
                raise RpcError(-5, "block hash not in dataset")
            return dataset.raw_hex(block)
        block = dataset.by_height.get(int(params[0]))
        if block is None:
            raise RpcError(-8, "block height out of range")
        return {
            "hash": block["hash"],
            "tx": block.get("txids", []),
            "trees": {
                "sapling": {"size": block.get("sapling_size", 0)},
                "orchard": {"size": block.get("orchard_size", 0)},
            },
        }
    raise RpcError(-32601, "method not found: " + str(method))


class Handler(BaseHTTPRequestHandler):
    dataset = None

    def log_message(self, *_args):
        pass  # keep stdout quiet; the harness scrapes proxies, not the mock

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""
        request_id = None
        result = None
        error = None
        try:
            request = json.loads(body)
            request_id = request.get("id")
            result = dispatch(self.dataset, request.get("method"), request.get("params"))
        except RpcError as rpc_error:
            error = {"code": rpc_error.code, "message": rpc_error.message}
        except Exception as unexpected:  # malformed request; report, never crash
            error = {"code": -32603, "message": str(unexpected)}
        payload = json.dumps({"result": result, "error": error, "id": request_id})
        encoded = payload.encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def main():
    Handler.dataset = Dataset(DATASET)
    tip = Handler.dataset.index["tip_height"]
    count = len(Handler.dataset.index["blocks"])
    print(f"mock-rpc: {count} blocks, tip {tip}, listening on {BIND_HOST}:{BIND_PORT}", flush=True)
    ThreadingHTTPServer((BIND_HOST, BIND_PORT), Handler).serve_forever()


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as missing:
        sys.exit(f"mock-rpc: dataset not found under {DATASET}: {missing}")
