# Architecture decision records

Short records of the architectural decisions that shape `lightwalletd-rs`. Each ADR captures one
decision in a fixed format (**Context**, **Decision**, **Consequences**), so the reasoning behind a
choice stays discoverable after the fact. The living overview is
[`../ARCHITECTURE.md`](../ARCHITECTURE.md); these records explain *why* it looks the way it does.

| ADR | Decision |
|---|---|
| [0001](0001-backend-zebrad-over-zcashd.md) | Backend node is `zebrad` over plain-HTTP JSON-RPC |
| [0002](0002-parse-blocks-with-librustzcash.md) | Parse transactions with `librustzcash`, hand-parse only block framing |
| [0003](0003-compute-txids-locally.md) | Compute transaction IDs locally |
| [0004](0004-redb-block-cache.md) | On-disk block cache backed by `redb` |
| [0005](0005-shared-mempool-monitor.md) | Shared mempool monitor (live mode) |
| [0006](0006-darkside-mock-via-noderpc-seam.md) | Darkside mocks the chain at the `NodeRpc` seam |
| [0007](0007-noderpc-seam.md) | `NodeRpc` trait is the single node-access seam |
| [0008](0008-library-plus-binary.md) | Ship as a library plus a thin binary |
| [0009](0009-service-per-method-family-modules.md) | Service split into per-method-family submodules |
| [0010](0010-node-error-grpc-mapping.md) | Map node errors to per-method gRPC status codes |
| [0011](0011-up-front-input-validation.md) | Reject malformed requests up front |
| [0012](0012-tls-default-insecure-flags.md) | TLS by default; dangerous features gated behind `*-very-insecure` flags |
| [0013](0013-resource-limits.md) | Bound the resources a client can hold or accumulate |
| [0014](0014-cache-ingestor-resilience.md) | Cache and ingestor recover from corruption and reorgs locally |
| [0015](0015-layered-testing-strategy.md) | Layered testing: fakes, golden fixtures, and in-process E2E |
| [0016](0016-test-placement-by-visibility.md) | Place tests by visibility: handler tests grouped by family, internals tested inline |
| [0017](0017-benchmark-methodology.md) | Benchmark the hot read-path against the reference implementation |
| [0018](0018-parse-time-branch-id-hardcoded.md) | Keep the parse-time consensus branch ID hardcoded at `Nu5` |
| [0019](0019-pin-librustzcash-prereleases-nu63.md) | Pin the librustzcash pre-release cohort for NU6.3 |
| [0020](0020-windowed-ingest-batched-commits.md) | Windowed concurrent ingest with batched cache commits |
| [0021](0021-mempool-staleness-contract.md) | Mempool staleness contract: stale snapshots become `Unavailable` |
| [0022](0022-ops-surface-parity.md) | Close the operational-surface gap with the Go reference: reflection, default-on metrics, log flags, `--gen-cert-very-insecure`, darkside auto-shutdown, `--nocache` |
| [0023](0023-zebra-readstate-backend.md) | Hybrid Zebra ReadStateService backend behind the NodeRpc seam |
| [0024](0024-snapshot-bootstrap.md) | Bootstrap the cache from a peer's snapshot, anchored to the operator's own node |
| [0025](0025-taddress-range-bounds.md) | Pin an open-ended transparent-address range to the chain tip and bound its span |
| [0026](0026-grpc-web-support.md) | Serve gRPC-web natively, behind an off-by-default runtime flag |
| [0027](0027-block-range-continuity.md) | Serve only self-consistent block ranges |
| [0028](0028-mvcc-chunked-cache-reads.md) | Read the cache in MVCC chunks when serving a range |
| [0029](0029-mixnet-transport-scope.md) | Keep a mixnet transport out of the crate, behind a sidecar |
| [0030](0030-subtree-index-range.md) | Bound the subtree index range at the service boundary |
| [0031](0031-lightwallet-protocol-version.md) | Report the served lightwallet-protocol version as a constant |
| [0032](0032-connection-gauge-at-the-listener.md) | Count connections at the listener |
| [0033](0033-ingest-floor-from-network-parameters.md) | Take the default ingest floor from the network parameters, not the node |
| [0034](0034-cap-the-node-response-body.md) | Cap how much of a node response body is held in memory |
| [0035](0035-bounded-metric-labels.md) | Take metric label values from the server, not from the request |
| [0036](0036-bulkhead-the-wallet-facing-node-calls.md) | Reserve node capacity for ingestion with a wallet-facing bulkhead |
| [0037](0037-batch-streamed-messages-into-full-frames.md) | Batch small streamed messages into full DATA frames |
| [0038](0038-bound-the-unspent-outputs-one-request-returns.md) | Bound the unspent outputs one address request returns |
