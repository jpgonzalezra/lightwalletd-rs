# 0007. NodeRpc trait is the single node-access seam

## Context

Every part of the server that needs node data — the `service` handlers, the `ingestor`, and `fetch` —
must reach the backend. Depending directly on a concrete RPC client would make those modules
impossible to test without a live node and would hard-wire the live backend into every call path.

## Decision

Define a `NodeRpc` trait that exposes the typed RPC surface, and have `service`, `ingestor`, and
`fetch` work against the `dyn NodeRpc` trait object rather than the concrete client (`service` holds an
`Arc<dyn NodeRpc>`; `ingestor` and `fetch` borrow it as `&dyn NodeRpc`). `NodeClient` (the JSON-RPC
implementation) is one implementor; tests inject a `FakeNode`, and darkside injects a `DarksideNode`
(see [0006](0006-darkside-mock-via-noderpc-seam.md)).

## Consequences

- The RPC↔gRPC translation, reorg handling, and block assembly become unit-testable against canned
  responses, with no network.
- The same seam is the darkside injection point, so darkside reuses the production read path unchanged.
- One dynamic-dispatch indirection per node call — negligible against network I/O — and `Streamer`
  stays cheap to clone, since it holds an `Arc`.
