# 0001. Backend node is zebrad over plain-HTTP JSON-RPC

## Context

The proxy needs a full node to source blocks, the mempool, tree states, and transparent-address
indices. The node is expected to run on the same host as the proxy, on a loopback connection. Two
full-node implementations expose a compatible JSON-RPC surface, so a backend had to be chosen.

## Decision

Target `zebrad` as the backend. Reach it over JSON-RPC via plain HTTP `POST` with HTTP Basic auth
(`rpcuser`/`rpcpassword` read from flags or a `zcash.conf`), without TLS, since the connection is
local and never crosses the open network. `zebrad` is the only supported backend; `zcashd` is out of
scope, so the RPC surface, the error-code mapping, and the tests all target `zebrad`'s behaviour
specifically.

## Consequences

- The server↔node hop carries no transport security. This is acceptable only because it is loopback;
  the wallet↔server hop is the one protected by TLS ([0012](0012-tls-default-insecure-flags.md)).
- The node is trusted for consensus and validation; the proxy reimplements neither.
- Targeting one backend keeps the JSON-RPC quirks unambiguous: error codes and response shapes are
  pinned to `zebrad`, not hedged across implementations.
- Some RPC responses are node-specific in shape and behaviour. The exact fields and quirks the proxy
  relies on are pinned by unit tests against a fake node and a `wiremock` HTTP layer, so a node-side
  change surfaces as a test failure rather than a silent runtime error.
