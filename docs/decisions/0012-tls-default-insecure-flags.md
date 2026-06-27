# 0012. TLS by default; dangerous features gated behind *-very-insecure flags

## Context

The server is meant to face the public internet. The wallet↔server hop carries query metadata that
must stay private, and several capabilities are useful for development or testing but unsafe to expose
in production.

## Decision

Run the wallet-facing gRPC server over TLS by default, requiring a certificate and key. Plaintext is
available only behind `--no-tls-very-insecure`, which logs a warning on startup. Dangerous or
testing-only features follow the same convention — off by default, opt-in through a flag whose name
carries a `-very-insecure` suffix, and never configurable from `zcash.conf`: `--darkside-very-insecure`
(the mock chain) and `--ping-very-insecure` (the client-controlled `Ping`, a DoS vector if left open).

## Consequences

- A default deployment is encrypted and exposes no testing surface.
- The `-very-insecure` naming makes every dangerous opt-in obvious at the call site and in process
  listings.
- The server↔node hop stays plain HTTP on purpose ([0001](0001-backend-zebrad-over-zcashd.md)); it is
  loopback and never crosses the open network.
