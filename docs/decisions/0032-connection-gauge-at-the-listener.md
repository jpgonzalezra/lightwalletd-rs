# 0032. Count connections at the listener

## Context

Metrics come from a `tower` layer ([0022](0022-ops-surface-parity.md)): per-method request counts and
latency histograms. Neither says how many wallets are attached right now, which is the first thing an
operator wants when load spikes or connections leak.

The layer cannot say it either. It sees requests, and one HTTP/2 connection carries many, so what it
could count is streams. A connection's lifetime is visible only where the socket is: the accept loop,
which `tonic` owns and exposes as the `Stream` that `serve_with_incoming` consumes.

There is a second obstacle. `tonic_prometheus_layer` keeps its `prometheus::Registry` private and
creates a default one the first time it touches a metric, and `/metrics` encodes that registry. A gauge
registered anywhere else never reaches the output.

## Decision

Wrap the listener. `metrics::count_connections` maps every accepted connection into a guard that
increments `grpc_server_connections_current` on creation and decrements on drop, delegating
`AsyncRead`/`AsyncWrite`/`Connected` to the socket underneath. `run` binds the gRPC port itself and
serves with `serve_with_incoming_shutdown`.

- The name is the one lightwalletd deployments already scrape, so an existing dashboard needs no edit.
- Counting starts at accept, before the TLS handshake and before any request, so the gauge measures
  sockets this process holds. A client stalled mid-handshake counts, which is deliberate: a leak is
  exactly what shows up there.
- `metrics::init` hands `tonic_prometheus_layer` a registry we keep a handle to, before the layer can
  install its default. Every path that reaches a metric calls it first: `server_builder` for the two
  serve paths, `serve` for the endpoint.
- Once the listener comes from outside, tonic stops applying its own `tcp_*` options, so `TCP_NODELAY`
  and the keepalive from `--keepalive-interval` move to the `TcpIncoming`.

## Consequences

- gRPC, gRPC-web and TLS all ride the same accepted socket, so one wrapper covers every transport the
  server speaks. Nothing is instrumented per handler or per transport.
- The socket and the count are freed by the same `drop`, so an accounting slip cannot leave the gauge
  drifting upward.
- What it reports is sockets, not gRPC sessions. A scanner holding connections open against an
  internet-facing port lands in the number. That answers "what is this process holding"; for "how many
  wallets are syncing", the per-method counters are the better signal.
- Should the layer win the race and install its registry first, its own metrics keep working and the
  gauge goes missing, logged as an error. Metrics are diagnostics, and losing one series is no reason
  to refuse to serve.
- `metrics` becomes a public module: the connection wrapper and the endpoint are both things an
  embedder wires up.
