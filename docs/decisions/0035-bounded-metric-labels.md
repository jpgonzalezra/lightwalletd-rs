# 0035. Take metric label values from the server, not from the request

## Context

The Prometheus layer runs above tonic's router, which is the only place that knows which methods
exist. All it gets is an `http::Request`, so the label values it can reach are the request's verb
and its path, split at the first slash into `grpc_service` and `grpc_method`. Six metric families
are keyed on those: `grpc_server_started_total`, `grpc_server_handled_total` and the
`grpc_server_handling_seconds` histogram take the split path, and the `function_calls_*`
compatibility trio takes the verb and the whole path.

Both halves are the client's to choose. HTTP/2 accepts any token as `:method`, and any path is a
valid request: the router answers one it does not serve with an ordinary `Unimplemented`, which
travels back up through the layer and gets recorded like anything else. Prometheus holds the child
metric for each label combination in a map that only an explicit `reset` empties, so every new pair
costs memory the process never gives back. A few connections issuing cheap, well-formed requests are
enough to grow a server's memory until it is killed, and an operator who blocks the source still has
to restart to get the memory back.

## Decision

The layer is ours (`metrics::BoundedMetricsLayer`) and it decides the label values before handing
the call to the recording future the `tonic_prometheus_layer` crate exposes:

- A `POST` to a path the binary serves keeps its own labels, as before.
- Everything else lands in `/unknown/unknown`, with the verb reduced to `POST` or `OTHER`.

The server decodes the set of served paths once, at startup, from the descriptor set `build.rs`
emits and the reflection service already publishes, plus reflection's own. It is the union of the
live and darkside services rather than the ones a given process registers, because the builder runs
before either mode adds its services. A darkside path labelled honestly on a live server costs one
series that is already counted. When the descriptor set cannot be decoded the builder fails and
startup stops, rather than quietly recording everything as unknown.

The request itself is not touched. Normalization changes what a call is recorded as and nothing
else, so the router below sees the verb and URI the client sent.

`--no-metrics` now takes the layer out instead of only closing the endpoint.

## Consequences

Label cardinality is bounded by the `.proto` files: one series per method, plus one bucket for
everything else. Unrouted traffic stays visible as volume in that bucket, which is the signal worth
having while someone is probing paths. The series names and label names are unchanged, so existing
dashboards keep working.

Two costs. A method added to a `.proto` file needs a rebuild before its label appears, which is
already true of the code that serves it. The layer also leans on `MetricsFuture::new`, a public but
secondary entry point of the crate: an incompatible change there is a compile error, not a silent
regression, and `tests/metrics_cardinality.rs` pins the resulting series either way.
