# 0026. Serve gRPC-web natively, behind an off-by-default runtime flag

## Context

Browsers cannot speak gRPC. They expose no API for the trailers and the HTTP/2 framing gRPC requires,
so a web wallet reaches a lightwalletd through gRPC-web: the same protobuf messages, length-prefixed
inside an ordinary HTTP request, with the trailers carried as a final frame in the response body. The
usual answer is to put a translating proxy (Envoy, `grpcwebproxy`) in front of the server, which means
an operator who wants to serve browser wallets has to run and configure a second process.

`tonic-web` implements the translation as a `tower` layer inside the server, so the capability is a
few lines of wiring away. Two things kept it from being just switched on:

- gRPC-web over a plaintext port needs the server to accept HTTP/1.1, since a browser does not use
  prior-knowledge HTTP/2 on cleartext connections. That changes what the listener answers, and every
  malformed request that used to be dropped at the connection preface now reaches the router.
- A browser will not hand a cross-origin response to JavaScript without CORS headers, so the server
  has to state a policy: which origins, which request headers, and which response headers the page is
  allowed to read.

Both are policy an operator should choose, not side effects of an upgrade.

## Decision

Serve gRPC-web from the gRPC port, off by default, enabled with `--grpc-web`.

- `--grpc-web` adds two layers around the router and turns on `accept_http1`. Over TLS the flag is
  redundant for the browser's sake (ALPN settles on HTTP/2 there, and gRPC-web rides that just as
  well), but it stays the single switch for the transport either way.
- `--grpc-web-allow-origin <ORIGIN>`, repeatable, restricts the transport to an allowlist. With none
  given, any origin is allowed and startup logs why that is a choice: CORS bounds which origins a
  browser hands the response to, and is not an authentication mechanism. A public instance serving
  unauthenticated chain data to wallets on many origins is the normal case for a lightwalletd, so
  "any" is the useful default once the transport is on at all.
- Origins are validated at startup against the exact form a browser sends (`scheme://host[:port]`,
  no path, no trailing slash, no default port). A value that cannot match is rejected with the form
  that works, because the failure it would otherwise cause surfaces in the browser as an opaque CORS
  error that names nothing.
- The layer order is CORS, then metrics, then gRPC-web, then the router. CORS has to be outermost: a
  preflight is an `OPTIONS` with no gRPC content type, which the gRPC-web layer answers with `400`.
  It is the CORS layer that short-circuits the preflight before anything else sees it.
- `grpc-status`, `grpc-message` and `grpc-status-details-bin` are exposed to JavaScript. gRPC puts
  the outcome of a trailers-only response (every early rejection: `Unimplemented`, `InvalidArgument`,
  an expired deadline) in HTTP headers, and a browser gives a page only the headers the server
  exposed. Without this, such calls reach the client with their reason stripped off.
- Preflights are cached for an hour (`Access-Control-Max-Age`), since the policy is static for the
  life of the process and a wallet calls many methods.
- The gate is the runtime flag and nothing else: the transport is compiled in unconditionally, with
  no Cargo feature behind it. `tonic-web` is the only new crate, its dependencies are already in the
  tree, and CORS is a feature of a `tower-http` this build already carries, so a feature would save
  nothing measurable at build time while adding a second axis on which a deployment can lack the
  transport. That is the opposite of `readstate`, which is feature-gated precisely because it pulls
  RocksDB and the zebra crate tree.

Both layers are wired as `tower::util::option_layer`, so an enabled and a disabled server are the
same concrete type. That keeps one `server_builder` for both serve paths (live and darkside) and lets
the integration tests exercise the deployed stack rather than a look-alike assembled in the test.

## Consequences

- A browser wallet can talk to lightwalletd-rs directly, with no proxy in the deployment.
- **Client streaming does not work over this transport**: gRPC-web has no way to send a request
  stream, which is a limitation of the protocol and not of this implementation. `GetTaddressBalance`
  in its streaming form (`proto/service.proto`, `GetTaddressBalanceStream`) is therefore unreachable
  from a browser; its unary sibling is not. All eight server-streaming methods work.
- With the flag on, the listener accepts HTTP/1.1, so it answers requests it previously dropped at
  the connection preface. That is the point of the flag being explicit and off by default.
- The per-connection limits of [0013](0013-resource-limits.md) are HTTP/2 mechanisms
  (`max_concurrent_streams`, the keepalive pair), so they bound nothing on an HTTP/1.1 connection.
  What bounds a browser client there is one request per connection plus `tcp_keepalive`; there is no
  global cap on connections either way. The transport being off by default is what keeps this from
  widening the exposure of a default deployment.
- The transport is tested by hand-built HTTP requests (`tests/grpc_web.rs`), which is necessary but
  not sufficient: a test that sets the headers itself can pass while a browser fails, because the
  browser is what decides to send a preflight and what a page may read. `contrib/grpc-web-smoke.html`
  covers that last step manually, outside CI.
- The CORS policy is fixed apart from the origin list. Allowed request headers, exposed response
  headers and the preflight lifetime are what a gRPC-web client needs; making them tunable would
  invite policies that silently break clients. `authorization` is allowed on the request even though
  this server never reads it, because the alternative is a deployment behind token auth whose
  preflights fail with no way to widen the policy.
