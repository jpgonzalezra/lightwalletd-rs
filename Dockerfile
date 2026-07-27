# Build stage: compile the release binary (needs protoc for the gRPC codegen).
#
# Pinned to the same Debian release as the runtime stage below. `rust:1-slim` tracks the newest
# Debian, whose glibc is ahead of the runtime's: a dependency that links a symbol newer than the
# runtime provides then builds cleanly and dies on startup with "GLIBC_2.xx not found".
FROM rust:1-slim-bookworm@sha256:99e09cb2284e2ddbb73a995deee3e91783fd04d177602ccf6eab326d778ee777 AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY . .
RUN cargo build --release --locked

# Runtime stage: a slim image with just the binary, run as a non-root user.
FROM debian:bookworm-slim@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df
RUN useradd --system --uid 10001 --user-group lwd
COPY --from=builder /build/target/release/lightwalletd-rs /usr/local/bin/lightwalletd-rs
USER lwd
# gRPC (9067) and Prometheus metrics (9100) by default.
EXPOSE 9067 9100
ENTRYPOINT ["/usr/local/bin/lightwalletd-rs"]
