# The whole spike builds and runs in a container on purpose: `cargo build` executes build scripts
# and proc-macros from roughly 750 unaudited crates, and none of that should run on a development
# host. Discarding this experiment is deleting an image.
FROM rust:1.96-slim AS build

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates git pkg-config libssl-dev protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*

# Crates in this tree stamp build-time git metadata, which has no checkout to read here. Idempotent
# mode makes them emit placeholders instead of failing the build.
ENV VERGEN_IDEMPOTENT=1

WORKDIR /src
# No lockfile on this branch: the SDK comes from a git revision, which pins the tree exactly the way
# a lockfile would, and resolving it needs the network anyway.
COPY Cargo.toml ./
COPY src ./src

# Cache mounts keep the registry and target dir out of the image and off the host.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release \
 && mkdir -p /out \
 && cp target/release/sp target/release/dial target/release/repro /out/

# Must match the build image's distro: rust:1.96-slim is Debian 13 (glibc 2.41), and a bookworm
# runtime rejects the binary with a GLIBC_2.38 error.
FROM debian:trixie-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libssl3t64 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/sp /out/dial /out/repro /usr/local/bin/
