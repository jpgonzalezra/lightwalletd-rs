# syntax=docker/dockerfile:1
#
# Builds the reference lightwalletd (Go), pinned at commit fdf1af5, with a minimal
# patch so the block ingestor can start at a nonzero height (LWD_FIRST_HEIGHT)
# instead of the hardcoded genesis start. This only changes where ingestion
# begins; the measured read path is untouched. See the benchmark methodology ADR.
#
# No --platform, so it builds for the host architecture (arm64 on arm64 hosts).

FROM golang:1.25 AS build
ARG LWD_COMMIT=fdf1af5
RUN git clone https://github.com/zcash/lightwalletd /src
WORKDIR /src
RUN git checkout ${LWD_COMMIT}

# Anchor the cache/ingestor at LWD_FIRST_HEIGHT (default 0). One call-site edit
# plus a small helper file; the grep guards against the upstream line drifting.
COPY go-lwd/benchstart.go /src/cmd/benchstart.go
RUN sed -i \
      's/common.NewBlockCache(dbPath, chainName, 0, syncFromHeight)/common.NewBlockCache(dbPath, chainName, firstHeightFromEnv(), syncFromHeight)/' \
      cmd/root.go \
 && grep -q 'firstHeightFromEnv()' cmd/root.go
RUN make

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --user-group lwd
COPY --from=build /src/lightwalletd /usr/local/bin/lightwalletd
USER lwd
# gRPC (9067) and Prometheus metrics over HTTP (9068).
EXPOSE 9067 9068
ENTRYPOINT ["lightwalletd"]
