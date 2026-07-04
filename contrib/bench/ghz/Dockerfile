# syntax=docker/dockerfile:1
#
# Builds the ghz gRPC load client natively (no --platform), because the published
# image is amd64-only and would run emulated on arm64 — risking the client, not the
# proxy, becoming the bottleneck. Version is captured in the report.
FROM golang:1.25 AS build
ARG GHZ_VERSION=v0.121.0
RUN go install -ldflags "-X main.version=${GHZ_VERSION}" \
      github.com/bojand/ghz/cmd/ghz@${GHZ_VERSION}

FROM debian:bookworm-slim
COPY --from=build /go/bin/ghz /usr/local/bin/ghz
ENTRYPOINT ["ghz"]
