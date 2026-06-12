# Changelog

All notable changes to this project are documented here. The format is loosely based on
[Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### F0 — Skeleton
- Project scaffold, dependencies, and architecture docs.
- gRPC `CompactTxStreamer` service generated from the `.proto` contract.
- JSON-RPC client for the zebrad backend (generic `raw_request` + typed `getinfo`/`getblockchaininfo`).
- Configuration from CLI flags and an optional `zcash.conf`.
- Implemented `GetLightdInfo` and `GetLatestBlock`; remaining methods return `unimplemented`.
