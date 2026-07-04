# Contributing

## Build requirements

- Rust, via the toolchain pinned in `rust-toolchain.toml` (installed automatically by `rustup`).
- `protoc`, the Protocol Buffers compiler, on `PATH`.

## Before opening a pull request

Run the full verification gate and make sure it passes:

```sh
make verify   # fmt + clippy -D warnings + build + test
```

## Commit style

- [Conventional Commits](https://www.conventionalcommits.org/): `type: subject`.
- Single-line subject, imperative mood, no scope, no body.
- Keep commits small and atomic; each one should build on its own.

## Design decisions and protocol references

- Architectural or design decisions are recorded as short ADRs under
  [`docs/decisions/`](docs/decisions/README.md) (Context / Decision / Consequences), linked from
  `docs/ARCHITECTURE.md`.
- Changes backed by a ZIP, BIP, or a section of the Zcash Protocol Spec add a reference to
  [`docs/protocol-references.md`](docs/protocol-references.md).

## Security issues

Do not open a public issue for a security vulnerability — see [`SECURITY.md`](SECURITY.md) for how
to report it privately.
