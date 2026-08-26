# Contributing

## Build requirements

- Rust, via the toolchain pinned in `rust-toolchain.toml` (installed automatically by `rustup`).
- `protoc`, the Protocol Buffers compiler, on `PATH`.

## Before opening a pull request

Run the full verification gate and make sure it passes:

```sh
make verify   # fmt + prose + clippy -D warnings + build + test
```

## Commit style

- [Conventional Commits](https://www.conventionalcommits.org/): `type: subject`.
- Single-line subject, imperative mood, no scope, no body.
- Keep commits small and atomic; each one should build on its own.

## Prose

Markdown files and Rust doc comments go through a small linter, which `make verify` runs:

```sh
make prose
```

It flags em dashes, the vocabulary AI drafts overuse, wordy phrases that have a one-word
equivalent, and connectives used as paragraph glue. Sentence length, contractions and modal
verbs are left alone, since this project varies them on purpose. Code, URLs and link targets
are never read. When a line quotes wording set elsewhere, such as a log message or an official
spec title, add `prose-lint: allow` to it.

## Design decisions and protocol references

- Architectural or design decisions are recorded as short ADRs under
  [`docs/decisions/`](docs/decisions/README.md) (Context / Decision / Consequences), linked from
  `docs/ARCHITECTURE.md`.
- Changes backed by a ZIP, BIP, or a section of the Zcash Protocol Spec add a reference to
  [`docs/protocol-references.md`](docs/protocol-references.md).

## Security issues

Do not open a public issue for a security vulnerability; see [`SECURITY.md`](SECURITY.md) for how
to report it privately.
