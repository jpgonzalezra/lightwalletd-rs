# 0019. Pin the librustzcash pre-release cohort for NU6.3

## Context

NU6.3 introduces the Ironwood shielded pool and the v6 transaction format (ZIP 229). It is live on
testnet and scheduled for mainnet; without v6 parsing, the ingestor stalls permanently at the first v6
block, so support must ship before mainnet activation. At the time of the upgrade, librustzcash v6
support exists only in pre-releases: `zcash_primitives 0.29.0-pre.0`, `zcash_protocol 0.10.0-pre.0`,
`zcash_address 0.13.0-pre.0`. These crates must move together — mixing generations (e.g.
`zcash_address 0.12` with `zcash_protocol 0.10`) splits the dependency graph into two incompatible
`zcash_protocol` versions.

## Decision

Adopt the pre-release cohort now rather than waiting for final releases, and pin each crate exactly
(`=x.y.z-pre.n`) in `Cargo.toml`. Re-bump to the final releases when they are published. After any
bump, `cargo tree -d` must show exactly one version of `zcash_protocol` and `zcash_address` — the
cohort-consistency check.

## Consequences

- v6 transactions parse and produce correct ZIP-229 txids ahead of mainnet activation, instead of
  gambling on final releases landing with enough lead time.
- Exact pins keep cargo from silently resolving a different pre-release.
- Pre-releases carry no semver guarantee: the re-bump to finals must re-run the full suite and
  re-check both crates' CHANGELOGs. The finals are also expected to set the NU6.3 mainnet activation
  height, which the pre-releases leave unset.
- Tracked follow-up: re-bump when `zcash_primitives 0.29.0` / `zcash_protocol 0.10.0` finalize.
- Re-bumped to the finals on 2026-07-14; cohort check passed (`zcash_protocol 0.10.0` sets the NU6.3
  mainnet activation height, 3,428,143; no source changes were required).
- The pre-release cohort is exactly the kind of dependency this ADR is least certain about (no semver
  guarantee, both a pre-mainnet and a re-bump risk of silent divergence from the node's own consensus
  rules). `fetch`'s cross-check of locally computed txids against the node's verbose `getblock` list
  ([0020](0020-windowed-ingest-batched-commits.md)) is the runtime backstop for that risk: a
  pre-release/node disagreement on a v6 txid fails the ingest loudly instead of silently corrupting
  wallet spend detection.
