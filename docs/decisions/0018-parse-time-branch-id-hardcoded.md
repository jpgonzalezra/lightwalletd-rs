# 0018. Keep the parse-time consensus branch ID hardcoded at `Nu5`

## Context

`zcash_primitives::transaction::Transaction::read` takes a consensus `BranchId`, and every call site
in `src/compact.rs` passes a hardcoded `BranchId::Nu5`. With NU6.3 introducing the v6 transaction
format, the question resurfaced: should the parse-time branch ID be derived from the block height and
the node's reported upgrade table instead?

The parameter is only consulted for pre-v5 transactions. For those, the transaction ID is the legacy
double SHA-256 of the raw bytes, which the branch ID does not affect. v5 and v6 transactions carry
`nConsensusBranchId` on the wire and ignore the passed value entirely.

## Decision

Keep the hardcoded `BranchId::Nu5`, documented in the `src/compact.rs` module doc. For everything this
server extracts from a transaction — bundle contents and txids — the parameter has no observable
effect: pre-v5 txids do not depend on it, and v5/v6 read the branch ID from the wire. Deriving it from
height would add a height-to-branch table (or a per-block node consult) that changes no output.

## Consequences

- No behavior change across NU6.3; verified against real testnet v6 blocks, whose locally computed
  txids match the node's (including the block carrying the first Ironwood action).
- If a future transaction format starts consulting the parse-time branch ID for data this server
  serves, this decision must be revisited; the module doc points here.
- The value reads as "current network upgrade" but means "ignored for everything we serve" — the
  module doc states this explicitly to prevent the next reader from "fixing" it.
- Should this decision ever be wrong in a way that produces a diverging txid (a future format
  consulting the branch ID after all, or a mistaken revisit), `fetch`'s cross-check against the node's
  verbose `getblock` txid list ([0020](0020-windowed-ingest-batched-commits.md)) turns that divergence
  into a loud, rejected block rather than a silently wrong txid served to a wallet. That check is a
  safety net for this decision being wrong, not a substitute for verifying it is right.
