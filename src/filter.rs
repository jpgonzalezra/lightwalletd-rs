//! Pruning a compact block (or a single compact transaction) down to the requested value pools.
//!
//! Reusable at block level (`GetBlockRange`) and at transaction level (mempool streaming).

use tonic::Status;

use crate::proto::{CompactBlock, CompactTx, PoolType};

/// Which value pools to keep when pruning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pools {
    pub transparent: bool,
    pub sapling: bool,
    pub orchard: bool,
    pub ironwood: bool,
}

impl Pools {
    /// Resolve a gRPC `pool_types` list into the pools to keep. An empty list means the legacy
    /// default: shielded (Sapling + Orchard + Ironwood) only, with transparent inputs/outputs
    /// stripped.
    ///
    /// Resolve once per request and pass the result down: [`Pools`] is `Copy`, and the list itself
    /// is client-supplied, so re-deriving it per block would put the client in charge of how often
    /// the server walks it.
    pub fn from_pool_types(pool_types: &[i32]) -> Self {
        Self::resolve(
            pool_types,
            pool_types.contains(&(PoolType::Transparent as i32)),
            pool_types.is_empty(),
        )
    }

    /// Resolve as [`Pools::from_pool_types`] would for the same request with every `Transparent`
    /// entry removed. `GetBlockRangeNullifiers` never returns transparent data (a wallet that
    /// wants it calls `GetBlockRange`), so it drops a transparent entry instead of honoring it,
    /// the same removal Go makes in `frontend/service.go` before delegating to the pool-filtering
    /// path `GetBlockRange` uses. A request naming transparent alone then has nothing left to
    /// honor, and falls back to the shielded default exactly as an empty request does.
    pub fn from_pool_types_dropping_transparent(pool_types: &[i32]) -> Self {
        let names_only_transparent = pool_types
            .iter()
            .all(|&pool_type| pool_type == PoolType::Transparent as i32);
        Self::resolve(pool_types, false, names_only_transparent)
    }

    fn resolve(pool_types: &[i32], transparent: bool, defaults_to_shielded: bool) -> Self {
        Self {
            transparent,
            sapling: defaults_to_shielded || pool_types.contains(&(PoolType::Sapling as i32)),
            orchard: defaults_to_shielded || pool_types.contains(&(PoolType::Orchard as i32)),
            ironwood: defaults_to_shielded || pool_types.contains(&(PoolType::Ironwood as i32)),
        }
    }
}

/// Most entries a `pool_types` filter may carry. Four pools exist, so anything past four is
/// already redundant, but the cap is not four: `PoolType` is an open proto3 enum, so a client
/// built against a newer protocol may name a pool this build has never heard of, and nothing
/// forbids duplicates. Four times the current set covers both and survives a new pool without
/// anyone revisiting the number. Any small constant would do. Without one the ceiling is the
/// 4 MiB gRPC decode limit, about 4.19 million single-byte entries.
pub const MAX_POOL_TYPES: usize = 16;

/// Reject a `pool_types` list that is longer than [`MAX_POOL_TYPES`] or contains
/// `PoolType::Invalid`. Shared by the block-range and mempool methods so they validate the same
/// contract identically.
pub fn validate_pool_types(pool_types: &[i32]) -> Result<(), Status> {
    if pool_types.len() > MAX_POOL_TYPES {
        return Err(Status::resource_exhausted(format!(
            "more than {MAX_POOL_TYPES} pool types requested"
        )));
    }
    if pool_types.contains(&(PoolType::Invalid as i32)) {
        return Err(Status::invalid_argument("invalid pool type requested"));
    }
    Ok(())
}

/// Prune every transaction in a compact block to the requested value pools, then drop any transaction
/// left with no components. Empty *blocks* are kept (a wallet still needs every height); only the empty
/// transactions within them are removed.
pub fn filter_block_to_pools(mut block: CompactBlock, pools: Pools) -> CompactBlock {
    for tx in &mut block.vtx {
        filter_tx_to_pools(tx, pools);
    }
    block.vtx.retain(|tx| {
        !tx.spends.is_empty()
            || !tx.outputs.is_empty()
            || !tx.actions.is_empty()
            || !tx.ironwood_actions.is_empty()
            || !tx.vin.is_empty()
            || !tx.vout.is_empty()
    });
    block
}

/// Strip from a single compact transaction the value pools not present in `pools`.
pub fn filter_tx_to_pools(tx: &mut CompactTx, pools: Pools) {
    if !pools.sapling {
        tx.spends.clear();
        tx.outputs.clear();
    }
    if !pools.orchard {
        tx.actions.clear();
    }
    if !pools.ironwood {
        tx.ironwood_actions.clear();
    }
    if !pools.transparent {
        tx.vin.clear();
        tx.vout.clear();
    }
}

/// `GetBlockRangeNullifiers`'s transform: prune to the requested (shielded) value pools exactly as
/// `GetBlockRange` does (including dropping any transaction the pool filter leaves with no
/// components), then reduce what survives to nullifiers only.
///
/// The order matters and mirrors Go (`frontend/service.go` `GetBlockRangeNullifiers`, which delegates
/// pool filtering to the same `common.GetBlockRange`/`filterBlockPool` used by the plain range call,
/// then reduces the surviving transactions to nullifiers): the emptiness check runs on the
/// *pre-nullifier-reduction* transaction, using the same fields `GetBlockRange` checks (spends,
/// outputs, actions, ironwood actions, vin, vout). The nullifier reduction that follows only clears
/// fields *within* the transactions that survive that check; it does not run the drop again, so a
/// transaction can (as in Go) end up in the response with only its `index`/`txid`/`fee` set if the
/// pool filter kept it for a component (e.g. a Sapling output) that the nullifier reduction then
/// clears. This is intentional parity with Go, not an oversight.
///
/// `pools` is the request's filter with transparent already dropped, as
/// [`Pools::from_pool_types_dropping_transparent`] builds it.
pub fn filter_block_to_pools_nullifiers_only(block: CompactBlock, pools: Pools) -> CompactBlock {
    nullifiers_only(filter_block_to_pools(block, pools))
}

/// Reduce a compact block to shielded nullifiers only: Sapling spend nullifiers and the nullifier of
/// each Orchard and Ironwood action. Drops transparent data, Sapling outputs, the rest of every
/// action, and the commitment tree sizes (`GetBlockNullifiers`/`GetBlockRangeNullifiers`).
pub fn nullifiers_only(mut block: CompactBlock) -> CompactBlock {
    if let Some(metadata) = block.chain_metadata.as_mut() {
        metadata.sapling_commitment_tree_size = 0;
        metadata.orchard_commitment_tree_size = 0;
        metadata.ironwood_commitment_tree_size = 0;
    }
    for tx in &mut block.vtx {
        tx.outputs.clear();
        tx.vin.clear();
        tx.vout.clear();
        for action in tx.actions.iter_mut().chain(tx.ironwood_actions.iter_mut()) {
            action.cmx.clear();
            action.ephemeral_key.clear();
            action.ciphertext.clear();
        }
    }
    block
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;
    use crate::proto::{
        ChainMetadata, CompactOrchardAction, CompactSaplingOutput, CompactSaplingSpend,
        CompactTxIn, TxOut,
    };

    fn pools(pool_types: &[i32]) -> Pools {
        Pools::from_pool_types(pool_types)
    }

    fn nullifier_pools(pool_types: &[i32]) -> Pools {
        Pools::from_pool_types_dropping_transparent(pool_types)
    }

    fn block_with_every_pool() -> CompactBlock {
        let tx = CompactTx {
            spends: vec![CompactSaplingSpend::default()],
            outputs: vec![CompactSaplingOutput::default()],
            actions: vec![CompactOrchardAction::default()],
            ironwood_actions: vec![CompactOrchardAction::default()],
            vin: vec![CompactTxIn::default()],
            vout: vec![TxOut::default()],
            ..Default::default()
        };
        CompactBlock {
            vtx: vec![tx],
            ..Default::default()
        }
    }

    #[test]
    fn empty_pool_types_keeps_shielded_and_strips_transparent() {
        let block = filter_block_to_pools(block_with_every_pool(), pools(&[]));
        let tx = &block.vtx[0];
        assert!(tx.vin.is_empty() && tx.vout.is_empty());
        assert!(!tx.outputs.is_empty() && !tx.actions.is_empty() && !tx.spends.is_empty());
        assert!(!tx.ironwood_actions.is_empty());
    }

    #[test]
    fn transparent_only_strips_shielded() {
        let block = filter_block_to_pools(
            block_with_every_pool(),
            pools(&[PoolType::Transparent as i32]),
        );
        let tx = &block.vtx[0];
        assert!(!tx.vin.is_empty() && !tx.vout.is_empty());
        assert!(tx.spends.is_empty() && tx.outputs.is_empty() && tx.actions.is_empty());
        assert!(tx.ironwood_actions.is_empty());
    }

    #[test]
    fn ironwood_only_strips_every_other_pool() {
        let block =
            filter_block_to_pools(block_with_every_pool(), pools(&[PoolType::Ironwood as i32]));
        let tx = &block.vtx[0];
        assert!(!tx.ironwood_actions.is_empty());
        assert!(tx.spends.is_empty() && tx.outputs.is_empty() && tx.actions.is_empty());
        assert!(tx.vin.is_empty() && tx.vout.is_empty());
    }

    #[test]
    fn transaction_left_ironwood_only_survives_retention() {
        let ironwood_only = CompactTx {
            ironwood_actions: vec![CompactOrchardAction::default()],
            vin: vec![CompactTxIn::default()],
            ..Default::default()
        };
        let block = CompactBlock {
            vtx: vec![ironwood_only],
            ..Default::default()
        };

        let filtered = filter_block_to_pools(block, pools(&[PoolType::Ironwood as i32]));
        assert_eq!(filtered.vtx.len(), 1);
        assert!(!filtered.vtx[0].ironwood_actions.is_empty());
    }

    #[test]
    fn drops_transactions_left_empty_after_pool_filter() {
        let transparent_only = CompactTx {
            vin: vec![CompactTxIn::default()],
            ..Default::default()
        };
        let sapling_only = CompactTx {
            spends: vec![CompactSaplingSpend::default()],
            ..Default::default()
        };
        let block = CompactBlock {
            vtx: vec![transparent_only, sapling_only],
            ..Default::default()
        };

        // Keep sapling only: the transparent-only tx is left empty and dropped.
        let filtered = filter_block_to_pools(block, pools(&[PoolType::Sapling as i32]));
        assert_eq!(filtered.vtx.len(), 1);
        assert!(!filtered.vtx[0].spends.is_empty());
    }

    #[test]
    fn keeps_block_when_all_transactions_filtered_out() {
        let transparent_only = CompactTx {
            vout: vec![TxOut::default()],
            ..Default::default()
        };
        let block = CompactBlock {
            height: 42,
            vtx: vec![transparent_only],
            ..Default::default()
        };

        // Filtering to sapling-only empties the sole tx; the block itself is still returned.
        let filtered = filter_block_to_pools(block, pools(&[PoolType::Sapling as i32]));
        assert!(filtered.vtx.is_empty());
        assert_eq!(filtered.height, 42);
    }

    #[test]
    fn nullifiers_only_keeps_nullifiers_and_drops_everything_else() {
        let mut block = block_with_every_pool();
        block.vtx[0].actions[0] = CompactOrchardAction {
            nullifier: vec![1; 32],
            cmx: vec![2; 32],
            ephemeral_key: vec![3; 32],
            ciphertext: vec![4; 52],
        };
        block.vtx[0].ironwood_actions[0] = CompactOrchardAction {
            nullifier: vec![5; 32],
            cmx: vec![6; 32],
            ephemeral_key: vec![7; 32],
            ciphertext: vec![8; 52],
        };
        block.chain_metadata = Some(ChainMetadata {
            sapling_commitment_tree_size: 99,
            orchard_commitment_tree_size: 99,
            ironwood_commitment_tree_size: 99,
        });

        let block = nullifiers_only(block);
        let tx = &block.vtx[0];

        // Kept: Sapling spend nullifiers and the Orchard/Ironwood action nullifiers.
        assert!(!tx.spends.is_empty());
        assert_eq!(tx.actions[0].nullifier, vec![1; 32]);
        assert_eq!(tx.ironwood_actions[0].nullifier, vec![5; 32]);
        // Dropped: outputs, transparent data, the rest of every action, and the tree sizes.
        assert!(tx.outputs.is_empty() && tx.vin.is_empty() && tx.vout.is_empty());
        assert!(tx.actions[0].cmx.is_empty() && tx.actions[0].ciphertext.is_empty());
        assert!(
            tx.ironwood_actions[0].cmx.is_empty() && tx.ironwood_actions[0].ciphertext.is_empty()
        );
        let metadata = block.chain_metadata.unwrap();
        assert_eq!(metadata.sapling_commitment_tree_size, 0);
        assert_eq!(metadata.orchard_commitment_tree_size, 0);
        assert_eq!(metadata.ironwood_commitment_tree_size, 0);
    }

    #[test]
    fn range_nullifiers_pool_filter_always_drops_transparent() {
        // Requesting `TRANSPARENT` alone strips it to an empty pool-types list (transparent is
        // dropped, unconditionally, from every `GetBlockRangeNullifiers` request), which then falls
        // back to the legacy shielded-only default, the same as an empty request. A
        // transparent-only transaction has no shielded component to fall back to, so it is dropped.
        let transparent_only = CompactTx {
            vin: vec![CompactTxIn::default()],
            ..Default::default()
        };
        let block = CompactBlock {
            vtx: vec![transparent_only],
            ..Default::default()
        };
        let filtered = filter_block_to_pools_nullifiers_only(
            block,
            nullifier_pools(&[PoolType::Transparent as i32]),
        );
        assert!(filtered.vtx.is_empty());
    }

    #[test]
    fn range_nullifiers_empty_pool_types_keeps_shielded_nullifiers_only() {
        let block = block_with_every_pool();
        let filtered = filter_block_to_pools_nullifiers_only(block, nullifier_pools(&[]));
        assert_eq!(filtered.vtx.len(), 1);
        let tx = &filtered.vtx[0];
        assert!(!tx.spends.is_empty());
        assert!(!tx.actions.is_empty());
        assert!(!tx.ironwood_actions.is_empty());
        assert!(tx.vin.is_empty() && tx.vout.is_empty() && tx.outputs.is_empty());
    }

    #[test]
    fn range_nullifiers_sapling_only_excludes_orchard_and_ironwood() {
        let block = block_with_every_pool();
        let filtered = filter_block_to_pools_nullifiers_only(
            block,
            nullifier_pools(&[PoolType::Sapling as i32]),
        );
        assert_eq!(filtered.vtx.len(), 1);
        let tx = &filtered.vtx[0];
        assert!(!tx.spends.is_empty());
        assert!(tx.actions.is_empty() && tx.ironwood_actions.is_empty());
    }

    #[test]
    fn range_nullifiers_orchard_only_excludes_sapling_and_ironwood() {
        let block = block_with_every_pool();
        let filtered = filter_block_to_pools_nullifiers_only(
            block,
            nullifier_pools(&[PoolType::Orchard as i32]),
        );
        assert_eq!(filtered.vtx.len(), 1);
        let tx = &filtered.vtx[0];
        assert!(!tx.actions.is_empty());
        assert!(tx.spends.is_empty() && tx.ironwood_actions.is_empty());
    }

    #[test]
    fn range_nullifiers_ironwood_only_excludes_sapling_and_orchard() {
        let block = block_with_every_pool();
        let filtered = filter_block_to_pools_nullifiers_only(
            block,
            nullifier_pools(&[PoolType::Ironwood as i32]),
        );
        assert_eq!(filtered.vtx.len(), 1);
        let tx = &filtered.vtx[0];
        assert!(!tx.ironwood_actions.is_empty());
        assert!(tx.spends.is_empty() && tx.actions.is_empty());
    }

    #[test]
    fn range_nullifiers_drops_tx_left_empty_by_pool_filter() {
        // Same emptiness check as `filter_block_to_pools`/`GetBlockRange`: a transaction that has only
        // transparent data (dropped here unconditionally) or only components outside the requested
        // pool ends up with none of spends/outputs/actions/ironwood_actions/vin/vout, and is dropped
        // from `vtx`: Go's `filterBlockPool` retains only transactions with a surviving component.
        let transparent_only = CompactTx {
            vin: vec![CompactTxIn::default()],
            vout: vec![TxOut::default()],
            ..Default::default()
        };
        let orchard_only = CompactTx {
            actions: vec![CompactOrchardAction::default()],
            ..Default::default()
        };
        let block = CompactBlock {
            vtx: vec![transparent_only, orchard_only],
            ..Default::default()
        };

        // Request Sapling only: the transparent-only tx and the orchard-only tx both end up empty.
        let filtered = filter_block_to_pools_nullifiers_only(
            block,
            nullifier_pools(&[PoolType::Sapling as i32]),
        );
        assert!(filtered.vtx.is_empty());
    }

    #[test]
    fn range_nullifiers_keeps_tx_with_sapling_output_but_no_spend_even_though_it_ends_up_empty() {
        // A shielding transaction (Sapling output, no spend) survives the pool filter, since it has
        // a surviving component (the output), but the nullifier reduction that follows unconditionally
        // clears Sapling outputs (only spend nullifiers are kept). Go performs this same two-stage
        // process without re-checking emptiness after the second stage, so the transaction reaches the
        // client with none of spends/outputs/actions/ironwood_actions/vin/vout populated. This is
        // intentional parity with Go (`frontend/service.go` `GetBlockRangeNullifiers`), not a bug: the
        // drop only happens once, at the pool-filter stage, using the pre-reduction transaction.
        let output_only = CompactTx {
            outputs: vec![CompactSaplingOutput::default()],
            ..Default::default()
        };
        let block = CompactBlock {
            vtx: vec![output_only],
            ..Default::default()
        };

        let filtered = filter_block_to_pools_nullifiers_only(
            block,
            nullifier_pools(&[PoolType::Sapling as i32]),
        );
        assert_eq!(filtered.vtx.len(), 1);
        let tx = &filtered.vtx[0];
        assert!(tx.spends.is_empty() && tx.outputs.is_empty());
    }

    #[test]
    fn range_nullifiers_index_field_is_preserved_not_renumbered() {
        // Go's `FilterTxPool` copies `Index` from the original transaction verbatim; it is not
        // renumbered to reflect the transaction's new position within the trimmed `vtx`. A dropped
        // leading transaction therefore leaves a gap in the surviving `index` values.
        let dropped = CompactTx {
            index: 0,
            vin: vec![CompactTxIn::default()],
            ..Default::default()
        };
        let kept = CompactTx {
            index: 1,
            spends: vec![CompactSaplingSpend::default()],
            ..Default::default()
        };
        let block = CompactBlock {
            vtx: vec![dropped, kept],
            ..Default::default()
        };

        let filtered = filter_block_to_pools_nullifiers_only(
            block,
            nullifier_pools(&[PoolType::Sapling as i32]),
        );
        assert_eq!(filtered.vtx.len(), 1);
        assert_eq!(filtered.vtx[0].index, 1);
    }

    #[test]
    fn pool_types_up_to_the_limit_are_accepted() {
        let pool_types = vec![PoolType::Sapling as i32; MAX_POOL_TYPES];
        assert!(validate_pool_types(&pool_types).is_ok());
    }

    #[test]
    fn pool_types_past_the_limit_are_rejected() {
        let pool_types = vec![PoolType::Sapling as i32; MAX_POOL_TYPES + 1];
        let code = validate_pool_types(&pool_types)
            .err()
            .map(|status| status.code());
        assert_eq!(code, Some(Code::ResourceExhausted));
    }

    #[test]
    fn unrecognized_pool_types_pass_validation_and_keep_nothing() {
        // An open proto3 enum admits values this build does not know. They are not the `Invalid`
        // sentinel, so they are accepted, and they name no pool, so nothing survives the filter.
        let unrecognized = 7;
        assert!(validate_pool_types(&[unrecognized]).is_ok());
        let filtered = filter_block_to_pools(block_with_every_pool(), pools(&[unrecognized]));
        assert!(filtered.vtx.is_empty());
    }

    #[test]
    fn dropping_transparent_resolves_as_if_the_entry_had_been_removed() {
        let transparent = PoolType::Transparent as i32;
        let sapling = PoolType::Sapling as i32;
        let unrecognized = 7;
        let every_shielded = Pools {
            transparent: false,
            sapling: true,
            orchard: true,
            ironwood: true,
        };
        let nothing = Pools {
            transparent: false,
            sapling: false,
            orchard: false,
            ironwood: false,
        };

        // Nothing left to name falls back to the shielded default, which is what an empty request
        // gets; a list that named something else does not fall back.
        assert_eq!(nullifier_pools(&[]), every_shielded);
        assert_eq!(nullifier_pools(&[transparent]), every_shielded);
        assert_eq!(nullifier_pools(&[transparent, transparent]), every_shielded);
        assert_eq!(nullifier_pools(&[unrecognized]), nothing);
        assert_eq!(
            nullifier_pools(&[transparent, sapling]),
            Pools {
                sapling: true,
                ..nothing
            }
        );
    }

    #[test]
    fn range_nullifiers_transparent_only_falls_back_to_the_shielded_default() {
        // Dropping transparent from a request that named nothing else leaves an empty filter, which
        // is the legacy shielded default: the wallet still gets its shielded nullifiers.
        let filtered = filter_block_to_pools_nullifiers_only(
            block_with_every_pool(),
            nullifier_pools(&[PoolType::Transparent as i32]),
        );
        let tx = &filtered.vtx[0];
        assert!(!tx.spends.is_empty() && !tx.actions.is_empty() && !tx.ironwood_actions.is_empty());
    }
}
