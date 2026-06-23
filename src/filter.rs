//! Pruning a compact block — or a single compact transaction — down to the requested value pools.
//!
//! Reusable at block level (`GetBlockRange`) and at transaction level (mempool streaming).

use crate::proto::{CompactBlock, CompactTx, PoolType};

/// Which value pools to keep when pruning.
#[derive(Debug, Clone, Copy)]
pub struct Pools {
    pub transparent: bool,
    pub sapling: bool,
    pub orchard: bool,
}

impl Pools {
    /// Resolve a gRPC `pool_types` list into the pools to keep. An empty list means the legacy
    /// default: shielded (Sapling + Orchard) only, with transparent inputs/outputs stripped.
    pub fn from_pool_types(pool_types: &[i32]) -> Self {
        Self {
            transparent: pool_types.contains(&(PoolType::Transparent as i32)),
            sapling: pool_types.is_empty() || pool_types.contains(&(PoolType::Sapling as i32)),
            orchard: pool_types.is_empty() || pool_types.contains(&(PoolType::Orchard as i32)),
        }
    }
}

/// Prune every transaction in a compact block to the requested value pools, then drop any transaction
/// left with no components. Empty *blocks* are kept (a wallet still needs every height); only the empty
/// transactions within them are removed.
pub fn filter_block_to_pools(mut block: CompactBlock, pool_types: &[i32]) -> CompactBlock {
    let pools = Pools::from_pool_types(pool_types);
    for tx in &mut block.vtx {
        filter_tx_to_pools(tx, pools);
    }
    block.vtx.retain(|tx| {
        !tx.spends.is_empty()
            || !tx.outputs.is_empty()
            || !tx.actions.is_empty()
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
    if !pools.transparent {
        tx.vin.clear();
        tx.vout.clear();
    }
}

/// Reduce a compact block to shielded nullifiers only: Sapling spend nullifiers and the nullifier of
/// each Orchard action. Drops transparent data, Sapling outputs, the rest of every Orchard action,
/// and the commitment tree sizes (`GetBlockNullifiers`/`GetBlockRangeNullifiers`).
pub fn nullifiers_only(mut block: CompactBlock) -> CompactBlock {
    if let Some(metadata) = block.chain_metadata.as_mut() {
        metadata.sapling_commitment_tree_size = 0;
        metadata.orchard_commitment_tree_size = 0;
    }
    for tx in &mut block.vtx {
        tx.outputs.clear();
        tx.vin.clear();
        tx.vout.clear();
        for action in &mut tx.actions {
            action.cmx.clear();
            action.ephemeral_key.clear();
            action.ciphertext.clear();
        }
    }
    block
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{
        ChainMetadata, CompactOrchardAction, CompactSaplingOutput, CompactSaplingSpend,
        CompactTxIn, TxOut,
    };

    fn block_with_every_pool() -> CompactBlock {
        let tx = CompactTx {
            spends: vec![CompactSaplingSpend::default()],
            outputs: vec![CompactSaplingOutput::default()],
            actions: vec![CompactOrchardAction::default()],
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
        let block = filter_block_to_pools(block_with_every_pool(), &[]);
        let tx = &block.vtx[0];
        assert!(tx.vin.is_empty() && tx.vout.is_empty());
        assert!(!tx.outputs.is_empty() && !tx.actions.is_empty() && !tx.spends.is_empty());
    }

    #[test]
    fn transparent_only_strips_shielded() {
        let block = filter_block_to_pools(block_with_every_pool(), &[PoolType::Transparent as i32]);
        let tx = &block.vtx[0];
        assert!(!tx.vin.is_empty() && !tx.vout.is_empty());
        assert!(tx.spends.is_empty() && tx.outputs.is_empty() && tx.actions.is_empty());
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
        let filtered = filter_block_to_pools(block, &[PoolType::Sapling as i32]);
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
        let filtered = filter_block_to_pools(block, &[PoolType::Sapling as i32]);
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
        block.chain_metadata = Some(ChainMetadata {
            sapling_commitment_tree_size: 99,
            orchard_commitment_tree_size: 99,
        });

        let block = nullifiers_only(block);
        let tx = &block.vtx[0];

        // Kept: Sapling spend nullifiers and the Orchard action nullifier.
        assert!(!tx.spends.is_empty());
        assert_eq!(tx.actions[0].nullifier, vec![1; 32]);
        // Dropped: outputs, transparent data, the rest of the action, and the tree sizes.
        assert!(tx.outputs.is_empty() && tx.vin.is_empty() && tx.vout.is_empty());
        assert!(tx.actions[0].cmx.is_empty() && tx.actions[0].ciphertext.is_empty());
        let metadata = block.chain_metadata.unwrap();
        assert_eq!(metadata.sapling_commitment_tree_size, 0);
        assert_eq!(metadata.orchard_commitment_tree_size, 0);
    }
}
