//! The mutable, in-memory chain state the mock node serves and the control service edits.

use std::collections::HashMap;
use std::sync::Arc;

use crate::compact;
use crate::encoding;
use crate::node::{
    AddressUtxo, Consensus, GetBlockVerbose, GetBlockchainInfo, GetRawTransaction, GetTreeState,
    TreeCommitments, TreePool, TreeSize, Trees, Upgrade,
};
use crate::proto::{
    DarksideMetaState, DarksideSubtreeRoots, GetAddressUtxosReply, SubtreeRoot, TreeState,
};

use super::block::{ActiveBlock, raw_block_height, set_prevhash, synthetic_block};
use super::error::DarksideError;

/// Shared handle to the mock chain state, held by the node, the control service, and the streamer.
pub type DarksideHandle = Arc<tokio::sync::Mutex<DarksideState>>;

/// Subtree roots staged for one shielded protocol via `SetSubtreeRoots`.
struct StagedSubtreeRoots {
    start_index: u32,
    roots: Vec<SubtreeRoot>,
}

/// The mutable, in-memory chain state the mock node serves and the control service edits.
pub struct DarksideState {
    was_reset: bool,
    /// Height of `active[0]` (set by `Reset` to the Sapling activation height).
    start_height: u64,
    /// Sapling commitment tree size as of `start_height - 1`.
    start_sapling_size: u32,
    /// Orchard commitment tree size as of `start_height - 1`.
    start_orchard_size: u32,
    branch_id: String,
    chain_name: String,
    /// Highest height presented by the mock node; `-1` before the first `ApplyStaged`.
    pub(super) latest_height: i64,
    pub(super) active: Vec<ActiveBlock>,
    staged_blocks: Vec<Vec<u8>>,
    /// Staged `(height, raw_tx)` pairs waiting to be mined into their block by `ApplyStaged`.
    staged_txs: Vec<(u64, Vec<u8>)>,
    /// Transactions received via the production `SendTransaction`, conceptually the mempool.
    pub(super) incoming_txs: Vec<Vec<u8>>,
    utxos: Vec<GetAddressUtxosReply>,
    /// `(address, raw_tx, height)` entries returned by `GetTaddressTransactions`.
    addr_txs: Vec<(String, Vec<u8>, u64)>,
    treestates: Vec<TreeState>,
    /// Subtree roots keyed by shielded protocol (`sapling = 0`, `orchard = 1`).
    subtree_roots: HashMap<i32, StagedSubtreeRoots>,
}

impl Default for DarksideState {
    fn default() -> Self {
        Self::new()
    }
}

// --- Control-plane mutations ------------------------------------------------------------------

impl DarksideState {
    /// A fresh, un-reset state. Most operations require a `Reset` first.
    pub fn new() -> Self {
        Self {
            was_reset: false,
            start_height: 0,
            start_sapling_size: 0,
            start_orchard_size: 0,
            branch_id: String::new(),
            chain_name: "main".to_string(),
            latest_height: -1,
            active: Vec::new(),
            staged_blocks: Vec::new(),
            staged_txs: Vec::new(),
            incoming_txs: Vec::new(),
            utxos: Vec::new(),
            addr_txs: Vec::new(),
            treestates: Vec::new(),
            subtree_roots: HashMap::new(),
        }
    }

    /// Revert all state to empty and seed the chain parameters from `Reset`.
    pub(super) fn reset(&mut self, meta: &DarksideMetaState) {
        *self = Self {
            was_reset: true,
            start_height: meta.sapling_activation.max(0) as u64,
            start_sapling_size: meta.start_sapling_commitment_tree_size,
            start_orchard_size: meta.start_orchard_commitment_tree_size,
            branch_id: meta.branch_id.clone(),
            chain_name: meta.chain_name.clone(),
            latest_height: -1,
            active: Vec::new(),
            staged_blocks: Vec::new(),
            staged_txs: Vec::new(),
            incoming_txs: Vec::new(),
            utxos: Vec::new(),
            addr_txs: Vec::new(),
            treestates: Vec::new(),
            subtree_roots: HashMap::new(),
        };
    }

    /// Stage one raw block, after checking it parses and is not below Sapling activation.
    pub(super) fn stage_block(&mut self, raw: Vec<u8>) -> Result<(), DarksideError> {
        if !self.was_reset {
            return Err(DarksideError::NotReset);
        }
        let height = raw_block_height(&raw)?;
        if height < self.start_height {
            return Err(DarksideError::Invalid(format!(
                "block height {height} is less than sapling activation height {}",
                self.start_height
            )));
        }
        self.staged_blocks.push(raw);
        Ok(())
    }

    /// Stage `count` synthetic empty blocks at consecutive heights starting at `height`. The `nonce`
    /// varies the block hash so identical-height blocks can differ.
    pub(super) fn stage_blocks_create(
        &mut self,
        height: i32,
        nonce: i32,
        count: i32,
    ) -> Result<(), DarksideError> {
        if !self.was_reset {
            return Err(DarksideError::NotReset);
        }
        for height in (height..).take(count.max(0) as usize) {
            let raw = synthetic_block(height, nonce)?;
            self.stage_block(raw)?;
        }
        Ok(())
    }

    /// Stage one raw transaction to be mined into the block at `height` by the next `ApplyStaged`.
    pub(super) fn stage_transaction(
        &mut self,
        height: u64,
        raw: Vec<u8>,
    ) -> Result<(), DarksideError> {
        if !self.was_reset {
            return Err(DarksideError::NotReset);
        }
        // Validate that it parses before staging.
        compact::shielded_counts(&raw)?;
        self.staged_txs.push((height, raw));
        Ok(())
    }

    /// Merge staged blocks into the active chain (rewriting from the staged height, so this is how a
    /// reorg happens), mine staged transactions into their blocks, re-chain prev hashes, and set the
    /// presented tip to `target_height` (clamped to the active range).
    pub(super) fn apply_staged(&mut self, target_height: i64) -> Result<(), DarksideError> {
        if !self.was_reset {
            return Err(DarksideError::NotReset);
        }
        if target_height < self.start_height as i64 {
            return Err(DarksideError::Invalid(format!(
                "height {target_height} is less than sapling activation height {}",
                self.start_height
            )));
        }

        self.apply_staged_blocks()?;
        self.mine_staged_transactions()?;

        set_prevhash(&mut self.active);
        let max_height = self.start_height as i64 + self.active.len() as i64 - 1;
        self.latest_height = target_height.min(max_height);
        Ok(())
    }

    /// Merge every staged block into the active chain, then assert the chain is non-empty.
    fn apply_staged_blocks(&mut self) -> Result<(), DarksideError> {
        let staged_blocks = std::mem::take(&mut self.staged_blocks);
        for raw in staged_blocks {
            self.add_block_active(&raw)?;
        }
        if self.active.is_empty() {
            return Err(DarksideError::Invalid(
                "no active blocks after applying staged blocks".to_string(),
            ));
        }
        Ok(())
    }

    /// Mine each staged transaction into its block by height, growing that block's note-commitment
    /// tree sizes and every later block's.
    fn mine_staged_transactions(&mut self) -> Result<(), DarksideError> {
        let staged_txs = std::mem::take(&mut self.staged_txs);
        for (tx_height, raw) in staged_txs {
            if tx_height < self.start_height {
                return Err(DarksideError::Invalid(
                    "transaction height too low".to_string(),
                ));
            }
            let index = (tx_height - self.start_height) as usize;
            if index >= self.active.len() {
                return Err(DarksideError::Invalid(
                    "transaction height too high".to_string(),
                ));
            }
            let (sapling_outputs, orchard_actions) = compact::shielded_counts(&raw)?;
            {
                let block = &mut self.active[index];
                block.txs.push(raw);
                // Perturb HashFinalSaplingRoot so the mined block's hash changes.
                block.header[68] = block.header[68].wrapping_add(1);
            }
            // The mined notes grow this block's tree sizes and every later block's too.
            for block in &mut self.active[index..] {
                block.sapling_size += sapling_outputs;
                block.orchard_size += orchard_actions;
            }
        }
        Ok(())
    }

    /// Parse a raw block, validate it connects without a gap, compute its accumulated tree sizes, and
    /// append it (dropping the overwritten block and its children first).
    fn add_block_active(&mut self, raw: &[u8]) -> Result<(), DarksideError> {
        let (header, txs) = compact::split_block(raw)?;
        let coinbase = txs
            .first()
            .ok_or_else(|| DarksideError::Invalid("block has no transactions".to_string()))?;
        let height = compact::coinbase_height_from_raw(coinbase)?;
        if height > self.start_height + self.active.len() as u64 {
            return Err(DarksideError::Invalid(format!(
                "adding block at height {height} would create a gap in the blockchain"
            )));
        }
        if height < self.start_height {
            return Err(DarksideError::Invalid(format!(
                "adding block at height {height} is lower than sapling activation height {}",
                self.start_height
            )));
        }
        let offset = (height - self.start_height) as usize;
        let (mut sapling_size, mut orchard_size) = if offset > 0 {
            let prev = &self.active[offset - 1];
            (prev.sapling_size, prev.orchard_size)
        } else {
            (self.start_sapling_size, self.start_orchard_size)
        };
        for tx in &txs {
            let (sapling_outputs, orchard_actions) = compact::shielded_counts(tx)?;
            sapling_size += sapling_outputs;
            orchard_size += orchard_actions;
        }
        self.active.truncate(offset);
        self.active.push(ActiveBlock {
            header,
            txs,
            sapling_size,
            orchard_size,
        });
        Ok(())
    }

    /// Record a transaction received via `SendTransaction` and return its display-order txid.
    pub(super) fn push_incoming(&mut self, raw: Vec<u8>) -> Result<String, DarksideError> {
        let txid = compact::txid_display(&raw)?;
        self.incoming_txs.push(raw);
        Ok(txid)
    }

    pub(super) fn clear_incoming(&mut self) {
        self.incoming_txs.clear();
    }

    pub(super) fn add_utxo(&mut self, reply: GetAddressUtxosReply) {
        self.utxos.push(reply);
    }

    pub(super) fn clear_utxos(&mut self) {
        self.utxos.clear();
    }

    pub(super) fn add_addr_tx(&mut self, address: String, raw: Vec<u8>, height: u64) {
        self.addr_txs.push((address, raw, height));
    }

    pub(super) fn clear_addr_txs(&mut self) {
        self.addr_txs.clear();
    }

    pub(super) fn add_treestate(&mut self, tree_state: TreeState) -> Result<(), DarksideError> {
        if !self.was_reset {
            return Err(DarksideError::NotReset);
        }
        self.treestates
            .retain(|existing| existing.height != tree_state.height);
        self.treestates.push(tree_state);
        Ok(())
    }

    pub(super) fn remove_treestate(&mut self, height: u64, wire_hash: &[u8]) {
        if height > 0 {
            self.treestates
                .retain(|tree_state| tree_state.height != height);
        } else {
            let forward = hex::encode(wire_hash);
            let reversed = encoding::wire_to_display_hex(wire_hash);
            self.treestates
                .retain(|tree_state| tree_state.hash != forward && tree_state.hash != reversed);
        }
    }

    pub(super) fn clear_treestates(&mut self) {
        self.treestates.clear();
    }

    pub(super) fn set_subtree_roots(&mut self, arg: DarksideSubtreeRoots) {
        self.subtree_roots.insert(
            arg.shielded_protocol,
            StagedSubtreeRoots {
                start_index: arg.start_index,
                roots: arg.subtree_roots,
            },
        );
    }
}

// --- Node-plane reads -------------------------------------------------------------------------

impl DarksideState {
    /// Index into `active` of the presented tip, or `None` if nothing has been applied.
    fn tip_index(&self) -> Option<usize> {
        if self.latest_height < self.start_height as i64 {
            return None;
        }
        let index = (self.latest_height - self.start_height as i64) as usize;
        (index < self.active.len()).then_some(index)
    }

    /// The block at `height`, with the same not-found checks the mock zcashd applies.
    fn block_at(&self, height: u64) -> Result<&ActiveBlock, DarksideError> {
        if self.active.is_empty() || self.latest_height < 0 || height > self.latest_height as u64 {
            return Err(DarksideError::Invalid(format!(
                "-8: block {height} not found"
            )));
        }
        if height < self.start_height {
            return Err(DarksideError::Invalid(format!(
                "getblock: requesting height {height} is less than sapling activation height"
            )));
        }
        let index = (height - self.start_height) as usize;
        self.active
            .get(index)
            .ok_or_else(|| DarksideError::Invalid(format!("-8: block {height} not found")))
    }

    pub(super) fn block_count(&self) -> u64 {
        self.latest_height.max(0) as u64
    }

    pub(super) fn blockchain_info(&self) -> Result<GetBlockchainInfo, DarksideError> {
        let index = self.tip_index().ok_or_else(|| {
            DarksideError::Invalid(
                "getblockchaininfo requires at least one applied block".to_string(),
            )
        })?;
        let blocks = self.latest_height as u64;
        Ok(GetBlockchainInfo {
            chain: self.chain_name.clone(),
            blocks,
            bestblockhash: self.active[index].display_hash(),
            estimatedheight: blocks,
            consensus: Consensus {
                chaintip: self.branch_id.clone(),
            },
            upgrades: HashMap::from([(
                "76b809bb".to_string(),
                Upgrade {
                    name: "Sapling".to_string(),
                    activationheight: self.start_height,
                },
            )]),
        })
    }

    pub(super) fn block_verbose(&self, height: u64) -> Result<GetBlockVerbose, DarksideError> {
        let block = self.block_at(height)?;
        Ok(GetBlockVerbose {
            hash: block.display_hash(),
            trees: Trees {
                sapling: TreeSize {
                    size: block.sapling_size,
                },
                orchard: TreeSize {
                    size: block.orchard_size,
                },
            },
        })
    }

    pub(super) fn block_raw_by_hash(&self, display_hash: &str) -> Result<Vec<u8>, DarksideError> {
        self.active
            .iter()
            .find(|block| block.display_hash() == display_hash)
            .map(ActiveBlock::to_raw)
            .ok_or_else(|| {
                DarksideError::Invalid(format!("getblock: hash {display_hash} not found"))
            })
    }

    /// Display-order txids of every transaction in the staging area: all transactions of every
    /// staged block, then every staged transaction. This is the mock mempool.
    pub(super) fn raw_mempool(&self) -> Result<Vec<String>, DarksideError> {
        let mut txids = Vec::new();
        for raw in &self.staged_blocks {
            let (_, txs) = compact::split_block(raw)?;
            for tx in &txs {
                txids.push(compact::txid_display(tx)?);
            }
        }
        for (_, raw) in &self.staged_txs {
            txids.push(compact::txid_display(raw)?);
        }
        Ok(txids)
    }

    pub(super) fn raw_transaction(
        &self,
        display_txid: &str,
    ) -> Result<GetRawTransaction, DarksideError> {
        for (offset, block) in self.active.iter().enumerate() {
            for tx in &block.txs {
                if compact::txid_display(tx)? == display_txid {
                    return Ok(GetRawTransaction {
                        hex: hex::encode(tx),
                        height: (self.start_height + offset as u64) as i64,
                    });
                }
            }
        }
        for raw in &self.staged_blocks {
            let (_, txs) = compact::split_block(raw)?;
            let height = raw_block_height(raw)? as i64;
            for tx in &txs {
                if compact::txid_display(tx)? == display_txid {
                    return Ok(GetRawTransaction {
                        hex: hex::encode(tx),
                        height,
                    });
                }
            }
        }
        for (_, raw) in &self.staged_txs {
            if compact::txid_display(raw)? == display_txid {
                return Ok(GetRawTransaction {
                    hex: hex::encode(raw),
                    height: 0,
                });
            }
        }
        for (_, raw, height) in &self.addr_txs {
            if compact::txid_display(raw)? == display_txid {
                return Ok(GetRawTransaction {
                    hex: hex::encode(raw),
                    height: *height as i64,
                });
            }
        }
        Err(DarksideError::Invalid(
            "-5: No information available about transaction".to_string(),
        ))
    }

    pub(super) fn address_txids(
        &self,
        addresses: &[String],
        start: u64,
        end: u64,
    ) -> Result<Vec<String>, DarksideError> {
        let mut txids = Vec::new();
        for (address, raw, height) in &self.addr_txs {
            if !addresses.contains(address) || *height < start || (end > 0 && *height > end) {
                continue;
            }
            txids.push(compact::txid_display(raw)?);
        }
        Ok(txids)
    }

    pub(super) fn address_utxos(&self, addresses: &[String]) -> Vec<AddressUtxo> {
        self.utxos
            .iter()
            .filter(|utxo| addresses.contains(&utxo.address))
            .map(|reply| AddressUtxo {
                address: reply.address.clone(),
                txid: encoding::wire_to_display_hex(&reply.txid),
                output_index: reply.index as i64,
                script: hex::encode(&reply.script),
                satoshis: reply.value_zat as u64,
                height: reply.height,
            })
            .collect()
    }

    pub(super) fn treestate(&self, id: &str) -> Result<GetTreeState, DarksideError> {
        let found = if id.len() < 64 {
            let height: u64 = id.parse().map_err(|_| {
                DarksideError::Invalid("error parsing height as integer".to_string())
            })?;
            self.treestates.iter().find(|state| state.height == height)
        } else {
            self.treestates.iter().find(|state| state.hash == id)
        };
        let tree_state = found.ok_or_else(|| {
            DarksideError::Invalid(
                "no TreeState for the given height or block hash; stage it with AddTreeState first"
                    .to_string(),
            )
        })?;
        Ok(GetTreeState {
            hash: tree_state.hash.clone(),
            height: tree_state.height,
            time: tree_state.time,
            sapling: TreePool {
                commitments: TreeCommitments {
                    final_state: tree_state.sapling_tree.clone(),
                },
            },
            orchard: TreePool {
                commitments: TreeCommitments {
                    final_state: tree_state.orchard_tree.clone(),
                },
            },
        })
    }

    /// The staged subtree roots for `protocol`, applying the `start_index` offset and `max_entries`
    /// limit (`0` means all). Returned verbatim, since darkside sets complete roots.
    pub fn subtree_roots_for(
        &self,
        protocol: i32,
        start_index: u32,
        max_entries: u32,
    ) -> Vec<SubtreeRoot> {
        let Some(staged) = self.subtree_roots.get(&protocol) else {
            return Vec::new();
        };
        if start_index < staged.start_index {
            return Vec::new();
        }
        let offset = (start_index - staged.start_index) as usize;
        if offset >= staged.roots.len() {
            return Vec::new();
        }
        let available = staged.roots.len() - offset;
        let limit = if max_entries > 0 {
            (max_entries as usize).min(available)
        } else {
            available
        };
        staged.roots[offset..offset + limit].to_vec()
    }
}
