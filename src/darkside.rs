//! Darkside mode: a controllable, in-memory mock chain for deterministic wallet tests.
//!
//! Instead of talking to a real node, lightwalletd-rs serves block data from a [`DarksideState`] that
//! a test fabricates over gRPC. [`DarksideNode`] implements the [`NodeRpc`] seam over that state, so the
//! cache, ingestor, and `CompactTxStreamer` service are reused unchanged; [`DarksideService`] is the
//! `DarksideStreamer` control plane that mutates the same state (stage blocks/transactions, apply them,
//! trigger reorgs, capture sent transactions).

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use sha2::{Digest, Sha256};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::compact::{self, ParseError};
use crate::encoding;
use crate::node::{
    AddressUtxo, Consensus, GetAddressBalance, GetBlockVerbose, GetBlockchainInfo, GetInfo,
    GetRawTransaction, GetSubtrees, GetTreeState, NodeError, NodeRpc, TreeCommitments, TreePool,
    TreeSize, Trees, Upgrade,
};
use crate::proto::darkside_streamer_server::DarksideStreamer;
use crate::proto::{
    BlockId, DarksideAddressTransaction, DarksideBlock, DarksideBlocksUrl, DarksideEmptyBlocks,
    DarksideHeight, DarksideMetaState, DarksideSubtreeRoots, DarksideTransactionsUrl, Empty,
    GetAddressUtxosReply, RawTransaction, SubtreeRoot, TreeState,
};

/// Shared handle to the mock chain state, held by the node, the control service, and the streamer.
pub type DarksideHandle = Arc<tokio::sync::Mutex<DarksideState>>;

/// Boxed server-streaming response, shared by every streaming method's associated type.
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// Errors from mutating or reading the darkside state.
#[derive(Debug, thiserror::Error)]
pub enum DarksideError {
    /// An operation ran before `Reset` initialized the state.
    #[error("please call Reset first")]
    NotReset,
    /// A staging/apply invariant was violated (gap, bad height, missing entry, ...).
    #[error("{0}")]
    Invalid(String),
    /// A block or transaction failed to parse.
    #[error(transparent)]
    Parse(#[from] ParseError),
    /// A hex-encoded input could not be decoded.
    #[error("decoding hex: {0}")]
    Hex(#[from] hex::FromHexError),
}

impl From<DarksideError> for NodeError {
    fn from(error: DarksideError) -> Self {
        NodeError::Rpc {
            code: -1,
            message: error.to_string(),
        }
    }
}

impl From<DarksideError> for Status {
    fn from(error: DarksideError) -> Self {
        match error {
            DarksideError::NotReset => Status::failed_precondition(error.to_string()),
            other => Status::invalid_argument(other.to_string()),
        }
    }
}

/// One block presented by the mock chain, held split so it can be re-serialized after mutation.
struct ActiveBlock {
    header: Vec<u8>,
    txs: Vec<Vec<u8>>,
    sapling_size: u32,
    orchard_size: u32,
}

impl ActiveBlock {
    /// Re-serialize to the raw block format `header + CompactSize(tx_count) + txs`.
    fn to_raw(&self) -> Vec<u8> {
        let mut raw = self.header.clone();
        compact::write_compact_size(&mut raw, self.txs.len() as u64);
        for tx in &self.txs {
            raw.extend_from_slice(tx);
        }
        raw
    }

    /// The on-wire (protocol order) block hash.
    fn hash(&self) -> [u8; 32] {
        sha256d(&self.header)
    }

    /// The display-order (big-endian hex) block hash, as zebrad reports it.
    fn display_hash(&self) -> String {
        let mut bytes = self.hash().to_vec();
        bytes.reverse();
        hex::encode(bytes)
    }
}

/// Subtree roots staged for one shielded protocol via `SetSubtreeRoots`.
struct StagedSubtreeRoots {
    start_index: u32,
    roots: Vec<SubtreeRoot>,
}

/// The mutable, in-memory chain state the mock node serves and the control service edits.
pub struct DarksideState {
    resetted: bool,
    /// Height of `active[0]` (set by `Reset` to the Sapling activation height).
    start_height: u64,
    /// Sapling commitment tree size as of `start_height - 1`.
    start_sapling_size: u32,
    /// Orchard commitment tree size as of `start_height - 1`.
    start_orchard_size: u32,
    branch_id: String,
    chain_name: String,
    /// Highest height presented by the mock node; `-1` before the first `ApplyStaged`.
    latest_height: i64,
    active: Vec<ActiveBlock>,
    staged_blocks: Vec<Vec<u8>>,
    /// Staged `(height, raw_tx)` pairs waiting to be mined into their block by `ApplyStaged`.
    staged_txs: Vec<(u64, Vec<u8>)>,
    /// Transactions received via the production `SendTransaction`, conceptually the mempool.
    incoming_txs: Vec<Vec<u8>>,
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

impl DarksideState {
    /// A fresh, un-reset state. Most operations require a `Reset` first.
    pub fn new() -> Self {
        Self {
            resetted: false,
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

    // --- Control-plane mutations ------------------------------------------------------------

    /// Revert all state to empty and seed the chain parameters from `Reset`.
    fn reset(&mut self, meta: &DarksideMetaState) {
        *self = Self {
            resetted: true,
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
    fn stage_block(&mut self, raw: Vec<u8>) -> Result<(), DarksideError> {
        if !self.resetted {
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
    fn stage_blocks_create(
        &mut self,
        height: i32,
        nonce: i32,
        count: i32,
    ) -> Result<(), DarksideError> {
        if !self.resetted {
            return Err(DarksideError::NotReset);
        }
        let mut height = height;
        for _ in 0..count.max(0) {
            let raw = synthetic_block(height, nonce)?;
            self.stage_block(raw)?;
            height += 1;
        }
        Ok(())
    }

    /// Stage one raw transaction to be mined into the block at `height` by the next `ApplyStaged`.
    fn stage_transaction(&mut self, height: u64, raw: Vec<u8>) -> Result<(), DarksideError> {
        if !self.resetted {
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
    fn apply_staged(&mut self, target_height: i64) -> Result<(), DarksideError> {
        if !self.resetted {
            return Err(DarksideError::NotReset);
        }
        if target_height < self.start_height as i64 {
            return Err(DarksideError::Invalid(format!(
                "height {target_height} is less than sapling activation height {}",
                self.start_height
            )));
        }

        let staged_blocks = std::mem::take(&mut self.staged_blocks);
        for raw in staged_blocks {
            self.add_block_active(&raw)?;
        }
        if self.active.is_empty() {
            return Err(DarksideError::Invalid(
                "no active blocks after applying staged blocks".to_string(),
            ));
        }

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

        set_prevhash(&mut self.active);
        let max_height = self.start_height as i64 + self.active.len() as i64 - 1;
        self.latest_height = target_height.min(max_height);
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
    fn push_incoming(&mut self, raw: Vec<u8>) -> Result<String, DarksideError> {
        let txid = compact::txid_display(&raw)?;
        self.incoming_txs.push(raw);
        Ok(txid)
    }

    fn clear_incoming(&mut self) {
        self.incoming_txs.clear();
    }

    fn add_utxo(&mut self, reply: GetAddressUtxosReply) {
        self.utxos.push(reply);
    }

    fn clear_utxos(&mut self) {
        self.utxos.clear();
    }

    fn add_addr_tx(&mut self, address: String, raw: Vec<u8>, height: u64) {
        self.addr_txs.push((address, raw, height));
    }

    fn clear_addr_txs(&mut self) {
        self.addr_txs.clear();
    }

    fn add_treestate(&mut self, tree_state: TreeState) -> Result<(), DarksideError> {
        if !self.resetted {
            return Err(DarksideError::NotReset);
        }
        self.treestates
            .retain(|existing| existing.height != tree_state.height);
        self.treestates.push(tree_state);
        Ok(())
    }

    fn remove_treestate(&mut self, height: u64, wire_hash: &[u8]) {
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

    fn clear_treestates(&mut self) {
        self.treestates.clear();
    }

    fn set_subtree_roots(&mut self, arg: DarksideSubtreeRoots) {
        self.subtree_roots.insert(
            arg.shielded_protocol,
            StagedSubtreeRoots {
                start_index: arg.start_index,
                roots: arg.subtree_roots,
            },
        );
    }

    // --- Node-plane reads -------------------------------------------------------------------

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

    fn block_count(&self) -> u64 {
        self.latest_height.max(0) as u64
    }

    fn blockchain_info(&self) -> Result<GetBlockchainInfo, DarksideError> {
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

    fn block_verbose(&self, height: u64) -> Result<GetBlockVerbose, DarksideError> {
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

    fn block_raw_by_hash(&self, display_hash: &str) -> Result<Vec<u8>, DarksideError> {
        self.active
            .iter()
            .find(|block| block.display_hash() == display_hash)
            .map(ActiveBlock::to_raw)
            .ok_or_else(|| {
                DarksideError::Invalid(format!("getblock: hash {display_hash} not found"))
            })
    }

    fn raw_transaction(&self, display_txid: &str) -> Result<GetRawTransaction, DarksideError> {
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

    fn address_txids(
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

    fn address_utxos(&self, addresses: &[String]) -> Vec<AddressUtxo> {
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

    fn treestate(&self, id: &str) -> Result<GetTreeState, DarksideError> {
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

/// Double SHA-256 in protocol (little-endian) byte order.
fn sha256d(data: &[u8]) -> [u8; 32] {
    Sha256::digest(Sha256::digest(data)).into()
}

/// Rewrite each active block's `prevHash` (header bytes `4..36`) so the chain links together; the
/// first block's prev hash is left as staged.
fn set_prevhash(active: &mut [ActiveBlock]) {
    let mut prev_hash: Option<[u8; 32]> = None;
    for block in active.iter_mut() {
        if let Some(hash) = prev_hash {
            block.header[4..36].copy_from_slice(&hash);
        }
        prev_hash = Some(sha256d(&block.header));
    }
}

/// Read the BIP34 height from a raw block's coinbase.
fn raw_block_height(raw: &[u8]) -> Result<u64, DarksideError> {
    let (_, txs) = compact::split_block(raw)?;
    let coinbase = txs
        .first()
        .ok_or_else(|| DarksideError::Invalid("block has no transactions".to_string()))?;
    Ok(compact::coinbase_height_from_raw(coinbase)?)
}

/// Build a synthetic empty block (a single fake coinbase carrying `height`) for `StageBlocksCreate`.
fn synthetic_block(height: i32, nonce: i32) -> Result<Vec<u8>, DarksideError> {
    // A real coinbase from block 797905 (little-endian height 0xD12C0C00); the height bytes are
    // patched to the requested height below.
    const FAKE_COINBASE: &str = concat!(
        "0400008085202f890100000000000000000000000000000000000000000000000000",
        "00000000000000ffffffff2a03d12c0c00043855975e464b8896790758f824ceac97836",
        "22c17ed38f1669b8a45ce1da857dbbe7950e2ffffffff02a0ebce1d000000001976a914",
        "7ed15946ec14ae0cd8fa8991eb6084452eb3f77c88ac405973070000000017a914e445cf",
        "a944b6f2bdacefbda904a81d5fdd26d77f8700000000000000000000000000000000000000",
    );

    let merkle_root = Sha256::digest(format!("{nonce}#{height}").as_bytes());
    let mut header = Vec::with_capacity(1487);
    header.extend_from_slice(&4u32.to_le_bytes()); // version
    header.extend_from_slice(&[0u8; 32]); // prevHash
    header.extend_from_slice(&merkle_root); // merkleRoot
    header.extend_from_slice(&[0u8; 32]); // hashFinalSaplingRoot
    header.extend_from_slice(&1u32.to_le_bytes()); // time
    header.extend_from_slice(&[0u8; 4]); // nBits
    header.extend_from_slice(&[0u8; 32]); // nonce
    compact::write_compact_size(&mut header, 1344); // solution length
    header.extend_from_slice(&[0u8; 1344]); // solution

    let height_le = hex::encode((height as u32).to_le_bytes());
    let coinbase = hex::decode(FAKE_COINBASE.replace("d12c0c00", &height_le))?;

    let mut block = header;
    block.push(1); // transaction count
    block.extend_from_slice(&coinbase);
    Ok(block)
}

/// A [`NodeRpc`] implementation backed by the mock chain state, injected in place of `NodeClient`.
pub struct DarksideNode {
    state: DarksideHandle,
}

impl DarksideNode {
    /// Build a mock node sharing `state` with the control service.
    pub fn new(state: DarksideHandle) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl NodeRpc for DarksideNode {
    async fn get_info(&self) -> Result<GetInfo, NodeError> {
        Ok(GetInfo {
            build: "lightwalletd-rs-darkside".to_string(),
            subversion: "lightwalletd-rs-darkside".to_string(),
        })
    }

    async fn get_blockchain_info(&self) -> Result<GetBlockchainInfo, NodeError> {
        Ok(self.state.lock().await.blockchain_info()?)
    }

    async fn get_block_verbose(&self, height: u64) -> Result<GetBlockVerbose, NodeError> {
        Ok(self.state.lock().await.block_verbose(height)?)
    }

    async fn get_block_count(&self) -> Result<u64, NodeError> {
        Ok(self.state.lock().await.block_count())
    }

    async fn get_block_raw(&self, hash: &str) -> Result<Vec<u8>, NodeError> {
        Ok(self.state.lock().await.block_raw_by_hash(hash)?)
    }

    async fn get_raw_transaction(&self, txid: &str) -> Result<GetRawTransaction, NodeError> {
        Ok(self.state.lock().await.raw_transaction(txid)?)
    }

    async fn send_raw_transaction(&self, hex: &str) -> Result<String, NodeError> {
        let raw = hex::decode(hex)?;
        Ok(self.state.lock().await.push_incoming(raw)?)
    }

    async fn get_treestate(&self, id: &str) -> Result<GetTreeState, NodeError> {
        Ok(self.state.lock().await.treestate(id)?)
    }

    async fn get_address_balance(
        &self,
        _addresses: &[String],
    ) -> Result<GetAddressBalance, NodeError> {
        // Darkside has no balance staging; a zero balance keeps callers from failing.
        Ok(GetAddressBalance { balance: 0 })
    }

    async fn get_address_utxos(&self, addresses: &[String]) -> Result<Vec<AddressUtxo>, NodeError> {
        Ok(self.state.lock().await.address_utxos(addresses))
    }

    async fn get_address_txids(
        &self,
        addresses: &[String],
        start: u64,
        end: u64,
    ) -> Result<Vec<String>, NodeError> {
        Ok(self
            .state
            .lock()
            .await
            .address_txids(addresses, start, end)?)
    }

    async fn get_subtrees(
        &self,
        _protocol: &str,
        _start_index: u32,
        _max_entries: u32,
    ) -> Result<GetSubtrees, NodeError> {
        // Subtree roots are served by the GetSubtreeRoots override, not from here.
        Ok(GetSubtrees {
            subtrees: Vec::new(),
        })
    }

    async fn get_raw_mempool(&self) -> Result<Vec<String>, NodeError> {
        Ok(Vec::new())
    }
}

/// The `DarksideStreamer` control-plane service. Shares state with [`DarksideNode`] and holds the
/// shutdown notifier for `Stop`.
pub struct DarksideService {
    state: DarksideHandle,
    shutdown: Arc<tokio::sync::Notify>,
}

impl DarksideService {
    /// Build the control service over the shared `state` and a shutdown notifier.
    pub fn new(state: DarksideHandle, shutdown: Arc<tokio::sync::Notify>) -> Self {
        Self { state, shutdown }
    }
}

/// Fetch a URL's body as text (used by the URL-based staging RPCs).
async fn fetch_lines(url: &str) -> Result<String, Status> {
    reqwest::get(url)
        .await
        .map_err(|error| Status::unavailable(format!("fetch failed: {error}")))?
        .text()
        .await
        .map_err(|error| Status::unavailable(format!("reading body failed: {error}")))
}

#[tonic::async_trait]
impl DarksideStreamer for DarksideService {
    async fn reset(&self, request: Request<DarksideMetaState>) -> Result<Response<Empty>, Status> {
        let meta = request.into_inner();
        if meta.branch_id.is_empty() || !meta.branch_id.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(Status::invalid_argument(format!(
                "Reset: invalid BranchID (must be hex): {}",
                meta.branch_id
            )));
        }
        if meta.chain_name.is_empty() || !meta.chain_name.chars().all(|c| c.is_ascii_alphanumeric())
        {
            return Err(Status::invalid_argument("invalid chain name"));
        }
        self.state.lock().await.reset(&meta);
        Ok(Response::new(Empty {}))
    }

    async fn stage_blocks_stream(
        &self,
        request: Request<tonic::Streaming<DarksideBlock>>,
    ) -> Result<Response<Empty>, Status> {
        let mut stream = request.into_inner();
        while let Some(block) = stream.message().await? {
            let raw = hex::decode(&block.block)
                .map_err(|error| Status::invalid_argument(format!("bad block hex: {error}")))?;
            self.state.lock().await.stage_block(raw)?;
        }
        Ok(Response::new(Empty {}))
    }

    async fn stage_blocks(
        &self,
        request: Request<DarksideBlocksUrl>,
    ) -> Result<Response<Empty>, Status> {
        let body = fetch_lines(&request.into_inner().url).await?;
        let mut state = self.state.lock().await;
        for line in body.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if line == "404: Not Found" {
                return Err(Status::not_found(line.to_string()));
            }
            let raw = hex::decode(line)
                .map_err(|error| Status::invalid_argument(format!("bad block hex: {error}")))?;
            state.stage_block(raw)?;
        }
        Ok(Response::new(Empty {}))
    }

    async fn stage_blocks_create(
        &self,
        request: Request<DarksideEmptyBlocks>,
    ) -> Result<Response<Empty>, Status> {
        let arg = request.into_inner();
        self.state
            .lock()
            .await
            .stage_blocks_create(arg.height, arg.nonce, arg.count)?;
        Ok(Response::new(Empty {}))
    }

    async fn stage_transactions_stream(
        &self,
        request: Request<tonic::Streaming<RawTransaction>>,
    ) -> Result<Response<Empty>, Status> {
        let mut stream = request.into_inner();
        while let Some(tx) = stream.message().await? {
            self.state
                .lock()
                .await
                .stage_transaction(tx.height, tx.data)?;
        }
        Ok(Response::new(Empty {}))
    }

    async fn stage_transactions(
        &self,
        request: Request<DarksideTransactionsUrl>,
    ) -> Result<Response<Empty>, Status> {
        let arg = request.into_inner();
        let body = fetch_lines(&arg.url).await?;
        let height = arg.height.max(0) as u64;
        let mut state = self.state.lock().await;
        for line in body.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if line == "404: Not Found" {
                return Err(Status::not_found(line.to_string()));
            }
            let raw = hex::decode(line).map_err(|error| {
                Status::invalid_argument(format!("bad transaction hex: {error}"))
            })?;
            state.stage_transaction(height, raw)?;
        }
        Ok(Response::new(Empty {}))
    }

    async fn apply_staged(
        &self,
        request: Request<DarksideHeight>,
    ) -> Result<Response<Empty>, Status> {
        let height = request.into_inner().height as i64;
        self.state.lock().await.apply_staged(height)?;
        Ok(Response::new(Empty {}))
    }

    type GetIncomingTransactionsStream = BoxStream<RawTransaction>;
    async fn get_incoming_transactions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::GetIncomingTransactionsStream>, Status> {
        let incoming = self.state.lock().await.incoming_txs.clone();
        let replies: Vec<Result<RawTransaction, Status>> = incoming
            .into_iter()
            .map(|data| Ok(RawTransaction { data, height: 0 }))
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(replies))))
    }

    async fn clear_incoming_transactions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Empty>, Status> {
        self.state.lock().await.clear_incoming();
        Ok(Response::new(Empty {}))
    }

    async fn add_address_utxo(
        &self,
        request: Request<GetAddressUtxosReply>,
    ) -> Result<Response<Empty>, Status> {
        self.state.lock().await.add_utxo(request.into_inner());
        Ok(Response::new(Empty {}))
    }

    async fn clear_address_utxo(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Empty>, Status> {
        self.state.lock().await.clear_utxos();
        Ok(Response::new(Empty {}))
    }

    async fn add_address_transaction(
        &self,
        request: Request<DarksideAddressTransaction>,
    ) -> Result<Response<Empty>, Status> {
        let arg = request.into_inner();
        self.state
            .lock()
            .await
            .add_addr_tx(arg.address, arg.data, arg.height);
        Ok(Response::new(Empty {}))
    }

    async fn clear_address_transactions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Empty>, Status> {
        self.state.lock().await.clear_addr_txs();
        Ok(Response::new(Empty {}))
    }

    async fn add_tree_state(&self, request: Request<TreeState>) -> Result<Response<Empty>, Status> {
        self.state
            .lock()
            .await
            .add_treestate(request.into_inner())?;
        Ok(Response::new(Empty {}))
    }

    async fn remove_tree_state(
        &self,
        request: Request<BlockId>,
    ) -> Result<Response<Empty>, Status> {
        let block_id = request.into_inner();
        self.state
            .lock()
            .await
            .remove_treestate(block_id.height, &block_id.hash);
        Ok(Response::new(Empty {}))
    }

    async fn clear_all_tree_states(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Empty>, Status> {
        self.state.lock().await.clear_treestates();
        Ok(Response::new(Empty {}))
    }

    async fn set_subtree_roots(
        &self,
        request: Request<DarksideSubtreeRoots>,
    ) -> Result<Response<Empty>, Status> {
        self.state
            .lock()
            .await
            .set_subtree_roots(request.into_inner());
        Ok(Response::new(Empty {}))
    }

    async fn stop(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        tracing::info!("stop requested via gRPC");
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            // Let the reply reach the client before the server stops accepting connections.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            shutdown.notify_one();
        });
        Ok(Response::new(Empty {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sapling activation height of the consecutive blocks in `testdata/blocks` (380640..=380643).
    const START_HEIGHT: u64 = 380640;

    /// The four consecutive raw blocks in `testdata/blocks`.
    fn blocks() -> Vec<Vec<u8>> {
        std::fs::read_to_string("testdata/blocks")
            .unwrap()
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| hex::decode(line).unwrap())
            .collect()
    }

    fn meta(start_height: u64) -> DarksideMetaState {
        DarksideMetaState {
            sapling_activation: start_height as i32,
            branch_id: "2bb40e60".to_string(),
            chain_name: "main".to_string(),
            start_sapling_commitment_tree_size: 0,
            start_orchard_commitment_tree_size: 0,
        }
    }

    /// A v5 transaction (branch id patched to NU5) with known sapling outputs and orchard actions.
    fn shielded_tx() -> (Vec<u8>, u32, u32) {
        let json = std::fs::read_to_string("testdata/tx_v5.json").unwrap();
        let rows: Vec<Vec<serde_json::Value>> = serde_json::from_str(&json).unwrap();
        // Row with nOutputsSapling=2, nActionsOrchard=4.
        let row = &rows[5];
        let mut raw = hex::decode(row[0].as_str().unwrap()).unwrap();
        raw[8..12].copy_from_slice(&0xc2d6_d0b4u32.to_le_bytes());
        (
            raw,
            row[10].as_u64().unwrap() as u32,
            row[14].as_u64().unwrap() as u32,
        )
    }

    /// State with the first `n` blocks staged and applied, wrapped in a shared handle.
    async fn applied_handle(n: usize) -> DarksideHandle {
        let mut state = DarksideState::new();
        state.reset(&meta(START_HEIGHT));
        for raw in blocks().into_iter().take(n) {
            state.stage_block(raw).unwrap();
        }
        state
            .apply_staged(START_HEIGHT as i64 + n as i64 - 1)
            .unwrap();
        Arc::new(tokio::sync::Mutex::new(state))
    }

    // --- Phase 2: stage/apply engine -------------------------------------------------------

    #[test]
    fn apply_staged_chains_three_blocks() {
        let mut state = DarksideState::new();
        state.reset(&meta(START_HEIGHT));
        for raw in blocks().into_iter().take(3) {
            state.stage_block(raw).unwrap();
        }
        state.apply_staged(380642).unwrap();

        assert_eq!(state.active.len(), 3);
        assert_eq!(state.latest_height, 380642);
        for index in 1..state.active.len() {
            assert_eq!(
                state.active[index].header[4..36],
                state.active[index - 1].hash(),
                "block {index} prev hash should chain onto its predecessor"
            );
        }
    }

    #[test]
    fn apply_staged_reorg_rewrites_from_staged_height() {
        let mut state = DarksideState::new();
        state.reset(&meta(START_HEIGHT));
        for raw in blocks().into_iter().take(3) {
            state.stage_block(raw).unwrap();
        }
        state.apply_staged(380642).unwrap();
        let original_hash = state.active[1].hash();

        // A different block at height 380641 reorgs the chain from there: 380641 is replaced and
        // 380642 is dropped.
        state
            .stage_block(synthetic_block(380641, 99).unwrap())
            .unwrap();
        state.apply_staged(380641).unwrap();

        assert_eq!(state.active.len(), 2);
        assert_ne!(state.active[1].hash(), original_hash);
        assert_eq!(state.active[1].header[4..36], state.active[0].hash());
    }

    #[test]
    fn apply_staged_starts_tree_sizes_from_reset() {
        let mut state = DarksideState::new();
        let mut meta = meta(START_HEIGHT);
        meta.start_sapling_commitment_tree_size = 100;
        meta.start_orchard_commitment_tree_size = 200;
        state.reset(&meta);
        state.stage_block(blocks()[0].clone()).unwrap();
        state.apply_staged(380640).unwrap();

        // The first testdata block is pre-Sapling (no shielded outputs), so the sizes equal the
        // configured start sizes.
        assert_eq!(state.active[0].sapling_size, 100);
        assert_eq!(state.active[0].orchard_size, 200);
    }

    #[test]
    fn apply_staged_accumulates_mined_transaction_tree_sizes() {
        let (tx, sapling_outputs, orchard_actions) = shielded_tx();
        assert!(sapling_outputs > 0 && orchard_actions > 0);

        let mut state = DarksideState::new();
        state.reset(&meta(START_HEIGHT));
        for raw in blocks().into_iter().take(3) {
            state.stage_block(raw).unwrap();
        }
        // Mine the shielded transaction into the middle block (height 380641, index 1).
        state.stage_transaction(380641, tx.clone()).unwrap();
        state.apply_staged(380642).unwrap();

        // The block before the mined transaction is unchanged; the mined block and every later block
        // grow by the transaction's shielded note counts.
        assert_eq!(
            (state.active[0].sapling_size, state.active[0].orchard_size),
            (0, 0)
        );
        assert_eq!(
            (state.active[1].sapling_size, state.active[1].orchard_size),
            (sapling_outputs, orchard_actions)
        );
        assert_eq!(
            (state.active[2].sapling_size, state.active[2].orchard_size),
            (sapling_outputs, orchard_actions)
        );

        // The mined block now reconstructs with the extra transaction (coinbase + mined tx).
        let raw = state.active[1].to_raw();
        let (_, rebuilt) = compact::split_block(&raw).unwrap();
        assert_eq!(rebuilt.len(), 2);
        assert_eq!(rebuilt[1], tx);
    }

    #[test]
    fn stage_blocks_create_builds_parseable_consecutive_blocks() {
        let mut state = DarksideState::new();
        state.reset(&meta(1000));
        state.stage_blocks_create(1000, 7, 3).unwrap();
        state.apply_staged(1002).unwrap();

        assert_eq!(state.active.len(), 3);
        for (index, block) in state.active.iter().enumerate() {
            assert_eq!(
                raw_block_height(&block.to_raw()).unwrap(),
                1000 + index as u64
            );
        }
    }

    #[test]
    fn stage_block_rejects_height_below_sapling_activation() {
        let mut state = DarksideState::new();
        state.reset(&meta(500000));
        let error = state.stage_block(blocks()[0].clone()).unwrap_err();
        assert!(matches!(error, DarksideError::Invalid(_)));
    }

    // --- Phase 3: DarksideNode + Streamer --------------------------------------------------

    #[tokio::test]
    async fn darkside_node_serves_block_reads() {
        let handle = applied_handle(3).await;
        let node = DarksideNode::new(handle.clone());

        assert_eq!(node.get_block_count().await.unwrap(), 380642);

        let info = node.get_blockchain_info().await.unwrap();
        assert_eq!(info.chain, "main");
        assert_eq!(info.blocks, 380642);

        let verbose = node.get_block_verbose(380640).await.unwrap();
        let raw = node.get_block_raw(&verbose.hash).await.unwrap();
        assert_eq!(raw_block_height(&raw).unwrap(), 380640);
    }

    #[tokio::test]
    async fn darkside_node_send_transaction_populates_incoming() {
        let handle = applied_handle(1).await;
        let node = DarksideNode::new(handle.clone());
        let (_, txs) = compact::split_block(&blocks()[3]).unwrap();
        let tx = txs[1].clone();

        let txid = node.send_raw_transaction(&hex::encode(&tx)).await.unwrap();

        assert_eq!(txid, compact::txid_display(&tx).unwrap());
        assert_eq!(handle.lock().await.incoming_txs, vec![tx]);
    }

    #[tokio::test]
    async fn darkside_node_returns_staged_utxos() {
        let handle = applied_handle(1).await;
        handle.lock().await.add_utxo(GetAddressUtxosReply {
            address: "t1".to_string(),
            txid: vec![0x11, 0x22, 0x33, 0x44],
            index: 0,
            script: vec![0xab, 0xcd],
            value_zat: 5,
            height: 380640,
        });
        let node = DarksideNode::new(handle);

        let utxos = node.get_address_utxos(&["t1".to_string()]).await.unwrap();

        assert_eq!(utxos.len(), 1);
        // The wire txid is returned in display (reversed) order, as zebrad would report it.
        assert_eq!(utxos[0].txid, "44332211");
        assert_eq!(utxos[0].satoshis, 5);
    }

    #[tokio::test]
    async fn streamer_get_block_range_emits_staged_blocks() {
        use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
        use crate::proto::{BlockId, BlockRange};
        use tokio_stream::StreamExt;

        let handle = applied_handle(3).await;
        let node: Arc<dyn NodeRpc> = Arc::new(DarksideNode::new(handle.clone()));
        let dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(crate::cache::Cache::open(&dir.path().join("blocks.redb")).unwrap());
        let streamer = crate::service::Streamer::new(node, cache, "main".to_string(), Some(handle));

        let range = BlockRange {
            start: Some(BlockId {
                height: 380640,
                hash: vec![],
            }),
            end: Some(BlockId {
                height: 380642,
                hash: vec![],
            }),
            ..Default::default()
        };
        let response = streamer
            .get_block_range(Request::new(range))
            .await
            .unwrap()
            .into_inner();
        let emitted: Vec<_> = response.collect().await;

        let heights: Vec<u64> = emitted
            .iter()
            .map(|block| block.as_ref().unwrap().height)
            .collect();
        assert_eq!(heights, vec![380640, 380641, 380642]);
        // The compact blocks chain: each block's prev hash is the previous block's hash.
        assert_eq!(
            emitted[1].as_ref().unwrap().prev_hash,
            emitted[0].as_ref().unwrap().hash
        );
    }

    #[tokio::test]
    async fn streamer_get_subtree_roots_serves_staged_roots() {
        use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
        use crate::proto::{GetSubtreeRootsArg, ShieldedProtocol};
        use tokio_stream::StreamExt;

        let handle = applied_handle(1).await;
        handle.lock().await.set_subtree_roots(DarksideSubtreeRoots {
            shielded_protocol: ShieldedProtocol::Sapling as i32,
            start_index: 0,
            subtree_roots: vec![SubtreeRoot {
                root_hash: vec![1, 2, 3],
                completing_block_hash: vec![4, 5, 6],
                completing_block_height: 380640,
            }],
        });
        let node: Arc<dyn NodeRpc> = Arc::new(DarksideNode::new(handle.clone()));
        let dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(crate::cache::Cache::open(&dir.path().join("blocks.redb")).unwrap());
        let streamer = crate::service::Streamer::new(node, cache, "main".to_string(), Some(handle));

        let arg = GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: ShieldedProtocol::Sapling as i32,
            max_entries: 0,
        };
        let response = streamer
            .get_subtree_roots(Request::new(arg))
            .await
            .unwrap()
            .into_inner();
        let roots: Vec<_> = response.collect().await;

        assert_eq!(roots.len(), 1);
        let root = roots[0].as_ref().unwrap();
        assert_eq!(root.completing_block_height, 380640);
        assert_eq!(root.root_hash, vec![1, 2, 3]);
    }
}
