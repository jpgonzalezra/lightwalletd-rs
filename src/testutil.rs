//! Test-only helpers and fixtures shared across the module unit tests.

use std::sync::Mutex;

use async_trait::async_trait;

use crate::cache::Cache;
use crate::node::{
    AddressUtxo, GetAddressBalance, GetBlockVerbose, GetBlockchainInfo, GetInfo, GetRawTransaction,
    GetSubtrees, GetTreeState, NodeError, NodeRpc,
};

/// A fresh, empty [`Cache`] in a throwaway temp dir. The returned `TempDir` must be kept alive for
/// the cache file to outlive the test.
pub fn temp_cache() -> (tempfile::TempDir, Cache) {
    let dir = tempfile::tempdir().unwrap();
    let cache = Cache::open(&dir.path().join("blocks.redb")).unwrap();
    (dir, cache)
}

/// The consecutive raw blocks in `testdata/blocks` (heights 380640..=380643).
pub fn testdata_blocks() -> Vec<Vec<u8>> {
    std::fs::read_to_string("testdata/blocks")
        .unwrap()
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| hex::decode(line).unwrap())
        .collect()
}

/// Every v5 transaction vector in `testdata/tx_v5.json` (skipping its two header rows), each as
/// `(raw_tx, sapling_outputs, orchard_actions)`. The consensus branch id (bytes 8..12) is patched to
/// NU5 so the parser accepts these synthetic vectors.
pub fn shielded_v5_txs() -> Vec<(Vec<u8>, u32, u32)> {
    let json = std::fs::read_to_string("testdata/tx_v5.json").unwrap();
    let rows: Vec<Vec<serde_json::Value>> = serde_json::from_str(&json).unwrap();
    rows.iter()
        .skip(2)
        .map(|row| {
            let mut raw = hex::decode(row[0].as_str().unwrap()).unwrap();
            raw[8..12].copy_from_slice(&0xc2d6_d0b4u32.to_le_bytes());
            (
                raw,
                row[10].as_u64().unwrap() as u32,
                row[14].as_u64().unwrap() as u32,
            )
        })
        .collect()
}

/// A representative v5 transaction carrying both Sapling outputs and Orchard actions (the vector with
/// `nOutputsSapling = 2` and `nActionsOrchard = 4`).
pub fn shielded_v5_tx() -> (Vec<u8>, u32, u32) {
    shielded_v5_txs()
        .into_iter()
        .find(|(_, sapling, orchard)| *sapling == 2 && *orchard == 4)
        .expect("a v5 vector with 2 sapling outputs and 4 orchard actions")
}

/// A configurable [`NodeRpc`] fake. Each field holds the canned response for one RPC; a method whose
/// field is unset panics, so a test only configures the calls it exercises.
#[derive(Default)]
pub struct FakeNode {
    pub info: Option<GetInfo>,
    pub blockchain_info: Option<GetBlockchainInfo>,
    pub block_verbose: Option<GetBlockVerbose>,
    pub block_verbose_err: Option<(i64, String)>,
    pub block_count: Option<u64>,
    pub block_raw: Option<Vec<u8>>,
    pub raw_transaction: Option<GetRawTransaction>,
    pub raw_transaction_err: Option<(i64, String)>,
    pub send_ok: Option<String>,
    pub send_err: Option<(i64, String)>,
    pub treestate: Option<GetTreeState>,
    pub address_balance: Option<GetAddressBalance>,
    pub address_balance_err: Option<(i64, String)>,
    pub address_utxos: Option<Vec<AddressUtxo>>,
    pub address_utxos_err: Option<(i64, String)>,
    pub address_txids: Option<Vec<String>>,
    pub address_txids_err: Option<(i64, String)>,
    pub subtrees: Option<GetSubtrees>,
    pub raw_mempool: Option<Vec<String>>,
    /// Captures the txid string the service passed to `get_raw_transaction`.
    pub requested_txid: Mutex<Option<String>>,
}

#[async_trait]
impl NodeRpc for FakeNode {
    async fn get_info(&self) -> Result<GetInfo, NodeError> {
        Ok(self
            .info
            .clone()
            .expect("FakeNode: get_info not configured"))
    }

    async fn get_blockchain_info(&self) -> Result<GetBlockchainInfo, NodeError> {
        Ok(self
            .blockchain_info
            .clone()
            .expect("FakeNode: get_blockchain_info not configured"))
    }

    async fn get_block_verbose(&self, _height: u64) -> Result<GetBlockVerbose, NodeError> {
        if let Some((code, message)) = self.block_verbose_err.clone() {
            return Err(NodeError::Rpc { code, message });
        }
        Ok(self
            .block_verbose
            .clone()
            .expect("FakeNode: get_block_verbose not configured"))
    }

    async fn get_block_count(&self) -> Result<u64, NodeError> {
        Ok(self
            .block_count
            .expect("FakeNode: get_block_count not configured"))
    }

    async fn get_block_raw(&self, _hash: &str) -> Result<Vec<u8>, NodeError> {
        Ok(self
            .block_raw
            .clone()
            .expect("FakeNode: get_block_raw not configured"))
    }

    async fn get_raw_transaction(&self, txid: &str) -> Result<GetRawTransaction, NodeError> {
        *self.requested_txid.lock().unwrap() = Some(txid.to_string());
        if let Some((code, message)) = self.raw_transaction_err.clone() {
            return Err(NodeError::Rpc { code, message });
        }
        Ok(self
            .raw_transaction
            .clone()
            .expect("FakeNode: get_raw_transaction not configured"))
    }

    async fn send_raw_transaction(&self, _hex: &str) -> Result<String, NodeError> {
        if let Some((code, message)) = self.send_err.clone() {
            return Err(NodeError::Rpc { code, message });
        }
        Ok(self
            .send_ok
            .clone()
            .expect("FakeNode: send_raw_transaction not configured"))
    }

    async fn get_treestate(&self, _id: &str) -> Result<GetTreeState, NodeError> {
        Ok(self
            .treestate
            .clone()
            .expect("FakeNode: get_treestate not configured"))
    }

    async fn get_address_balance(
        &self,
        _addresses: &[String],
    ) -> Result<GetAddressBalance, NodeError> {
        if let Some((code, message)) = self.address_balance_err.clone() {
            return Err(NodeError::Rpc { code, message });
        }
        Ok(self
            .address_balance
            .clone()
            .expect("FakeNode: get_address_balance not configured"))
    }

    async fn get_address_utxos(
        &self,
        _addresses: &[String],
    ) -> Result<Vec<AddressUtxo>, NodeError> {
        if let Some((code, message)) = self.address_utxos_err.clone() {
            return Err(NodeError::Rpc { code, message });
        }
        Ok(self
            .address_utxos
            .clone()
            .expect("FakeNode: get_address_utxos not configured"))
    }

    async fn get_address_txids(
        &self,
        _addresses: &[String],
        _start: u64,
        _end: u64,
    ) -> Result<Vec<String>, NodeError> {
        if let Some((code, message)) = self.address_txids_err.clone() {
            return Err(NodeError::Rpc { code, message });
        }
        Ok(self
            .address_txids
            .clone()
            .expect("FakeNode: get_address_txids not configured"))
    }

    async fn get_subtrees(
        &self,
        _protocol: &str,
        _start_index: u32,
        _max_entries: u32,
    ) -> Result<GetSubtrees, NodeError> {
        Ok(self
            .subtrees
            .clone()
            .expect("FakeNode: get_subtrees not configured"))
    }

    async fn get_raw_mempool(&self) -> Result<Vec<String>, NodeError> {
        Ok(self
            .raw_mempool
            .clone()
            .expect("FakeNode: get_raw_mempool not configured"))
    }
}
