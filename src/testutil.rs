//! Test-only helpers shared across the module unit tests.

use std::sync::Mutex;

use async_trait::async_trait;

use crate::node::{
    AddressUtxo, GetAddressBalance, GetBlockVerbose, GetBlockchainInfo, GetInfo, GetRawTransaction,
    GetSubtrees, GetTreeState, NodeError, NodeRpc,
};

/// A configurable [`NodeRpc`] fake. Each field holds the canned response for one RPC; a method whose
/// field is unset panics, so a test only configures the calls it exercises.
#[derive(Default)]
pub struct FakeNode {
    pub info: Option<GetInfo>,
    pub blockchain_info: Option<GetBlockchainInfo>,
    pub block_verbose: Option<GetBlockVerbose>,
    pub block_count: Option<u64>,
    pub block_raw: Option<Vec<u8>>,
    pub raw_transaction: Option<GetRawTransaction>,
    pub send_ok: Option<String>,
    pub send_err: Option<(i64, String)>,
    pub treestate: Option<GetTreeState>,
    pub address_balance: Option<GetAddressBalance>,
    pub address_utxos: Option<Vec<AddressUtxo>>,
    pub address_txids: Option<Vec<String>>,
    pub subtrees: Option<GetSubtrees>,
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
        Ok(self
            .address_balance
            .clone()
            .expect("FakeNode: get_address_balance not configured"))
    }

    async fn get_address_utxos(
        &self,
        _addresses: &[String],
    ) -> Result<Vec<AddressUtxo>, NodeError> {
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
}
