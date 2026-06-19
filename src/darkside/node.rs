//! [`DarksideNode`]: a [`NodeRpc`] implementation backed by the mock chain state, injected in place
//! of `NodeClient`.

use crate::node::{
    AddressUtxo, GetAddressBalance, GetBlockVerbose, GetBlockchainInfo, GetInfo, GetRawTransaction,
    GetSubtrees, GetTreeState, NodeError, NodeRpc,
};

use super::state::DarksideHandle;

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
        Ok(self.state.lock().await.raw_mempool()?)
    }
}
