use apibara_dna_common::{fragment::FragmentInfo, ChainSupport};
use filter::StarknetFilterFactory;
use fragment::{
    CONTRACT_CHANGE_FRAGMENT_ID, CONTRACT_CHANGE_FRAGMENT_NAME, EVENT_FRAGMENT_ID,
    EVENT_FRAGMENT_NAME, MESSAGE_FRAGMENT_ID, MESSAGE_FRAGMENT_NAME, NONCE_UPDATE_FRAGMENT_ID,
    NONCE_UPDATE_FRAGMENT_NAME, RECEIPT_FRAGMENT_ID, RECEIPT_FRAGMENT_NAME,
    STORAGE_DIFF_FRAGMENT_ID, STORAGE_DIFF_FRAGMENT_NAME, TRANSACTION_FRAGMENT_ID,
    TRANSACTION_FRAGMENT_NAME,
};
use ingestion::StarknetBlockIngestion;
use provider::StarknetProvider;

pub use ingestion::StarknetBlockIngestionOptions;

pub mod cli;
pub mod error;
pub mod filter;
pub mod fragment;
pub mod ingestion;
pub mod proto;
pub mod provider;

pub struct StarknetChainSupport {
    provider: StarknetProvider,
    options: StarknetBlockIngestionOptions,
}

impl StarknetChainSupport {
    pub fn new(provider: StarknetProvider, options: StarknetBlockIngestionOptions) -> Self {
        Self { provider, options }
    }
}

impl ChainSupport for StarknetChainSupport {
    type BlockIngestion = StarknetBlockIngestion;
    type BlockFilterFactory = StarknetFilterFactory;

    fn fragment_info(&self) -> Vec<FragmentInfo> {
        vec![
            FragmentInfo {
                fragment_id: TRANSACTION_FRAGMENT_ID,
                name: TRANSACTION_FRAGMENT_NAME.to_string(),
            },
            FragmentInfo {
                fragment_id: RECEIPT_FRAGMENT_ID,
                name: RECEIPT_FRAGMENT_NAME.to_string(),
            },
            FragmentInfo {
                fragment_id: EVENT_FRAGMENT_ID,
                name: EVENT_FRAGMENT_NAME.to_string(),
            },
            FragmentInfo {
                fragment_id: MESSAGE_FRAGMENT_ID,
                name: MESSAGE_FRAGMENT_NAME.to_string(),
            },
            FragmentInfo {
                fragment_id: STORAGE_DIFF_FRAGMENT_ID,
                name: STORAGE_DIFF_FRAGMENT_NAME.to_string(),
            },
            FragmentInfo {
                fragment_id: CONTRACT_CHANGE_FRAGMENT_ID,
                name: CONTRACT_CHANGE_FRAGMENT_NAME.to_string(),
            },
            FragmentInfo {
                fragment_id: NONCE_UPDATE_FRAGMENT_ID,
                name: NONCE_UPDATE_FRAGMENT_NAME.to_string(),
            },
        ]
    }

    fn block_filter_factory(&self) -> Self::BlockFilterFactory {
        StarknetFilterFactory
    }

    fn block_ingestion(&self) -> Self::BlockIngestion {
        StarknetBlockIngestion::new(self.provider.clone(), self.options.clone())
    }
}
