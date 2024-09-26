use apibara_dna_common::{fragment::FragmentInfo, ChainSupport};
use filter::StarknetFilterFactory;
use fragment::{
    EVENT_FRAGMENT_ID, EVENT_FRAGMENT_NAME, MESSAGE_FRAGMENT_ID, MESSAGE_FRAGMENT_NAME,
    RECEIPT_FRAGMENT_ID, RECEIPT_FRAGMENT_NAME, TRANSACTION_FRAGMENT_ID, TRANSACTION_FRAGMENT_NAME,
};
use ingestion::StarknetBlockIngestion;
use provider::StarknetProvider;

pub mod cli;
pub mod error;
pub mod filter;
pub mod fragment;
pub mod ingestion;
pub mod proto;
pub mod provider;

pub struct StarknetChainSupport {
    provider: StarknetProvider,
}

impl StarknetChainSupport {
    pub fn new(provider: StarknetProvider) -> Self {
        Self { provider }
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
        ]
    }

    fn block_filter_factory(&self) -> Self::BlockFilterFactory {
        StarknetFilterFactory
    }

    fn block_ingestion(&self) -> Self::BlockIngestion {
        StarknetBlockIngestion::new(self.provider.clone())
    }
}
