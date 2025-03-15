pub mod cli;
pub mod error;
pub mod filter;
pub mod fragment;
pub mod ingestion;
pub mod proto;
pub mod provider;

use apibara_dna_common::{fragment::FragmentInfo, ChainSupport};
use fragment::{TRACE_FRAGMENT_ID, TRACE_FRAGMENT_NAME};

use crate::{
    filter::EvmFilterFactory,
    fragment::{
        LOG_FRAGMENT_ID, LOG_FRAGMENT_NAME, RECEIPT_FRAGMENT_ID, RECEIPT_FRAGMENT_NAME,
        TRANSACTION_FRAGMENT_ID, TRANSACTION_FRAGMENT_NAME, WITHDRAWAL_FRAGMENT_ID,
        WITHDRAWAL_FRAGMENT_NAME,
    },
    ingestion::EvmBlockIngestion,
    provider::JsonRpcProvider,
};

pub use ingestion::EvmBlockIngestionOptions;

pub struct EvmChainSupport {
    provider: JsonRpcProvider,
    options: EvmBlockIngestionOptions,
}

impl EvmChainSupport {
    pub fn new(provider: JsonRpcProvider, options: EvmBlockIngestionOptions) -> Self {
        Self { provider, options }
    }
}

impl ChainSupport for EvmChainSupport {
    type BlockIngestion = EvmBlockIngestion;
    type BlockFilterFactory = EvmFilterFactory;

    fn fragment_info(&self) -> Vec<FragmentInfo> {
        vec![
            FragmentInfo {
                fragment_id: WITHDRAWAL_FRAGMENT_ID,
                name: WITHDRAWAL_FRAGMENT_NAME.to_string(),
            },
            FragmentInfo {
                fragment_id: TRANSACTION_FRAGMENT_ID,
                name: TRANSACTION_FRAGMENT_NAME.to_string(),
            },
            FragmentInfo {
                fragment_id: RECEIPT_FRAGMENT_ID,
                name: RECEIPT_FRAGMENT_NAME.to_string(),
            },
            FragmentInfo {
                fragment_id: LOG_FRAGMENT_ID,
                name: LOG_FRAGMENT_NAME.to_string(),
            },
            FragmentInfo {
                fragment_id: TRACE_FRAGMENT_ID,
                name: TRACE_FRAGMENT_NAME.to_string(),
            },
        ]
    }

    fn block_filter_factory(&self) -> Self::BlockFilterFactory {
        EvmFilterFactory
    }

    fn block_ingestion(&self) -> Self::BlockIngestion {
        EvmBlockIngestion::new(self.provider.clone(), self.options.clone())
    }
}
