use apibara_dna_common::{fragment::FragmentInfo, ChainSupport};
use filter::BeaconChainFilterFactory;
use fragment::{
    BLOB_FRAGMENT_ID, BLOB_FRAGMENT_NAME, TRANSACTION_FRAGMENT_ID, TRANSACTION_FRAGMENT_NAME,
    VALIDATOR_FRAGMENT_ID, VALIDATOR_FRAGMENT_NAME,
};
use ingestion::BeaconChainBlockIngestion;
use provider::http::BeaconApiProvider;

pub mod cli;
pub mod error;
pub mod filter;
pub mod fragment;
pub mod ingestion;
pub mod proto;
pub mod provider;

pub struct BeaconChainChainSupport {
    provider: BeaconApiProvider,
}

impl BeaconChainChainSupport {
    pub fn new(provider: BeaconApiProvider) -> Self {
        Self { provider }
    }
}

impl ChainSupport for BeaconChainChainSupport {
    type BlockIngestion = BeaconChainBlockIngestion;
    type BlockFilterFactory = BeaconChainFilterFactory;

    fn fragment_info(&self) -> Vec<FragmentInfo> {
        vec![
            FragmentInfo {
                fragment_id: TRANSACTION_FRAGMENT_ID,
                name: TRANSACTION_FRAGMENT_NAME.to_string(),
            },
            FragmentInfo {
                fragment_id: VALIDATOR_FRAGMENT_ID,
                name: VALIDATOR_FRAGMENT_NAME.to_string(),
            },
            FragmentInfo {
                fragment_id: BLOB_FRAGMENT_ID,
                name: BLOB_FRAGMENT_NAME.to_string(),
            },
        ]
    }

    fn block_filter_factory(&self) -> Self::BlockFilterFactory {
        BeaconChainFilterFactory
    }

    fn block_ingestion(&self) -> Self::BlockIngestion {
        BeaconChainBlockIngestion::new(self.provider.clone())
    }
}
