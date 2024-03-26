use apibara_dna_protocol::ingestion::ingestion_client::IngestionClient;
use tokio_util::sync::CancellationToken;
use tonic::{async_trait, transport::Endpoint};

use crate::error::Result;

pub struct IngestionState {}

impl IngestionState {
    pub async fn start(self, ct: CancellationToken) -> Result<()> {
        todo!();
    }
}
