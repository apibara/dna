use apibara_sink_common::Sink;
use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum WebhookError {}

pub struct WebhookSink {}

#[async_trait]
impl Sink for WebhookSink {
    type Error = WebhookError;

    async fn handle_data(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn handle_invalidate(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
