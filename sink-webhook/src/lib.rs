use apibara_core::{
    node::v1alpha2::{Cursor, DataFinality},
    starknet::v1alpha2::Block,
};
use apibara_sink_common::Sink;
use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum WebhookError {}

pub struct WebhookSink {}

#[async_trait]
impl Sink<Block> for WebhookSink {
    type Error = WebhookError;

    async fn handle_data(
        &mut self,
        cursor: Option<Cursor>,
        end_cursor: Cursor,
        _finality: DataFinality,
        _batch: Vec<Block>,
    ) -> Result<(), Self::Error> {
        println!("calling web hook with data {:?} - {:?}", cursor, end_cursor);
        Ok(())
    }

    async fn handle_invalidate(&mut self, cursor: Option<Cursor>) -> Result<(), Self::Error> {
        println!("calling web hook with invalidate {:?}", cursor);
        Ok(())
    }
}
