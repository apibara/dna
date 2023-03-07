use std::{
    pin::Pin,
    task::{Context, Poll},
};

use config::Configuration;
use futures::Stream;
use pb::stream::v1alpha2::{stream_client::StreamClient, StreamDataResponse};
use pin_project::pin_project;
use tokio::sync::mpsc::{error::SendError, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Channel, Streaming};

use crate::pb::{
    starknet::v1alpha2::Filter,
    stream::v1alpha2::{Cursor, StreamDataRequest},
};

pub mod config;
pub mod pb {
    pub mod starknet {
        pub mod v1alpha2 {
            use std::fmt::Display;

            use prost::Message;
            tonic::include_proto!("apibara.starknet.v1alpha2");

            #[derive(Clone, PartialEq, Message)]
            pub struct BlockBody {
                #[prost(message, repeated, tag = "1")]
                pub transactions: prost::alloc::vec::Vec<Transaction>,
            }

            #[derive(Clone, PartialEq, Message)]
            pub struct BlockReceipts {
                #[prost(message, repeated, tag = "1")]
                pub receipts: prost::alloc::vec::Vec<TransactionReceipt>,
            }

            pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
                tonic::include_file_descriptor_set!("starknet_descriptor_v1alpha2");

            pub fn starknet_file_descriptor_set() -> &'static [u8] {
                FILE_DESCRIPTOR_SET
            }

            impl BlockStatus {
                pub fn is_finalized(&self) -> bool {
                    *self == BlockStatus::AcceptedOnL1
                }

                pub fn is_accepted(&self) -> bool {
                    *self == BlockStatus::AcceptedOnL2
                }

                pub fn is_rejected(&self) -> bool {
                    *self == BlockStatus::Rejected
                }
            }

            impl FieldElement {
                pub fn from_u64(value: u64) -> FieldElement {
                    FieldElement {
                        lo_lo: 0,
                        lo_hi: 0,
                        hi_lo: 0,
                        hi_hi: value,
                    }
                }

                pub fn from_bytes(bytes: &[u8; 32]) -> Self {
                    let lo_lo = u64::from_be_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                        bytes[7],
                    ]);
                    let lo_hi = u64::from_be_bytes([
                        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                        bytes[15],
                    ]);
                    let hi_lo = u64::from_be_bytes([
                        bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21],
                        bytes[22], bytes[23],
                    ]);
                    let hi_hi = u64::from_be_bytes([
                        bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29],
                        bytes[30], bytes[31],
                    ]);

                    FieldElement {
                        lo_lo,
                        lo_hi,
                        hi_lo,
                        hi_hi,
                    }
                }

                pub fn to_bytes(&self) -> [u8; 32] {
                    let lo_lo = self.lo_lo.to_be_bytes();
                    let lo_hi = self.lo_hi.to_be_bytes();
                    let hi_lo = self.hi_lo.to_be_bytes();
                    let hi_hi = self.hi_hi.to_be_bytes();
                    [
                        lo_lo[0], lo_lo[1], lo_lo[2], lo_lo[3], lo_lo[4], lo_lo[5], lo_lo[6],
                        lo_lo[7], lo_hi[0], lo_hi[1], lo_hi[2], lo_hi[3], lo_hi[4], lo_hi[5],
                        lo_hi[6], lo_hi[7], hi_lo[0], hi_lo[1], hi_lo[2], hi_lo[3], hi_lo[4],
                        hi_lo[5], hi_lo[6], hi_lo[7], hi_hi[0], hi_hi[1], hi_hi[2], hi_hi[3],
                        hi_hi[4], hi_hi[5], hi_hi[6], hi_hi[7],
                    ]
                }
            }

            impl Display for FieldElement {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "0x{}", hex::encode(self.to_bytes()))
                }
            }
        }
    }

    pub mod stream {
        pub mod v1alpha2 {
            tonic::include_proto!("apibara.node.v1alpha2");

            pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
                tonic::include_file_descriptor_set!("starknet_descriptor_v1alpha2");

            pub fn stream_file_descriptor_set() -> &'static [u8] {
                FILE_DESCRIPTOR_SET
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientBuilderError {
    #[error("Failed to build indexer")]
    FailedToBuildIndexer,
    #[error(transparent)]
    TonicError(#[from] tonic::transport::Error),
    #[error(transparent)]
    FailedToConfigureStream(#[from] SendError<Configuration>),
    #[error(transparent)]
    StreamError(#[from] tonic::Status),
}

pub struct ClientBuilder<StreamClient> {
    client: Option<StreamClient>,
    configuration: Option<Configuration>,
    configuration_channel: Option<(Sender<Configuration>, Receiver<Configuration>)>,
    inner_channel: Option<(Sender<StreamDataRequest>, Receiver<StreamDataRequest>)>,
}

impl<StreamClient> Default for ClientBuilder<StreamClient> {
    fn default() -> Self {
        Self {
            client: None,
            configuration: None,
            configuration_channel: None,
            inner_channel: None,
        }
    }
}

#[derive(Debug)]
#[pin_project]
pub struct DataStream {
    stream_id: u64,
    configuration_rx: Receiver<Configuration>,
    #[pin]
    inner: Streaming<StreamDataResponse>,
    inner_tx: Sender<StreamDataRequest>,
}

impl ClientBuilder<StreamClient<Channel>> {
    pub fn with_client(mut self, client: StreamClient<Channel>) -> Self {
        self.client = Some(client);
        self
    }

    pub fn with_configuration(mut self, configuration: Configuration) -> Self {
        self.configuration = Some(configuration);
        self
    }

    /// Apibara indexer SDK
    ///
    /// let base_configuration = Configuration::default();
    /// let (mut stream, _configuration_handle) =
    ///     ApibaraIndexer::default().build().await?;
    /// while let Some(response) = stream.next().await {
    ///     // Handle your own business logic in there.
    /// }
    pub async fn build(
        self,
        indexer_url: Option<String>,
    ) -> Result<(DataStream, Sender<Configuration>), ClientBuilderError> {
        let mut client = self.client.unwrap_or(
            StreamClient::connect(indexer_url.unwrap_or("http://0.0.0.0:7171".into())).await?,
        );
        let (configuration_tx, configuration_rx) = tokio::sync::mpsc::channel(128);
        let (inner_tx, inner_rx) = tokio::sync::mpsc::channel(128);

        if let Some(configuration) = self.configuration {
            configuration_tx.send(configuration).await?;
        }

        let inner_stream = client
            .stream_data(ReceiverStream::new(inner_rx))
            .await?
            .into_inner();

        Ok((
            DataStream {
                stream_id: 0,
                configuration_rx,
                inner: inner_stream,
                inner_tx,
            },
            configuration_tx,
        ))
    }
}

impl Stream for DataStream {
    type Item = Result<StreamDataResponse, Box<dyn std::error::Error>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.configuration_rx.poll_recv(cx) {
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(configuration)) => {
                self.stream_id += 1;
                let request = StreamDataRequest {
                    stream_id: Some(self.stream_id),
                    batch_size: Some(configuration.batch_size),
                    starting_cursor: configuration.starting_cursor,
                    finality: configuration.finality.into(),
                    filter: configuration.filter,
                };

                self.inner_tx.try_send(request)?;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Pending => match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(response))) => {
                    if response.stream_id != self.stream_id {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }

                    return Poll::Ready(Some(Ok(response)));
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Box::new(e)))),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{config::Configuration, pb::starknet::v1alpha2::HeaderFilter, ClientBuilder};
    use futures_util::StreamExt;

    #[tokio::test]
    async fn test_apibara_high_level_api() -> Result<(), Box<dyn std::error::Error>> {
        let (mut stream, configuration_handle) = ClientBuilder::default()
            .build(Some("http://0.0.0.0:7171".into()))
            .await?;

        let mut config = Configuration::default();
        configuration_handle
            .send(
                config
                    .starting_at_block(1)
                    .with_filter(|mut filter| filter.with_header(HeaderFilter { weak: false }))
                    .clone(),
            )
            .await?;

        while let Some(response) = stream.next().await {
            println!("Response: {:?}", response);
        }

        Ok(())
    }
}
