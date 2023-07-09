use std::time::{Duration, Instant};

use apibara_core::node::v1alpha2::{
    stream_data_response, Data, DataFinality, Heartbeat, Invalidate, StreamDataResponse,
};
use async_stream::stream;
use futures::{stream::FusedStream, Stream, StreamExt};
use prost::Message;
use tracing::{debug_span, instrument, trace, Instrument};

use crate::{core::Cursor, server::RequestMeter, stream::BatchCursor};

use super::{
    BatchProducer, CursorProducer, IngestionMessage, IngestionResponse, ReconfigureResponse,
    StreamConfiguration, StreamError,
};

pub fn new_data_stream<C, F, B, M>(
    configuration_stream: impl Stream<Item = Result<StreamConfiguration<C, F>, StreamError>> + Unpin,
    ingestion_stream: impl Stream<Item = Result<IngestionMessage<C>, StreamError>> + Unpin,
    mut cursor_producer: impl CursorProducer<Cursor = C, Filter = F> + Unpin + FusedStream,
    mut batch_producer: impl BatchProducer<Cursor = C, Filter = F, Block = B>,
    meter: M,
) -> impl Stream<Item = Result<StreamDataResponse, StreamError>>
where
    C: Cursor + Send + Sync,
    F: Message + Default + Clone,
    B: Message + Default + Clone,
    M: RequestMeter,
{
    let mut configuration_stream = configuration_stream.fuse();
    let mut ingestion_stream = ingestion_stream.fuse();

    // try_stream! doesn't work with tokio::select! so we have to use stream! and helper functions.
    Box::pin(stream! {
        let mut stream_id = 0;
        let mut has_configuration = false;
        let mut last_batch_sent = Instant::now();
        // Send a batch (no matter if empty or not) at least once every this interval.
        let max_batch_interval = Duration::from_secs(10);

        {
            // Some clients (notably tonic) wait for the first message before
            // returning the response stream. Since this stream won't produce
            // any data until a configuration is sent, it will result in the
            // client waiting for the first heartbeat message.
            // To avoid this, we send a heartbeat message as soon as possible.
            use stream_data_response::Message;
            yield Ok(StreamDataResponse {
                stream_id,
                message: Some(Message::Heartbeat(Heartbeat::default())),
            });
        }

        loop {
            tokio::select! {
                // check streams in order.
                // always check configuration stream first since any change to configuration will
                // change the data being produced.
                // then check ingestion messages, this also helps avoid sending data and then
                // immediately invalidating it.
                // only at the end, produce new data.
                biased;

                configuration_message = configuration_stream.select_next_some() => {
                    has_configuration = true;
                    match handle_configuration_message(&mut cursor_producer, &mut batch_producer, configuration_message).await {
                        Ok((new_stream_id, configure_response)) => {
                            stream_id = new_stream_id;
                            // send invalidate message if the specified cursor is no longer valid.
                            match configure_response {
                                ReconfigureResponse::Ok => {},
                                ReconfigureResponse::MissingStartingCursor => {
                                    yield Err(StreamError::invalid_request("the specified starting cursor doesn't exist".to_string()));
                                    break;
                                },
                                ReconfigureResponse::Invalidate(cursor) => {
                                    use stream_data_response::Message;
                                    let message = Invalidate {
                                        cursor: Some(cursor.to_proto()),
                                    };

                                    yield Ok(StreamDataResponse {
                                        stream_id,
                                        message: Some(Message::Invalidate(message)),
                                    });
                                },
                            };
                        },
                        Err(err) => {
                            yield Err(err);
                            break;
                        },
                    }
                },

                ingestion_message = ingestion_stream.select_next_some() => {
                    match handle_ingestion_message(&mut cursor_producer, ingestion_message).await {
                        Ok(IngestionResponse::Invalidate(cursor)) => {
                            use stream_data_response::Message;
                            let message = Invalidate {
                                cursor: Some(cursor.to_proto()),
                            };

                            yield Ok(StreamDataResponse {
                                stream_id,
                                message: Some(Message::Invalidate(message)),
                            });
                        },
                        Ok(IngestionResponse::Ok) => {
                            // nothing to do.
                            // either message was a new accepted/finalized block, or stream is at
                            // lower block than invalidated message.
                        },
                        Err(err) => {
                            yield Err(err);
                            break;
                        },
                    }
                },

                batch_cursor = cursor_producer.select_next_some(), if has_configuration => {
                    use stream_data_response::Message;

                    match handle_batch_cursor(&mut cursor_producer, &mut batch_producer, batch_cursor, &meter).await {
                        Ok((data, finality)) => {
                            let should_send_data =
                                if !data.data.is_empty() || finality == DataFinality::DataStatusAccepted {
                                    true
                                } else {
                                    last_batch_sent.elapsed() > max_batch_interval
                                };

                            if !should_send_data {
                                trace!("skip empty batch");
                                continue
                            }

                            last_batch_sent = Instant::now();
                            yield Ok(StreamDataResponse {
                                stream_id,
                                message: Some(Message::Data(data)),
                            });
                        },
                        Err(err) => {
                            yield Err(err);
                            break;
                        },
                    }
                }
            }
        }
    })
}

#[instrument(skip_all, level = "debug")]
async fn handle_configuration_message<C, F, B>(
    cursor_producer: &mut impl CursorProducer<Cursor = C, Filter = F>,
    batch_producer: &mut impl BatchProducer<Cursor = C, Filter = F, Block = B>,
    configuration_message: Result<StreamConfiguration<C, F>, StreamError>,
) -> Result<(u64, ReconfigureResponse<C>), StreamError>
where
    C: Cursor + Send + Sync,
    F: Message + Default + Clone,
    B: Message + Default + Clone,
{
    let configuration_message = configuration_message?;

    let cursor_producer_span = debug_span!(
        "reconfigure_cursor_producer",
        stream_id = configuration_message.stream_id
    );
    let ingestion_response = cursor_producer
        .reconfigure(&configuration_message)
        .instrument(cursor_producer_span)
        .await?;

    let batch_producer_span = debug_span!(
        "reconfigure_batch_producer",
        stream_id = configuration_message.stream_id
    );
    batch_producer_span.in_scope(|| batch_producer.reconfigure(&configuration_message))?;

    Ok((configuration_message.stream_id, ingestion_response))
}

#[instrument(skip_all, level = "debug")]
async fn handle_ingestion_message<C, F>(
    cursor_producer: &mut impl CursorProducer<Cursor = C, Filter = F>,
    ingestion_message: Result<IngestionMessage<C>, StreamError>,
) -> Result<IngestionResponse<C>, StreamError>
where
    C: Cursor + Send + Sync,
    F: Message + Default + Clone,
{
    let ingestion_message = ingestion_message?;
    cursor_producer
        .handle_ingestion_message(&ingestion_message)
        .await
}

async fn handle_batch_cursor<C, F, B, M>(
    _cursor_producer: &mut impl CursorProducer<Cursor = C, Filter = F>,
    batch_producer: &mut impl BatchProducer<Cursor = C, Filter = F, Block = B>,
    batch_cursor: Result<BatchCursor<C>, StreamError>,
    meter: &M,
) -> Result<(Data, DataFinality), StreamError>
where
    C: Cursor + Send + Sync,
    F: Message + Default + Clone,
    B: Message + Default + Clone,
    M: RequestMeter,
{
    let batch_cursor = batch_cursor?;
    let (start_cursor, cursors, end_cursor, finality) = match batch_cursor {
        BatchCursor::Finalized(start_cursor, cursors) => {
            let end_cursor = cursors.last().cloned();
            (
                start_cursor,
                cursors,
                end_cursor,
                DataFinality::DataStatusFinalized,
            )
        }
        BatchCursor::Accepted(start_cursor, cursor) => (
            start_cursor,
            vec![cursor.clone()],
            Some(cursor),
            DataFinality::DataStatusAccepted,
        ),
        BatchCursor::Pending(start_cursor, cursor) => (
            start_cursor,
            vec![cursor.clone()],
            Some(cursor),
            DataFinality::DataStatusPending,
        ),
    };

    let handle_batch_span = debug_span!(
        "handle_batch",
        start_cursor = ?start_cursor,
        end_cursor = ?end_cursor,
    );
    async move {
        let next_batch_span = debug_span!(
            "next_batch",
            start_cursor = ?start_cursor,
            end_cursor = ?end_cursor,
        );
        let batch = batch_producer
            .next_batch(cursors.into_iter(), meter)
            .instrument(next_batch_span)
            .await?;

        let serialize_batch_span = debug_span!(
            "serialize_batch",
            start_cursor = ?start_cursor,
            end_cursor = ?end_cursor,
        );
        let data = serialize_batch_span.in_scope(|| {
            batch
                .iter()
                .map(|block| block.encode_to_vec())
                .collect::<Vec<_>>()
        });

        let data = Data {
            cursor: start_cursor.map(|cursor| cursor.to_proto()),
            end_cursor: end_cursor.map(|cursor| cursor.to_proto()),
            finality: finality as i32,
            data,
        };
        Ok((data, finality))
    }
    .instrument(handle_batch_span)
    .await
}
