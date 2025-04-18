use std::{collections::HashMap, sync::Arc, time::Duration};

use apibara_dna_protocol::dna::stream::{
    dna_stream_server::{self, DnaStream},
    DataFinality, StatusRequest, StatusResponse, StreamDataRequest,
};
use error_stack::Result;
use futures::{Future, TryFutureExt};
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::{
    block_store::BlockStoreReader,
    chain_view::{CanonicalCursor, ChainView, ChainViewError, ValidatedCursor},
    data_stream::{BlockFilterFactory, DataStream, DataStreamMetrics},
    fragment::FragmentId,
    server::stream_with_heartbeat::ResponseStreamWithHeartbeat,
    Cursor,
};

static STREAM_SEMAPHORE_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct StreamServiceOptions {
    /// Maximum number of concurrent streams.
    pub max_concurrent_streams: usize,
    /// Number of segments to prefetch.
    pub prefetch_segment_count: usize,
    /// Size of the channel used to send data to the stream.
    pub channel_size: usize,
}

pub struct StreamService<BFF>
where
    BFF: BlockFilterFactory,
{
    filter_factory: BFF,
    stream_semaphore: Arc<Semaphore>,
    chain_view: tokio::sync::watch::Receiver<Option<ChainView>>,
    fragment_id_to_name: HashMap<FragmentId, String>,
    block_store: BlockStoreReader,
    options: StreamServiceOptions,
    metrics: DataStreamMetrics,
    ct: CancellationToken,
}

impl<BFF> StreamService<BFF>
where
    BFF: BlockFilterFactory,
{
    pub fn new(
        filter_factory: BFF,
        chain_view: tokio::sync::watch::Receiver<Option<ChainView>>,
        fragment_id_to_name: HashMap<FragmentId, String>,
        block_store: BlockStoreReader,
        options: StreamServiceOptions,
        ct: CancellationToken,
    ) -> Self {
        let stream_semaphore = Arc::new(Semaphore::new(options.max_concurrent_streams));
        Self {
            filter_factory,
            stream_semaphore,
            chain_view,
            fragment_id_to_name,
            block_store,
            options,
            metrics: Default::default(),
            ct,
        }
    }

    pub fn into_service(self) -> dna_stream_server::DnaStreamServer<Self> {
        dna_stream_server::DnaStreamServer::new(self)
    }

    pub fn current_stream_count(&self) -> usize {
        self.options.max_concurrent_streams - self.stream_semaphore.available_permits()
    }

    pub fn current_stream_available(&self) -> usize {
        self.stream_semaphore.available_permits()
    }
}

#[tonic::async_trait]
impl<BFF> DnaStream for StreamService<BFF>
where
    BFF: BlockFilterFactory + Send + Sync + 'static,
{
    type StreamDataStream = ResponseStreamWithHeartbeat;

    #[tracing::instrument(name = "stream::status", skip_all)]
    async fn status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> tonic::Result<tonic::Response<StatusResponse>, tonic::Status> {
        let Some(chain_view) = self.chain_view.borrow().clone() else {
            return Err(tonic::Status::unavailable("chain view not initialized yet"));
        };

        let response = chain_view.get_status().await.map_err(|err| {
            error!(error = ?err, "DnaStream::status error");
            tonic::Status::internal("internal server error")
        })?;

        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(
        name = "stream::stream_data",
        skip_all,
        fields(stream_count, stream_available)
    )]
    async fn stream_data(
        &self,
        request: tonic::Request<StreamDataRequest>,
    ) -> tonic::Result<tonic::Response<Self::StreamDataStream>, tonic::Status> {
        let current_span = tracing::Span::current();

        let request = request.into_inner();
        info!(request = ?request, "stream data request");

        let Some(chain_view) = self.chain_view.borrow().clone() else {
            return Err(tonic::Status::unavailable("chain view not initialized yet"));
        };

        let permit = match tokio::time::timeout(
            STREAM_SEMAPHORE_ACQUIRE_TIMEOUT,
            self.stream_semaphore.clone().acquire_owned(),
        )
        .await
        {
            Err(_) => {
                return Err(tonic::Status::resource_exhausted("too many streams"));
            }
            Ok(Err(_)) => return Err(tonic::Status::internal("internal server error")),
            Ok(Ok(permit)) => permit,
        };

        current_span.record("stream_count", self.current_stream_count());
        current_span.record("stream_available", self.current_stream_available());

        // Validate starting cursor by checking it's in range.
        // The block could be reorged but that's handled by the `DataStream`.
        let starting_cursor = if let Some(cursor) = request.starting_cursor {
            let cursor = Cursor::from(cursor);
            debug!(cursor = %cursor, "starting cursor before validation");
            chain_view.ensure_cursor_in_range(&cursor).await?;
            match chain_view.validate_cursor(&cursor).await {
                Ok(ValidatedCursor::Valid(cursor)) => Some(cursor),
                Ok(ValidatedCursor::Invalid(canonical, siblings)) => {
                    let sibling_hashes = if siblings.is_empty() {
                        "none".to_string()
                    } else {
                        siblings
                            .iter()
                            .map(|c| c.hash_as_hex())
                            .collect::<Vec<_>>()
                            .join(", ")
                    };
                    return Err(tonic::Status::invalid_argument(format!(
                        "starting cursor {cursor} not found. canonical: {}, reorged: {sibling_hashes}",
                        canonical.hash_as_hex()
                    )));
                }
                Err(_) => {
                    return Err(tonic::Status::internal("internal server error"));
                }
            }
        } else {
            None
        };

        let finalized = chain_view
            .get_finalized_cursor()
            .await
            .map_err(|_| tonic::Status::internal("internal server error"))?;

        // Convert finality.
        let finality: DataFinality = request
            .finality
            .map(TryFrom::try_from)
            .transpose()
            .map_err(|_| tonic::Status::invalid_argument("invalid finality"))?
            .unwrap_or(DataFinality::Accepted);

        let heartbeat_interval = request
            .heartbeat_interval
            .map(TryFrom::try_from)
            .transpose()
            .map_err(|_| tonic::Status::invalid_argument("invalid heartbeat interval"))
            .and_then(validate_heartbeat_interval)?;

        // Parse and validate filter.
        let filter = self.filter_factory.create_block_filter(&request.filter)?;

        let ds = DataStream::new(
            filter,
            starting_cursor,
            finalized,
            finality,
            chain_view,
            self.fragment_id_to_name.clone(),
            self.block_store.clone(),
            self.options.prefetch_segment_count,
            permit,
            self.metrics.clone(),
        );
        let (tx, rx) = mpsc::channel(self.options.channel_size);

        tokio::spawn(ds.start(tx, self.ct.clone()).inspect_err(|err| {
            error!(error = ?err, "data stream error");
        }));

        let stream = ResponseStreamWithHeartbeat::new(rx, heartbeat_interval);

        Ok(tonic::Response::new(stream))
    }
}

trait ChainViewExt {
    fn get_status(&self) -> impl Future<Output = Result<StatusResponse, ChainViewError>> + Send;
    fn ensure_cursor_in_range(
        &self,
        cursor: &Cursor,
    ) -> impl Future<Output = tonic::Result<(), tonic::Status>> + Send;
}

impl ChainViewExt for ChainView {
    async fn get_status(&self) -> Result<StatusResponse, ChainViewError> {
        let starting = self.get_starting_cursor().await?;
        let finalized = self.get_finalized_cursor().await?;
        let head = self.get_head().await?;

        Ok(StatusResponse {
            current_head: None,
            last_ingested: Some(head.into()),
            finalized: Some(finalized.into()),
            starting: Some(starting.into()),
        })
    }

    async fn ensure_cursor_in_range(&self, cursor: &Cursor) -> tonic::Result<(), tonic::Status> {
        // If the cursor is _after_ the last ingested block, it's out of range because eventually
        // it will become available.
        match self
            .get_canonical(cursor.number)
            .await
            .map_err(|_| tonic::Status::internal("internal server error"))?
        {
            CanonicalCursor::AfterAvailable(last) => Err(tonic::Status::out_of_range(format!(
                "cursor {} is after the last ingested block {}",
                cursor.number, last.number
            ))),
            CanonicalCursor::BeforeAvailable(first) => {
                Err(tonic::Status::invalid_argument(format!(
                    "cursor {} is before the first ingested block {}",
                    cursor.number, first.number
                )))
            }
            CanonicalCursor::Canonical(_) => Ok(()),
        }
    }
}

fn validate_heartbeat_interval(
    heartbeat_interval: Option<Duration>,
) -> tonic::Result<Duration, tonic::Status> {
    let Some(heartbeat_interval) = heartbeat_interval else {
        return Ok(Duration::from_secs(30));
    };

    if heartbeat_interval < Duration::from_secs(10) {
        Err(tonic::Status::invalid_argument(format!(
            "heartbeat interval must be at least 10 seconds, got {}",
            heartbeat_interval.as_secs()
        )))
    } else if heartbeat_interval > Duration::from_secs(60) {
        Err(tonic::Status::invalid_argument(format!(
            "heartbeat interval must be at most 60 seconds, got {}",
            heartbeat_interval.as_secs()
        )))
    } else {
        Ok(heartbeat_interval)
    }
}
