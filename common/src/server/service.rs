use std::{pin::Pin, sync::Arc, time::Duration};

use apibara_dna_protocol::dna::stream::{
    dna_stream_server::{self, DnaStream},
    DataFinality, StatusRequest, StatusResponse, StreamDataRequest, StreamDataResponse,
};
use error_stack::Result;
use futures::{Future, Stream, StreamExt};
use tokio::sync::{mpsc, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    chain_view::{CanonicalCursor, ChainView, ChainViewError},
    data_stream::{DataStream, ScannerFactory},
    Cursor,
};

const CHANNEL_SIZE: usize = 1024;

static STREAM_SEMAPHORE_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct StreamServiceOptions {
    /// Maximum number of concurrent streams.
    pub max_concurrent_streams: usize,
}

pub struct StreamService<SF>
where
    SF: ScannerFactory,
{
    scanner_factory: SF,
    stream_semaphore: Arc<Semaphore>,
    chain_view: tokio::sync::watch::Receiver<Option<ChainView>>,
    ct: CancellationToken,
}

impl<SF> StreamService<SF>
where
    SF: ScannerFactory,
{
    pub fn new(
        scanner_factory: SF,
        chain_view: tokio::sync::watch::Receiver<Option<ChainView>>,
        options: StreamServiceOptions,
        ct: CancellationToken,
    ) -> Self {
        let stream_semaphore = Arc::new(Semaphore::new(options.max_concurrent_streams));
        Self {
            scanner_factory,
            stream_semaphore,
            chain_view,
            ct,
        }
    }

    pub fn into_service(self) -> dna_stream_server::DnaStreamServer<Self> {
        dna_stream_server::DnaStreamServer::new(self)
    }
}

#[tonic::async_trait]
impl<SF> DnaStream for StreamService<SF>
where
    SF: ScannerFactory + Send + Sync + 'static,
{
    type StreamDataStream = Pin<
        Box<dyn Stream<Item = tonic::Result<StreamDataResponse, tonic::Status>> + Send + 'static>,
    >;

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

    async fn stream_data(
        &self,
        request: tonic::Request<StreamDataRequest>,
    ) -> tonic::Result<tonic::Response<Self::StreamDataStream>, tonic::Status> {
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

        // Validate starting cursor by checking it's in range.
        // The block could be reorged but that's handled by the `DataStream`.
        let starting_cursor = if let Some(cursor) = request.starting_cursor {
            let cursor = Cursor::from(cursor);
            chain_view.ensure_cursor_in_range(&cursor).await?;
            cursor.into()
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
        let scanner = self.scanner_factory.create_scanner(&request.filter)?;

        let ds = DataStream::new(
            scanner,
            starting_cursor,
            finalized,
            finality,
            heartbeat_interval,
            chain_view,
            permit,
        );
        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);

        tokio::spawn(ds.start(tx, self.ct.clone()));

        let stream = ReceiverStream::new(rx).boxed();

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
