use std::{marker::PhantomData, time::Duration};

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sdk::{ClientBuilder, Configuration, DataMessage, Uri};
use async_trait::async_trait;
use exponential_backoff::Backoff;
use jrsonnet_evaluator::{apply_tla, val::ArrValue, val::StrValue, ObjValue, State, Val};
use prost::Message;
use serde::ser::Serialize;
use serde_json::{json, Value};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

#[async_trait]
pub trait Sink {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn handle_data(
        &mut self,
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        finality: &DataFinality,
        batch: &Value,
    ) -> Result<(), Self::Error>;

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error>;
}

#[derive(Debug, thiserror::Error)]
pub enum SinkConnectorError {
    #[error(transparent)]
    ClientBuilder(#[from] apibara_sdk::ClientBuilderError),
    #[error("Failed to send configuration")]
    SendConfiguration,
    #[error("Stream error: {0}")]
    Stream(#[from] Box<dyn std::error::Error>),
    #[error("Transform error: {0}")]
    Transform(#[from] jrsonnet_evaluator::Error),
    #[error("Sink error: {0}")]
    Sink(Box<dyn std::error::Error>),
    #[error("Maximum number of retries exceeded")]
    MaximumRetriesExceeded,
}

pub struct Transformer {
    state: State,
    expr: Val,
}

pub struct SinkConnector<F, B>
where
    F: Message + Default + Clone,
    B: Message + Default,
{
    configuration: Configuration<F>,
    stream_url: Uri,
    backoff: Backoff,
    transformer: Option<Transformer>,
    _phantom: PhantomData<B>,
}

impl<F, B> SinkConnector<F, B>
where
    F: Message + Default + Clone,
    B: Message + Default + Serialize,
{
    /// Creates a new connector with the given stream URL.
    pub fn new(
        stream_url: Uri,
        configuration: Configuration<F>,
        transformer: Option<Transformer>,
    ) -> Self {
        let retries = 10;
        let min_delay = Duration::from_secs(1);
        let max_delay = Duration::from_secs(60);
        let backoff = Backoff::new(retries, min_delay, Some(max_delay));

        Self {
            stream_url,
            configuration,
            backoff,
            transformer,
            _phantom: PhantomData::default(),
        }
    }

    /// Start consuming the stream, calling the configured callback for each message.
    pub async fn consume_stream<S>(
        self,
        mut sink: S,
        ct: CancellationToken,
    ) -> Result<(), SinkConnectorError>
    where
        S: Sink,
    {
        debug!("start consume stream");
        let (mut data_stream, data_client) = ClientBuilder::<F, B>::default()
            .connect(self.stream_url.clone())
            .await?;

        debug!(configuration = ?self.configuration, "sending configuration");
        data_client
            .send(self.configuration.clone())
            .await
            .map_err(|_| SinkConnectorError::SendConfiguration)?;

        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    return Ok(())
                }
                maybe_message = data_stream.try_next() => {
                    match maybe_message.map_err(SinkConnectorError::Stream)? {
                        None => {
                            warn!("data stream closed");
                            return Ok(())
                        }
                        Some(message) => {
                            self.handle_message(message, &mut sink, ct.clone()).await?;
                        }
                    }
                }
            }
        }
    }

    async fn handle_message<S>(
        &self,
        message: DataMessage<B>,
        sink: &mut S,
        ct: CancellationToken,
    ) -> Result<(), SinkConnectorError>
    where
        S: Sink,
    {
        match message {
            DataMessage::Data {
                cursor,
                end_cursor,
                finality,
                batch,
            } => {
                debug!(cursor = ?cursor, end_cursor = ?end_cursor, "received data");
                let data = if let Some(transformer) = &self.transformer {
                    let result = transformer.apply_tla(batch)?;
                    json!(result)
                } else {
                    json!(batch)
                };
                for duration in &self.backoff {
                    match sink
                        .handle_data(&cursor, &end_cursor, &finality, &data)
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(err) => {
                            warn!(err = ?err, "handle_data error");
                            if ct.is_cancelled() {
                                return Err(SinkConnectorError::Sink(err.into()));
                            }
                            tokio::time::sleep(duration).await;
                        }
                    }
                }
                Err(SinkConnectorError::MaximumRetriesExceeded)
            }
            DataMessage::Invalidate { cursor } => {
                debug!(cursor = ?cursor, "received invalidate");
                for duration in &self.backoff {
                    match sink.handle_invalidate(&cursor).await {
                        Ok(_) => return Ok(()),
                        Err(err) => {
                            warn!(err = ?err, "handle_invalidate error");
                            if ct.is_cancelled() {
                                return Err(SinkConnectorError::Sink(err.into()));
                            }
                            tokio::time::sleep(duration).await;
                        }
                    }
                }
                Err(SinkConnectorError::MaximumRetriesExceeded)
            }
        }
    }
}

trait ToVal {
    fn to_val(&self) -> Result<Val, SinkConnectorError>;
}

impl ToVal for Value {
    fn to_val(&self) -> Result<Val, SinkConnectorError> {
        match self {
            Value::Null => Ok(Val::Null),
            Value::Bool(b) => Ok(Val::Bool(*b)),
            Value::Number(n) => {
                if let Some(n) = n.as_i64() {
                    Ok(Val::Num(n as f64))
                } else if let Some(n) = n.as_u64() {
                    Ok(Val::Num(n as f64))
                } else if let Some(n) = n.as_f64() {
                    Ok(Val::Num(n))
                } else {
                    panic!("invalid number")
                }
            }
            Value::String(s) => Ok(Val::Str(StrValue::Flat(s.into()))),
            Value::Array(a) => {
                let inner: ArrValue = a
                    .iter()
                    .map(|v| v.to_val())
                    .collect::<Result<Vec<_>, _>>()?
                    .into();
                Ok(Val::Arr(inner))
            }
            Value::Object(o) => {
                let mut builder = ObjValue::builder();
                for (k, v) in o.iter() {
                    let value = v.to_val()?;
                    builder.member(k.into()).value(value)?;
                }
                Ok(Val::Obj(builder.build()))
            }
        }
    }
}

impl Transformer {
    pub fn new(state: State, expr: Val) -> Self {
        Self { state, expr }
    }

    pub fn apply_tla<D: Serialize>(&self, data: D) -> Result<Val, SinkConnectorError> {
        let data = json!(data).to_val()?;
        let result = apply_tla(self.state.clone(), &vec![data], self.expr.clone())?;
        Ok(result)
    }
}
