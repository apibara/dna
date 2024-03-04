use apibara_dna_protocol::dna::Cursor;
use async_trait::async_trait;
use error_stack::Result;
use prost::Message;
use serde::{Deserialize, Serialize};

use crate::{filter::Filter, SinkError};

#[derive(Clone, PartialEq, Message, Deserialize, Serialize)]
pub struct PersistedState<F: Message + Default> {
    #[prost(message, tag = "1")]
    pub cursor: Option<Cursor>,
    #[prost(message, tag = "2")]
    pub filter: Option<F>,
}

/// Client used to interact with the persistence backend.
#[async_trait]
pub trait PersistenceClient {
    /// Attempts to acquire a lock on the sink.
    async fn lock(&mut self) -> Result<(), SinkError>;

    /// Unlock the previously acquired lock.
    async fn unlock(&mut self) -> Result<(), SinkError>;

    /// Reads the currently stored state.
    async fn get_state<F: Filter>(&mut self) -> Result<PersistedState<F>, SinkError>;

    /// Updates the sink state.
    async fn put_state<F: Filter>(&mut self, state: PersistedState<F>) -> Result<(), SinkError>;

    /// Deletes any stored sink state.
    async fn delete_state(&mut self) -> Result<(), SinkError>;
}

impl<F: Message + Default> PersistedState<F> {
    pub fn with_cursor(cursor: Cursor) -> Self {
        Self {
            cursor: Some(cursor),
            filter: None,
        }
    }

    pub fn new(cursor: Option<Cursor>, filter: Option<F>) -> Self {
        Self { cursor, filter }
    }
}

#[async_trait]
impl<P> PersistenceClient for Box<P>
where
    P: PersistenceClient + ?Sized + Send,
{
    async fn lock(&mut self) -> Result<(), SinkError> {
        (**self).lock().await
    }

    async fn unlock(&mut self) -> Result<(), SinkError> {
        (**self).unlock().await
    }

    async fn get_state<F: Filter>(&mut self) -> Result<PersistedState<F>, SinkError> {
        (**self).get_state().await
    }

    async fn put_state<F: Filter>(&mut self, state: PersistedState<F>) -> Result<(), SinkError> {
        (**self).put_state(state).await
    }

    async fn delete_state(&mut self) -> Result<(), SinkError> {
        (**self).delete_state().await
    }
}
