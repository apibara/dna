use apibara_core::node::v1alpha2::Cursor;
use async_trait::async_trait;
use error_stack::Result;
use prost::Message;
use serde::{Deserialize, Serialize};

use crate::SinkError;

#[derive(Clone, PartialEq, Message, Deserialize, Serialize)]
pub struct PersistedState {
    #[prost(message, tag = "1")]
    pub cursor: Option<Cursor>,
}

/// Client used to interact with the persistence backend.
#[async_trait]
pub trait PersistenceClient {
    /// Attempts to acquire a lock on the sink.
    async fn lock(&mut self) -> Result<(), SinkError>;

    /// Unlock the previously acquired lock.
    async fn unlock(&mut self) -> Result<(), SinkError>;

    /// Reads the currently stored state.
    async fn get_state(&mut self) -> Result<PersistedState, SinkError>;

    /// Updates the sink state.
    async fn put_state(&mut self, state: PersistedState) -> Result<(), SinkError>;

    /// Deletes any stored sink state.
    async fn delete_state(&mut self) -> Result<(), SinkError>;
}

impl PersistedState {
    pub fn with_cursor(cursor: Cursor) -> Self {
        Self {
            cursor: Some(cursor),
        }
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

    async fn get_state(&mut self) -> Result<PersistedState, SinkError> {
        (**self).get_state().await
    }

    async fn put_state(&mut self, state: PersistedState) -> Result<(), SinkError> {
        (**self).put_state(state).await
    }

    async fn delete_state(&mut self) -> Result<(), SinkError> {
        (**self).delete_state().await
    }
}
