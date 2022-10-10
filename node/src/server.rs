//! Stream gRPC server.

use std::{marker::PhantomData, sync::Arc};

use apibara_core::stream::MessageData;

use crate::message_storage::MessageStorage;

pub struct Server<M, S>
where
    M: MessageData,
    S: MessageStorage<M>,
{
    storage: Arc<S>,
    _phantom: PhantomData<M>,
}

impl<M, S> Server<M, S>
where
    M: MessageData,
    S: MessageStorage<M>,
{
    /// Creates a new stream server.
    pub fn new(storage: Arc<S>) -> Self {
        Server {
            storage,
            _phantom: PhantomData::default(),
        }
    }

    pub async fn start(self) {}
}
