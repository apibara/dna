use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll, Waker},
};

use apibara_core::ethereum::v1alpha2::Filter;
use apibara_node::stream::{
    IngestionMessage, StreamConfiguration, StreamError,
};
use futures::Stream;

use crate::{access::Erigon, erigon::GlobalBlockId};

pub struct FilteredDataStream {
    erigon: Erigon,
}

impl FilteredDataStream {
    pub fn new(erigon: Erigon) -> Self {
        FilteredDataStream { erigon }
    }
}

impl Stream for FilteredDataStream {
    type Item = Result<apibara_core::node::v1alpha2::StreamDataResponse, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
