use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use apibara_dna_protocol::dna::stream::{stream_data_response, StreamDataResponse};
use futures::Stream;
use tokio::{sync::mpsc, time::Interval};

pub struct ResponseStreamWithHeartbeat {
    rx: mpsc::Receiver<Result<StreamDataResponse, tonic::Status>>,
    interval: Interval,
}

impl ResponseStreamWithHeartbeat {
    pub fn new(
        rx: mpsc::Receiver<Result<StreamDataResponse, tonic::Status>>,
        heartbeat_interval: Duration,
    ) -> Self {
        let mut interval = tokio::time::interval(heartbeat_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.reset();

        Self { rx, interval }
    }
}

impl Stream for ResponseStreamWithHeartbeat {
    type Item = Result<StreamDataResponse, tonic::Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(data) = self.rx.poll_recv(cx) {
            self.interval.reset();
            return Poll::Ready(data);
        }

        if self.interval.poll_tick(cx).is_ready() {
            let message = StreamDataResponse {
                message: Some(stream_data_response::Message::Heartbeat(Default::default())),
            };

            return Poll::Ready(Some(Ok(message)));
        }

        Poll::Pending
    }
}
