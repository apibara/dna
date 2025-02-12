use std::{
    future::Future,
    task::{self, Poll},
    time::Instant,
};

use futures::TryFuture;

#[derive(Debug, Clone)]
pub struct RequestMetrics {
    pub duration: crate::Histogram<f64>,
    pub error: crate::Counter<u64>,
}

impl RequestMetrics {
    pub fn new(meter_name: &'static str, metric_name: &'static str) -> Self {
        let meter = crate::meter(meter_name);

        Self {
            duration: meter
                .f64_histogram(format!("{metric_name}.duration"))
                .with_description(format!("{metric_name} duration"))
                .with_unit("s")
                .with_boundaries(vec![
                    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5,
                    10.0, 20.0, 30.0, 60.0, 120.0,
                ])
                .build(),
            error: meter
                .u64_counter(format!("{metric_name}.error"))
                .with_description(format!("{metric_name} error count"))
                .build(),
        }
    }
}

#[pin_project::pin_project]
pub struct RecordedRequest<T> {
    #[pin]
    inner: T,
    start: Instant,
    metrics: RequestMetrics,
    attributes: Vec<crate::KeyValue>,
}

pub trait RecordRequest: Sized {
    fn record_request(self, metrics: RequestMetrics) -> RecordedRequest<Self> {
        self.record_request_with_attributes(metrics, &[])
    }

    fn record_request_with_attributes(
        self,
        metrics: RequestMetrics,
        attributes: &[crate::KeyValue],
    ) -> RecordedRequest<Self> {
        RecordedRequest {
            inner: self,
            start: Instant::now(),
            metrics,
            attributes: attributes.to_vec(),
        }
    }
}

impl<T: Sized> RecordRequest for T {}

impl<T: TryFuture> Future for RecordedRequest<T> {
    type Output = Result<T::Ok, T::Error>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.inner.try_poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(output) => {
                this.metrics
                    .duration
                    .record(this.start.elapsed().as_secs_f64(), this.attributes);

                if output.is_err() {
                    this.metrics.error.add(1, this.attributes);
                }

                Poll::Ready(output)
            }
        }
    }
}
