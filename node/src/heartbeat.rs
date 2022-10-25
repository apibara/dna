//! Add heartbeat to streams.

use std::{
    fmt,
    pin::Pin,
    task::{self, Poll},
    time::Duration,
};

use futures::{stream::Fuse, Future, StreamExt};
use pin_project::pin_project;
use tokio::time::{Instant, Sleep};
use tokio_stream::Stream;

pub trait HeartbeatStreamExt: Stream {
    /// Returns a new stream that contains heartbeat messages.
    ///
    /// Heartbeat messages are produced if the original stream
    /// doesn't produce any message for `interval` time.
    /// The heartbeat interval is reset every time the stream
    /// produces a new message.
    fn heartbeat(self, interval: Duration) -> Heartbeat<Self>
    where
        Self: Sized,
    {
        Heartbeat::new(self, interval)
    }
}

impl<St: ?Sized> HeartbeatStreamExt for St where St: Stream {}

#[pin_project]
#[must_use = "streams do nothing unless polled"]
#[derive(Debug)]
/// Stream returned by [`heartbeat`](HeartbeatStreamExt::timeout).
pub struct Heartbeat<S> {
    #[pin]
    stream: Fuse<S>,
    #[pin]
    deadline: Sleep,
    interval: Duration,
    needs_reset: bool,
}

impl<S: Stream> Heartbeat<S> {
    pub fn new(stream: S, interval: Duration) -> Heartbeat<S> {
        let stream = stream.fuse();
        let next = Instant::now() + interval;
        let deadline = tokio::time::sleep_until(next);

        Heartbeat {
            stream,
            deadline,
            interval,
            needs_reset: true,
        }
    }
}

impl<S: Stream> Stream for Heartbeat<S> {
    type Item = Result<S::Item, Beat>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if *this.needs_reset {
            let next = Instant::now() + *this.interval;
            this.deadline.reset(next);
            *this.needs_reset = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        match this.stream.poll_next(cx) {
            Poll::Ready(v) => {
                if v.is_some() {
                    *this.needs_reset = true;
                }
                return Poll::Ready(v.map(Ok));
            }
            Poll::Pending => {}
        }

        match this.deadline.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => {
                *this.needs_reset = true;
                Poll::Ready(Some(Err(Beat::new())))
            }
        }
    }
}

/// Error returned by `Heartbeat`.
#[derive(Debug, PartialEq)]
pub struct Beat(());

impl Beat {
    pub(crate) fn new() -> Self {
        Beat(())
    }
}

impl fmt::Display for Beat {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        "heartbeat deadline has elapsed".fmt(fmt)
    }
}

impl std::error::Error for Beat {}

impl From<Beat> for std::io::Error {
    fn from(_err: Beat) -> std::io::Error {
        std::io::ErrorKind::TimedOut.into()
    }
}
