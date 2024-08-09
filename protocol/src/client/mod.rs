//! An opinionated client for the DNA API.
//!
//! This client adds features that make interacting with the DNA API easier.
//!
//! - Authentication with bearer token.
//! - Add a timeout to the stream.
mod builder;
mod error;
mod interceptor;
mod stream_client;

pub use self::builder::StreamClientBuilder;
pub use self::error::StreamClientError;
pub use self::interceptor::{MetadataInterceptor, MetadataKey, MetadataValue};
pub use self::stream_client::{DataStream, DataStreamError, StreamClient, StreamMessage};
