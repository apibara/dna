//! # Apibara SDK
//!
//! This crate is the official SDK for [Apibara](https://www.apibara.com).
//! Apibara is the fastest way to build production-grade web3 indexers.
//!
//! The SDK provides the following features:
//!
//!  - A client to stream onchain data. The client handles authentication and connection timeouts.
//!  - Chain-specific filter builders and data parsers.
//!
//! ## Features
//!
//! By default, the SDK is built without any chain support. To enable support for a chain, you need to enable the corresponding feature.
//!
//! - `starknet`: enable Starknet support.
//!
//! ## Authentication
//!
//! If you're connected to a protected endpoint (like the ones hosted by Apibara), you need to provide a valid API key.
//!
//! The main mechanism to provide authentication is through the [BearerTokenProvider] trait. We provide two implementations:
//!
//!  - [BearerTokenFromEnv]: reads the API key from the `DNA_TOKEN` environment variable.
//!  - [StaticBearerToken]: uses the API key provided at construction.
//!
//! You are free to implement your own bearer token provider, for example one that fetches the token from a remote vault server.
//!
//! ## Connection issues
//!
//! The client does not reconnect automatically. If the connection is lost, you need to create a new client.
//! This is done to prevent your application not processing a block or processing the same block twice.
//! By handling reconnection within your application, you can decide whether you want to process blocks
//! at least or at most once.

mod auth;
mod builder;
mod client;
mod interceptor;
mod request;
#[cfg(feature = "starknet")]
pub mod starknet;

pub use crate::auth::*;
pub use crate::builder::*;
pub use crate::client::*;
pub use crate::interceptor::{InvalidMetadataValue, MetadataKey, MetadataMap, MetadataValue};
pub use crate::request::StreamDataRequestBuilder;

/// Contains the gRPC types to interact with the Apibara DNA service.
pub use apibara_dna_protocol::dna::stream as proto;
