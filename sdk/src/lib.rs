//! # Apibara Rust SDK
//!
//! This crate contains the types to implement an Apibara application in Rust.
pub use apibara_core::stream;
pub use apibara_node::application;
pub use apibara_node::async_trait;
pub use apibara_node::db;
pub use apibara_node::node;

/// Contains protobuf definitions.
pub mod pb {
    pub use apibara_core::application::pb as application;
    pub use apibara_core::node::v1alpha1::pb as node;
}
