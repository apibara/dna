//! Request and response types for the HTTP ingestor push endpoint.

use serde::{Deserialize, Serialize};
use wings_metadata_core::partition::PartitionValue;

/// Request payload for the /v1/push endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushRequest {
    /// The namespace to push data to.
    pub namespace: String,
    /// List of batches to push.
    pub batches: Vec<Batch>,
}

/// A batch of data for a specific topic and partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    /// The topic to push data to.
    pub topic: String,
    /// Optional partition value for the batch.
    /// If None, the data will be partitioned based on the topic's partitioning strategy.
    pub partition: Option<PartitionValue>,
    /// List of JSON objects representing the data to push.
    /// Each object will be converted to an Arrow RecordBatch.
    pub data: Vec<serde_json::Value>,
}

/// Response payload for the /v1/push endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PushResponse {
    pub batches: Vec<BatchResponse>,
}

/// Response payload for errors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ErrorResponse {
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "_tag")]
pub enum BatchResponse {
    #[serde(rename = "success")]
    Success { start_offset: u64, end_offset: u64 },
    #[serde(rename = "error")]
    Error { message: String },
}
