//! HTTP client for pushing messages to Wings.

use serde_json::Value;
use wings_ingestor_http::types::{Batch, PushRequest, PushResponse};
use wings_metadata_core::{
    admin::{NamespaceName, TopicName},
    partition::PartitionValue,
};

/// A client for pushing messages to Wings over HTTP.
#[derive(Debug, Clone)]
pub struct HttpPushClient {
    client: reqwest::Client,
    base_url: String,
    namespace: NamespaceName,
}

impl HttpPushClient {
    /// Create a new HTTP push client.
    pub fn new(base_url: impl Into<String>, namespace: NamespaceName) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into(),
            namespace,
        }
    }

    /// Start building a push request for the given topic.
    pub fn push(&self, topic: TopicName) -> PushRequestBuilder {
        PushRequestBuilder::new(
            self.client.clone(),
            self.base_url.clone(),
            self.namespace.clone(),
            topic,
        )
    }
}

/// Builder for constructing push requests.
#[derive(Debug)]
pub struct PushRequestBuilder {
    client: reqwest::Client,
    base_url: String,
    namespace: NamespaceName,
    topic: TopicName,
    batches: Vec<Batch>,
}

impl PushRequestBuilder {
    fn new(
        client: reqwest::Client,
        base_url: String,
        namespace: NamespaceName,
        topic: TopicName,
    ) -> Self {
        Self {
            client,
            base_url,
            namespace,
            topic,
            batches: Vec::new(),
        }
    }

    /// Add a batch of data for a specific partition.
    pub fn partition(mut self, partition_value: PartitionValue, data: Vec<Value>) -> Self {
        let batch = Batch {
            topic: self.topic.id().to_string(),
            partition: Some(partition_value),
            data,
        };
        self.batches.push(batch);
        self
    }

    /// Add a batch of data without specifying a partition.
    pub fn batch(mut self, data: Vec<Value>) -> Self {
        let batch = Batch {
            topic: self.topic.id().to_string(),
            partition: None,
            data,
        };
        self.batches.push(batch);
        self
    }

    /// Send the push request to the server.
    pub async fn send(self) -> Result<PushResponse, reqwest::Error> {
        let request = PushRequest {
            namespace: self.namespace.to_string(),
            batches: self.batches,
        };

        let url = format!("{}/v1/push", self.base_url);

        self.client
            .post(&url)
            .json(&request)
            .send()
            .await?
            .json::<PushResponse>()
            .await
    }
}
