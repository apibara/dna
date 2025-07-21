//! HTTP client for pushing messages to Wings.

use error_stack::{Result, ResultExt, report};
use reqwest::StatusCode;
use serde_json::Value;
use thiserror::Error;
use wings_ingestor_http::types::{Batch, ErrorResponse, PushRequest, PushResponse};
use wings_metadata_core::{admin::NamespaceName, partition::PartitionValue};

/// A client for pushing messages to Wings over HTTP.
#[derive(Debug, Clone)]
pub struct HttpPushClient {
    client: reqwest::Client,
    base_url: String,
    namespace: NamespaceName,
}

#[derive(Error, Debug, Clone)]
pub enum HttpPushClientError {
    #[error("client error")]
    Client,
    #[error("response error: {0} {1}")]
    Response(StatusCode, String),
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
    pub fn push(&self) -> PushRequestBuilder {
        PushRequestBuilder::new(
            self.client.clone(),
            self.base_url.clone(),
            self.namespace.clone(),
        )
    }
}

/// Builder for constructing push requests.
#[derive(Debug)]
pub struct PushRequestBuilder {
    client: reqwest::Client,
    base_url: String,
    namespace: NamespaceName,
    batches: Vec<Batch>,
}

#[derive(Debug)]
pub struct TopicRequestBuilder {
    topic: String,
    push: PushRequestBuilder,
}

impl PushRequestBuilder {
    fn new(client: reqwest::Client, base_url: String, namespace: NamespaceName) -> Self {
        Self {
            client,
            base_url,
            namespace,
            batches: Vec::new(),
        }
    }

    pub fn topic(self, topic: String) -> TopicRequestBuilder {
        TopicRequestBuilder { push: self, topic }
    }

    /// Send the push request to the server.
    pub async fn send(self) -> Result<PushResponse, HttpPushClientError> {
        let request = PushRequest {
            namespace: self.namespace.to_string(),
            batches: self.batches,
        };

        let url = format!("{}/v1/push", self.base_url);

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .change_context(HttpPushClientError::Client)?;

        if response.status().is_success() {
            return response
                .json::<PushResponse>()
                .await
                .change_context(HttpPushClientError::Client);
        }

        let status = response.status();
        let body = response
            .json::<ErrorResponse>()
            .await
            .change_context(HttpPushClientError::Client)?;

        Err(report!(HttpPushClientError::Response(status, body.message)))
    }

    fn add_batch(&mut self, batch: Batch) {
        self.batches.push(batch);
    }
}

impl TopicRequestBuilder {
    pub fn partitioned(
        mut self,
        partition_value: PartitionValue,
        data: Vec<Value>,
    ) -> PushRequestBuilder {
        let batch = Batch {
            topic: self.topic,
            partition: Some(partition_value),
            data,
        };
        self.push.add_batch(batch);
        self.push
    }

    pub fn unpartitioned(mut self, data: Vec<Value>) -> PushRequestBuilder {
        let batch = Batch {
            topic: self.topic,
            partition: None,
            data,
        };
        self.push.add_batch(batch);
        self.push
    }
}
