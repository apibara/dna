use clap::Parser;
use error_stack::{ResultExt, report};
use serde_json::Value;
use wings_metadata_core::admin::{Admin, NamespaceName};

use crate::{
    error::{CliError, CliResult},
    http_client::HttpPushClient,
    remote::RemoteArgs,
};

/// Push messages to Wings topics
#[derive(Parser)]
pub struct PushArgs {
    /// Namespace name
    namespace: String,

    /// Batches to push in the format: <topic_id> [<partition_value>] <payload>
    ///
    /// - topic_id: required, used to construct the topic name by joining with namespace
    /// - partition_value: optional, value for the partition key
    /// - payload: required, JSON payload or @file_path for file containing JSON messages
    batches: Vec<String>,

    /// HTTP ingestor address.
    ///
    /// The address where the Wings HTTP ingestor is running. Should match
    /// the address used in the 'wings dev' command.
    #[arg(long, default_value = "http://127.0.0.1:7780")]
    http_address: String,
    #[clap(flatten)]
    remote: RemoteArgs,
}

impl PushArgs {
    pub async fn run(self, _ct: tokio_util::sync::CancellationToken) -> CliResult<()> {
        let admin = self.remote.admin_client().await?;
        let batches = self.parse_batches()?;

        let namespace_name = NamespaceName::parse(&self.namespace).change_context(
            CliError::InvalidConfiguration {
                message: "invalid namespace name".to_string(),
            },
        )?;

        let client = HttpPushClient::new(self.http_address, namespace_name);

        let mut request = client.push();
        for batch in batches.into_iter() {
            let topic_request = request.topic(batch.topic);
            request = if let Some(partition_value) = batch.partition_value {
                todo!()
            } else {
                topic_request.unpartitioned(batch.messages)
            };
        }

        let response = request.send().await.change_context(CliError::Server {
            message: "failed to push data".to_string(),
        })?;

        println!("XX {:?}", response);

        Ok(())
    }

    fn parse_batches(&self) -> CliResult<Vec<Batch>> {
        let mut batches = Vec::new();
        let mut i = 0;

        while i < self.batches.len() {
            let remaining = self.batches.len() - i;

            if remaining < 2 {
                return Err(report!(crate::error::CliError::InvalidArguments)
                    .attach_printable("Each batch requires at least topic_id and payload"));
            }

            let topic = &self.batches[i];
            let next_arg = &self.batches[i + 1];

            // Check if next_arg is a partition value or payload
            let (partition_value, payload_index) = if next_arg.starts_with('@')
                || next_arg.starts_with('{')
                || next_arg.starts_with('[')
            {
                // Next arg is payload, no partition value provided
                (None, i + 1)
            } else if remaining >= 3 {
                // Next arg is partition value, followed by payload
                (Some(next_arg.clone()), i + 2)
            } else {
                return Err(report!(crate::error::CliError::InvalidArguments)
                    .attach_printable("Missing payload after partition value"));
            };

            if payload_index >= self.batches.len() {
                return Err(report!(crate::error::CliError::InvalidArguments)
                    .attach_printable("Missing payload"));
            }

            let payload_str = &self.batches[payload_index];
            let messages = self.parse_payload(payload_str)?;

            batches.push(Batch {
                topic: topic.clone(),
                partition_value,
                messages,
            });

            i = payload_index + 1;
        }

        Ok(batches)
    }

    fn parse_payload(&self, payload_str: &str) -> CliResult<Vec<Value>> {
        if payload_str.starts_with('@') {
            // File path mode
            let file_path = &payload_str[1..];
            let content = std::fs::read_to_string(file_path)
                .change_context(crate::error::CliError::IoError)
                .attach_printable_lazy(|| format!("Failed to read file: {}", file_path))?;

            let mut messages = Vec::new();
            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                let json: Value = serde_json::from_str(line)
                    .change_context(crate::error::CliError::InvalidJson)
                    .attach_printable_lazy(|| {
                        format!("Invalid JSON in file {}: {}", file_path, line)
                    })?;

                messages.push(json);
            }

            Ok(messages)
        } else {
            // Direct JSON mode
            let json: Value = serde_json::from_str(payload_str)
                .change_context(crate::error::CliError::InvalidJson)
                .attach_printable_lazy(|| format!("Invalid JSON: {}", payload_str))?;

            Ok(vec![json])
        }
    }
}

#[derive(Debug)]
struct Batch {
    topic: String,
    partition_value: Option<String>,
    messages: Vec<Value>,
}
