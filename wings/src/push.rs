use arrow_schema::DataType;
use clap::Parser;
use error_stack::{ResultExt, report};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use wings_ingestor_http::types::BatchResponse;
use wings_metadata_core::{
    admin::{Admin, NamespaceName, RemoteAdminService, TopicName},
    partition::PartitionValue,
};

use crate::{
    error::{CliError, CliResult},
    http_client::{HttpPushClient, PushRequestBuilder},
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
    pub async fn run(self, _ct: CancellationToken) -> CliResult<()> {
        let admin = self.remote.admin_client().await?;

        let namespace_name = NamespaceName::parse(&self.namespace).change_context(
            CliError::InvalidConfiguration {
                message: "invalid namespace name".to_string(),
            },
        )?;

        let client = HttpPushClient::new(&self.http_address, namespace_name.clone());

        // let batches = self.parse_batches()?;
        let request = self
            .parse_batches_to_request(namespace_name, &admin, &client)
            .await?;

        let response = request.send().await.change_context(CliError::Server {
            message: "failed to push data".to_string(),
        })?;

        for batch in response.batches {
            match batch {
                BatchResponse::Success {
                    start_offset,
                    end_offset,
                } => {
                    println!("SUCCESS {start_offset} - {end_offset}");
                }
                BatchResponse::Error { message } => {
                    println!("ERROR: {message}");
                }
            }
        }

        Ok(())
    }

    async fn parse_batches_to_request(
        &self,
        namespace_name: NamespaceName,
        admin: &RemoteAdminService<Channel>,
        client: &HttpPushClient,
    ) -> CliResult<PushRequestBuilder> {
        let mut i = 0;

        let mut request = client.push();
        while i < self.batches.len() {
            let remaining = self.batches.len() - i;

            if remaining < 2 {
                return Err(report!(crate::error::CliError::InvalidArguments)
                    .attach_printable("Each batch requires at least topic_id and payload"));
            }

            let topic_id = &self.batches[i];
            let topic_name = TopicName::new(topic_id, namespace_name.clone())
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("invalid topic name: {}", topic_id))?;

            let topic = admin
                .get_topic(topic_name)
                .await
                .change_context(CliError::AdminApi {
                    message: "failed to get topic".to_string(),
                })?;

            let partition_column = topic.partition_column();

            let next_arg = &self.batches[i + 1];

            // Check what's the next argument based on whether the topic has a partition column
            let (partition_value, payload_index) = if let Some(partition_column) = partition_column
            {
                if remaining >= 3 {
                    // Next arg is partition value, followed by payload
                    let partition_value =
                        convert_partition_value(next_arg, partition_column.data_type())
                            .change_context(CliError::InvalidArguments)?;
                    (Some(partition_value), i + 2)
                } else {
                    return Err(report!(crate::error::CliError::InvalidArguments)
                        .attach_printable("Missing payload after partition value"));
                }
            } else {
                (None, i + 1)
            };

            if payload_index >= self.batches.len() {
                return Err(report!(crate::error::CliError::InvalidArguments)
                    .attach_printable("Missing payload"));
            }

            let payload_str = &self.batches[payload_index];
            let messages = self.parse_payload(payload_str)?;

            let topic_request = request.topic(topic_id.clone());
            request = if let Some(partition_value) = partition_value {
                topic_request.partitioned(partition_value, messages)
            } else {
                topic_request.unpartitioned(messages)
            };

            i = payload_index + 1;
        }

        Ok(request)
    }

    fn parse_payload(&self, payload_str: &str) -> CliResult<Vec<Value>> {
        if let Some(file_path) = payload_str.strip_prefix('@') {
            // File path mode
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

/// Convert a string partition value to the appropriate type based on the Arrow DataType
pub fn convert_partition_value(value: &str, data_type: &DataType) -> CliResult<PartitionValue> {
    match data_type {
        DataType::UInt8 => {
            let parsed = value
                .parse::<u8>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid u8 partition value: {value}"))?;
            Ok(PartitionValue::UInt8(parsed))
        }
        DataType::UInt16 => {
            let parsed = value
                .parse::<u16>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid u16 partition value: {value}"))?;
            Ok(PartitionValue::UInt16(parsed))
        }
        DataType::UInt32 => {
            let parsed = value
                .parse::<u32>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid u32 partition value: {value}"))?;
            Ok(PartitionValue::UInt32(parsed))
        }
        DataType::UInt64 => {
            let parsed = value
                .parse::<u64>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid u64 partition value: {value}"))?;
            Ok(PartitionValue::UInt64(parsed))
        }
        DataType::Int8 => {
            let parsed = value
                .parse::<i8>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid i8 partition value: {value}"))?;
            Ok(PartitionValue::Int8(parsed))
        }
        DataType::Int16 => {
            let parsed = value
                .parse::<i16>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid i16 partition value: {value}"))?;
            Ok(PartitionValue::Int16(parsed))
        }
        DataType::Int32 => {
            let parsed = value
                .parse::<i32>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid i32 partition value: {value}"))?;
            Ok(PartitionValue::Int32(parsed))
        }
        DataType::Int64 => {
            let parsed = value
                .parse::<i64>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid i64 partition value: {value}"))?;
            Ok(PartitionValue::Int64(parsed))
        }
        DataType::Utf8 | DataType::LargeUtf8 => Ok(PartitionValue::String(value.to_string())),
        DataType::Binary | DataType::LargeBinary => {
            // Handle hex encoding for binary data
            if let Some(stripped) = value.strip_prefix("0x") {
                let bytes = hex::decode(stripped)
                    .change_context(CliError::InvalidArguments)
                    .attach_printable_lazy(|| format!("Invalid hex partition value: {value}"))?;
                Ok(PartitionValue::Bytes(bytes))
            } else {
                let bytes = hex::decode(value)
                    .change_context(CliError::InvalidArguments)
                    .attach_printable_lazy(|| format!("Invalid hex partition value: {value}"))?;
                Ok(PartitionValue::Bytes(bytes))
            }
        }
        _ => Err(CliError::InvalidArguments)
            .attach_printable_lazy(|| format!("Unsupported partition key type: {data_type:?}")),
    }
}
