use clap::Parser;
use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_metadata_core::{
    admin::{Admin, TopicName},
    offset_registry::{OffsetLocation, OffsetRegistry},
};
use wings_object_store::{LocalFileSystemFactory, ObjectStoreFactory};

use crate::{
    error::{CliError, CliResult},
    push::convert_partition_value,
    remote::RemoteArgs,
};

/// Debug the content of a topic
#[derive(Parser)]
pub struct DebugDataArgs {
    /// Topic to debug
    topic: String,

    /// Message offset.
    offset: u64,

    /// Partition value.
    #[arg(long)]
    partition: Option<String>,

    /// Output file path.
    #[arg(long)]
    output: String,

    /// Base path for the object storage.
    #[arg(long)]
    base_path: String,

    #[clap(flatten)]
    remote: RemoteArgs,
}

impl DebugDataArgs {
    pub async fn run(self, _ct: CancellationToken) -> CliResult<()> {
        let admin = self.remote.admin_client().await?;
        let offset_registry = self.remote.offset_registry_client().await?;

        let object_store_factory = LocalFileSystemFactory::new(self.base_path).change_context(
            CliError::InvalidConfiguration {
                message: "invalid base path".to_string(),
            },
        )?;

        let topic_name =
            TopicName::parse(&self.topic).change_context(CliError::InvalidConfiguration {
                message: "invalid topic name".to_string(),
            })?;

        let topic =
            admin
                .get_topic(topic_name.clone())
                .await
                .change_context(CliError::AdminApi {
                    message: "failed to get topic".to_string(),
                })?;

        let namespace = admin
            .get_namespace(topic_name.parent.clone())
            .await
            .change_context(CliError::AdminApi {
                message: "failed to get namespace".to_string(),
            })?;

        let object_store = object_store_factory
            .create_object_store(namespace.default_object_store_config)
            .await
            .change_context(CliError::ObjectStore {
                message: "failed to create object store".to_string(),
            })?;

        let partition_value = match (self.partition, topic.partition_column()) {
            (None, None) => None,
            (Some(value), Some(column)) => {
                let value = convert_partition_value(&value, column.data_type())
                    .change_context(CliError::InvalidArguments)?;
                Some(value)
            }
            (None, Some(_)) => {
                return Err(CliError::InvalidArguments)
                    .attach_printable("missing required partition value")?;
            }
            (Some(_), None) => {
                return Err(CliError::InvalidArguments)
                    .attach_printable("unexpected partition value")?;
            }
        };

        let offset_location = offset_registry
            .offset_location(topic_name, partition_value, self.offset)
            .await
            .change_context(CliError::AdminApi {
                message: "failed to get offset location".to_string(),
            })?;

        match offset_location {
            OffsetLocation::Folio(location) => {
                println!("Fetching {}", location.file_ref);
                let response = object_store
                    .get(&location.file_ref.into())
                    .await
                    .change_context(CliError::ObjectStore {
                        message: "failed to get local file".to_string(),
                    })?;

                let bytes = response
                    .bytes()
                    .await
                    .change_context(CliError::ObjectStore {
                        message: "failed to read local file".to_string(),
                    })?;

                println!(
                    "Writing data for offset range {}-{}",
                    location.start_offset, location.end_offset
                );

                let parquet_start = location.offset_bytes as usize;
                let parquet_end = parquet_start + location.size_bytes as usize;

                println!("Offset in file {}-{}", parquet_start, parquet_end);

                std::fs::write(&self.output, &bytes[parquet_start..parquet_end]).change_context(
                    CliError::ObjectStore {
                        message: "failed to write parquet file".to_string(),
                    },
                )?;

                println!("Data written to {}", self.output);
            }
        }

        Ok(())
    }
}
