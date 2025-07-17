use std::time::Duration;

use arrow::datatypes::{DataType, Field};
use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_metadata_core::admin::{
    Admin, ListNamespacesRequest, ListTenantsRequest, ListTopicsRequest, Namespace, NamespaceName,
    NamespaceOptions, SecretName, Tenant, TenantName, Topic, TopicName, TopicOptions,
};

use crate::{
    error::{CliError, CliResult},
    remote::RemoteArgs,
};

#[derive(clap::Subcommand)]
pub enum AdminCommands {
    /// Create a new tenant
    CreateTenant {
        /// Tenant name
        name: String,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// List all tenants
    ListTenants {
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// Create a new namespace
    CreateNamespace {
        /// Namespace name
        namespace: String,
        /// Flush interval in milliseconds
        #[arg(long)]
        flush_millis: Option<u64>,
        /// Flush size in megabytes
        #[arg(long)]
        flush_mib: Option<u64>,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// List namespaces for a tenant
    ListNamespaces {
        /// Tenant name
        tenant: String,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// Create a new topic
    CreateTopic {
        /// Topic name in format 'tenant/namespace/topic'
        name: String,
        /// Comma-separated list of fields in format 'column_name:column_type'
        fields: Vec<String>,
        /// Partition key column name (must be one of the specified fields)
        #[clap(long)]
        partition: Option<String>,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// List topics for a namespace
    ListTopics {
        /// Namespace name in format 'tenant/namespace'
        namespace: String,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// Delete a topic
    DeleteTopic {
        /// Topic name in format 'tenant/namespace/topic'
        name: String,
        /// Force deletion even if topic has data
        #[clap(long)]
        force: bool,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
}

impl AdminCommands {
    pub async fn run(self, _ct: CancellationToken) -> CliResult<()> {
        match self {
            AdminCommands::CreateTenant { name, remote } => {
                let client = remote.admin_client().await?;
                let tenant_name =
                    TenantName::new(name).change_context(CliError::InvalidConfiguration {
                        message: "invalid tenant name".to_string(),
                    })?;

                let tenant = client
                    .create_tenant(tenant_name)
                    .await
                    .change_context(CliError::Remote)?;

                print_tenant(&tenant);

                Ok(())
            }
            AdminCommands::ListTenants { remote } => {
                let client = remote.admin_client().await?;

                let response = client
                    .list_tenants(ListTenantsRequest::default())
                    .await
                    .change_context(CliError::Remote)?;

                for tenant in response.tenants {
                    print_tenant(&tenant);
                }

                Ok(())
            }
            AdminCommands::CreateNamespace {
                namespace,
                flush_millis,
                flush_mib,
                remote,
            } => {
                let client = remote.admin_client().await?;

                let namespace_name = NamespaceName::parse(&namespace).change_context(
                    CliError::InvalidConfiguration {
                        message: format!("invalid namespace name: {namespace}"),
                    },
                )?;

                let secret_name = SecretName::new_unchecked("default-bucket");

                let mut options = NamespaceOptions::new(secret_name);

                if let Some(millis) = flush_millis {
                    options.flush_interval = Duration::from_millis(millis);
                }

                if let Some(mib) = flush_mib {
                    options.flush_size = bytesize::ByteSize::mib(mib);
                }

                let namespace = client
                    .create_namespace(namespace_name, options)
                    .await
                    .change_context(CliError::Remote)?;

                print_namespace(&namespace);

                Ok(())
            }
            AdminCommands::ListNamespaces { tenant, remote } => {
                let client = remote.admin_client().await?;

                let tenant_name =
                    TenantName::parse(&tenant).change_context(CliError::InvalidConfiguration {
                        message: format!("invalid tenant name: {tenant}"),
                    })?;

                let response = client
                    .list_namespaces(ListNamespacesRequest {
                        parent: tenant_name,
                        page_size: None,
                        page_token: None,
                    })
                    .await
                    .change_context(CliError::Remote)?;

                for namespace in response.namespaces {
                    print_namespace(&namespace);
                }

                Ok(())
            }
            AdminCommands::CreateTopic {
                name,
                fields,
                partition,
                remote,
            } => {
                let client = remote.admin_client().await?;

                // Parse topic name
                let topic_name =
                    TopicName::parse(&name).change_context(CliError::InvalidConfiguration {
                        message: "invalid topic name".to_string(),
                    })?;

                // Parse fields
                let parsed_fields = parse_fields(&fields)?;

                // Validate partition key if provided
                let partition_key = if let Some(partition_column) = partition {
                    let index = parsed_fields
                        .iter()
                        .position(|f| f.name() == &partition_column)
                        .ok_or_else(|| CliError::InvalidConfiguration {
                            message: format!(
                                "partition key column '{}' not found in fields",
                                partition_column
                            ),
                        })?;
                    Some(index)
                } else {
                    None
                };

                let topic_options =
                    TopicOptions::new_with_partition_key(parsed_fields, partition_key);

                let topic = client
                    .create_topic(topic_name, topic_options)
                    .await
                    .change_context(CliError::Remote)?;

                print_topic(&topic);

                Ok(())
            }
            AdminCommands::ListTopics { namespace, remote } => {
                let client = remote.admin_client().await?;

                let namespace_name = NamespaceName::parse(&namespace).change_context(
                    CliError::InvalidConfiguration {
                        message: "invalid namespace name".to_string(),
                    },
                )?;

                let response = client
                    .list_topics(ListTopicsRequest::new(namespace_name))
                    .await
                    .change_context(CliError::Remote)?;

                for topic in response.topics {
                    print_topic(&topic);
                }

                Ok(())
            }
            AdminCommands::DeleteTopic {
                name,
                force,
                remote,
            } => {
                let client = remote.admin_client().await?;

                let topic_name =
                    TopicName::parse(&name).change_context(CliError::InvalidConfiguration {
                        message: "invalid topic name".to_string(),
                    })?;

                client
                    .delete_topic(topic_name, force)
                    .await
                    .change_context(CliError::Remote)?;

                println!("Deleted topic '{}'", name);

                Ok(())
            }
        }
    }
}

/// Parse field specifications from strings like "column_name:column_type"
fn parse_fields(fields: &[String]) -> Result<Vec<Field>, CliError> {
    let mut parsed_fields = Vec::new();

    for field_str in fields {
        let parts: Vec<&str> = field_str.split(':').collect();
        if parts.len() != 2 {
            return Err(CliError::InvalidConfiguration {
                message: format!(
                    "invalid field format '{}'. Expected 'column_name:column_type'",
                    field_str
                ),
            });
        }

        let column_name = parts[0].trim();
        let type_str = parts[1].trim();

        if column_name.is_empty() {
            return Err(CliError::InvalidConfiguration {
                message: "column name cannot be empty".to_string(),
            });
        }

        let data_type = match type_str.to_lowercase().as_str() {
            "int8" | "i8" => DataType::Int8,
            "int16" | "i16" => DataType::Int16,
            "int32" | "i32" => DataType::Int32,
            "int64" | "i64" => DataType::Int64,
            "uint8" | "u8" => DataType::UInt8,
            "uint16" | "u16" => DataType::UInt16,
            "uint32" | "u32" => DataType::UInt32,
            "uint64" | "u64" => DataType::UInt64,
            "float32" | "f32" => DataType::Float32,
            "float64" | "f64" => DataType::Float64,
            "string" | "utf8" => DataType::Utf8,
            "bool" | "boolean" => DataType::Boolean,
            "binary" => DataType::Binary,
            _ => {
                return Err(CliError::InvalidConfiguration {
                    message: format!(
                        "unsupported type '{}'. Supported types: int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, string, bool, binary",
                        type_str
                    ),
                });
            }
        };

        parsed_fields.push(Field::new(column_name, data_type, false));
    }

    Ok(parsed_fields)
}

fn print_tenant(tenant: &Tenant) {
    println!("{}", tenant.name);
}

fn print_namespace(namespace: &Namespace) {
    println!("{}", namespace.name);
    println!("  flush interval: {:?}", namespace.flush_interval);
    println!("  flush size: {}", namespace.flush_size);
    println!(
        "  object store secret: {}",
        namespace.default_object_store_config
    );
    if let Some(ref secret) = namespace.frozen_object_store_config {
        println!("  frozen object store secret: {}", secret);
    }
}

fn print_topic(topic: &Topic) {
    println!("{}", topic.name);
    if let Some(partition_key) = topic.partition_key {
        println!("  partition key: {}", topic.fields[partition_key].name());
    }
    println!("  fields:");
    for field in topic.fields.iter() {
        println!("  - {}: {}", field.name(), field.data_type());
    }
}
