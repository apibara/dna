//! Conversions between admin domain types and protobuf types.

use std::time::Duration;

use arrow::datatypes::Schema;
use arrow_ipc::convert::{IpcSchemaEncoder, fb_to_schema};
use arrow_ipc::root_as_schema;
use bytesize::ByteSize;
use error_stack::{Report, ResultExt};

use crate::admin::error::{AdminError, AdminResult};
use crate::admin::types::*;
use crate::protocol::wings::v1 as pb;

// Tenant conversions

impl From<Tenant> for pb::Tenant {
    fn from(tenant: Tenant) -> Self {
        Self {
            name: tenant.name.name(),
        }
    }
}

impl TryFrom<pb::Tenant> for Tenant {
    type Error = Report<AdminError>;

    fn try_from(tenant: pb::Tenant) -> AdminResult<Self> {
        let name = TenantName::parse(&tenant.name).change_context(AdminError::InvalidArgument {
            resource: "tenant",
            message: "invalid tenant name format".to_string(),
        })?;

        Ok(Self { name })
    }
}

// Namespace conversions

impl From<Namespace> for pb::Namespace {
    fn from(namespace: Namespace) -> Self {
        Self {
            name: namespace.name.name(),
            flush_size_bytes: namespace.flush_size.as_u64(),
            flush_interval_millis: namespace.flush_interval.as_millis() as u64,
            default_object_store_config: namespace.default_object_store_config.name(),
            frozen_object_store_config: namespace
                .frozen_object_store_config
                .map(|config| config.name()),
        }
    }
}

impl TryFrom<pb::Namespace> for Namespace {
    type Error = Report<AdminError>;

    fn try_from(namespace: pb::Namespace) -> AdminResult<Self> {
        let name =
            NamespaceName::parse(&namespace.name).change_context(AdminError::InvalidArgument {
                resource: "namespace",
                message: "invalid namespace name format".to_string(),
            })?;
        let flush_size = ByteSize::b(namespace.flush_size_bytes);
        let flush_interval = Duration::from_millis(namespace.flush_interval_millis);
        let default_object_store_config = SecretName::parse(&namespace.default_object_store_config)
            .change_context(AdminError::InvalidArgument {
                resource: "secret",
                message: "invalid default object store config name format".to_string(),
            })?;
        let frozen_object_store_config = namespace
            .frozen_object_store_config
            .map(|config| {
                SecretName::parse(&config).change_context(AdminError::InvalidArgument {
                    resource: "secret",
                    message: "invalid frozen object store config name format".to_string(),
                })
            })
            .transpose()?;

        Ok(Self {
            name,
            flush_size,
            flush_interval,
            default_object_store_config,
            frozen_object_store_config,
        })
    }
}

impl TryFrom<pb::Namespace> for NamespaceOptions {
    type Error = Report<AdminError>;

    fn try_from(namespace: pb::Namespace) -> AdminResult<Self> {
        let flush_size = ByteSize::b(namespace.flush_size_bytes);
        let flush_interval = Duration::from_millis(namespace.flush_interval_millis);

        let default_object_store_config = SecretName::parse(&namespace.default_object_store_config)
            .change_context(AdminError::InvalidArgument {
                resource: "secret",
                message: "invalid default object store config name format".to_string(),
            })?;

        let frozen_object_store_config = namespace
            .frozen_object_store_config
            .map(|config| {
                SecretName::parse(&config).change_context(AdminError::InvalidArgument {
                    resource: "secret",
                    message: "invalid frozen object store config name format".to_string(),
                })
            })
            .transpose()?;

        Ok(Self {
            flush_size,
            flush_interval,
            default_object_store_config,
            frozen_object_store_config,
        })
    }
}

impl From<NamespaceOptions> for pb::Namespace {
    fn from(options: NamespaceOptions) -> Self {
        Self {
            name: String::new(),
            flush_size_bytes: options.flush_size.as_u64(),
            flush_interval_millis: options.flush_interval.as_millis() as u64,
            default_object_store_config: options.default_object_store_config.to_string(),
            frozen_object_store_config: options
                .frozen_object_store_config
                .map(|config| config.to_string()),
        }
    }
}

// Topic conversions

impl From<Topic> for pb::Topic {
    fn from(topic: Topic) -> Self {
        let fields = serialize_fields(&topic.fields);
        let partition_key = topic.partition_key.map(|idx| idx as u32);

        Self {
            name: topic.name.name(),
            fields,
            partition_key,
        }
    }
}

impl TryFrom<pb::Topic> for Topic {
    type Error = Report<AdminError>;

    fn try_from(topic: pb::Topic) -> AdminResult<Self> {
        let name = TopicName::parse(&topic.name).change_context(AdminError::InvalidArgument {
            resource: "topic",
            message: "invalid topic name format".to_string(),
        })?;
        let fields = deserialize_fields(&topic.fields)?;
        let partition_key = topic.partition_key.map(|idx| idx as usize);

        Ok(Self {
            name,
            fields,
            partition_key,
        })
    }
}

impl TryFrom<pb::Topic> for TopicOptions {
    type Error = Report<AdminError>;

    fn try_from(topic: pb::Topic) -> AdminResult<Self> {
        let fields = deserialize_fields(&topic.fields)?;
        let partition_key = topic.partition_key.map(|idx| idx as usize);

        Ok(Self {
            fields,
            partition_key,
        })
    }
}

impl From<TopicOptions> for pb::Topic {
    fn from(options: TopicOptions) -> Self {
        pb::Topic {
            name: String::new(),
            fields: serialize_fields(&options.fields),
            partition_key: options.partition_key.map(|idx| idx as u32),
        }
    }
}

// Request/Response conversions

impl From<ListTenantsRequest> for pb::ListTenantsRequest {
    fn from(request: ListTenantsRequest) -> Self {
        Self {
            page_size: request.page_size,
            page_token: request.page_token.clone(),
        }
    }
}

impl From<pb::ListTenantsRequest> for ListTenantsRequest {
    fn from(request: pb::ListTenantsRequest) -> Self {
        Self {
            page_size: request.page_size,
            page_token: request.page_token.clone(),
        }
    }
}

impl From<ListTenantsResponse> for pb::ListTenantsResponse {
    fn from(response: ListTenantsResponse) -> Self {
        let tenants = response.tenants.into_iter().map(pb::Tenant::from).collect();

        Self {
            tenants,
            next_page_token: response.next_page_token.unwrap_or_default(),
        }
    }
}

impl TryFrom<pb::ListTenantsResponse> for ListTenantsResponse {
    type Error = Report<AdminError>;

    fn try_from(response: pb::ListTenantsResponse) -> AdminResult<Self> {
        let tenants = response
            .tenants
            .into_iter()
            .map(Tenant::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            tenants,
            next_page_token: if response.next_page_token.is_empty() {
                None
            } else {
                Some(response.next_page_token)
            },
        })
    }
}

impl From<ListNamespacesRequest> for pb::ListNamespacesRequest {
    fn from(request: ListNamespacesRequest) -> Self {
        Self {
            parent: request.parent.name(),
            page_size: request.page_size,
            page_token: request.page_token.clone(),
        }
    }
}

impl TryFrom<pb::ListNamespacesRequest> for ListNamespacesRequest {
    type Error = Report<AdminError>;

    fn try_from(request: pb::ListNamespacesRequest) -> AdminResult<Self> {
        let parent =
            TenantName::parse(&request.parent).change_context(AdminError::InvalidArgument {
                resource: "tenant",
                message: "invalid parent tenant name format".to_string(),
            })?;

        Ok(Self {
            parent,
            page_size: request.page_size,
            page_token: request.page_token.clone(),
        })
    }
}

impl From<ListNamespacesResponse> for pb::ListNamespacesResponse {
    fn from(response: ListNamespacesResponse) -> Self {
        let namespaces = response
            .namespaces
            .into_iter()
            .map(pb::Namespace::from)
            .collect();

        Self {
            namespaces,
            next_page_token: response.next_page_token.unwrap_or_default(),
        }
    }
}

impl TryFrom<pb::ListNamespacesResponse> for ListNamespacesResponse {
    type Error = Report<AdminError>;

    fn try_from(response: pb::ListNamespacesResponse) -> AdminResult<Self> {
        let namespaces = response
            .namespaces
            .into_iter()
            .map(Namespace::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            namespaces,
            next_page_token: if response.next_page_token.is_empty() {
                None
            } else {
                Some(response.next_page_token)
            },
        })
    }
}

impl From<ListTopicsRequest> for pb::ListTopicsRequest {
    fn from(request: ListTopicsRequest) -> Self {
        Self {
            parent: request.parent.name(),
            page_size: request.page_size,
            page_token: request.page_token.clone(),
        }
    }
}

impl TryFrom<pb::ListTopicsRequest> for ListTopicsRequest {
    type Error = Report<AdminError>;

    fn try_from(request: pb::ListTopicsRequest) -> AdminResult<Self> {
        let parent =
            NamespaceName::parse(&request.parent).change_context(AdminError::InvalidArgument {
                resource: "namespace",
                message: "invalid parent namespace name format".to_string(),
            })?;

        Ok(Self {
            parent,
            page_size: request.page_size,
            page_token: request.page_token.clone(),
        })
    }
}

impl From<ListTopicsResponse> for pb::ListTopicsResponse {
    fn from(response: ListTopicsResponse) -> Self {
        let topics = response
            .topics
            .into_iter()
            .map(pb::Topic::from)
            .collect::<Vec<_>>();

        Self {
            topics,
            next_page_token: response.next_page_token.unwrap_or_default(),
        }
    }
}

impl TryFrom<pb::ListTopicsResponse> for ListTopicsResponse {
    type Error = Report<AdminError>;

    fn try_from(response: pb::ListTopicsResponse) -> AdminResult<Self> {
        let topics = response
            .topics
            .into_iter()
            .map(Topic::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            topics,
            next_page_token: if response.next_page_token.is_empty() {
                None
            } else {
                Some(response.next_page_token)
            },
        })
    }
}

// Helper functions for Arrow schema serialization

fn serialize_fields(fields: &arrow::datatypes::Fields) -> Vec<u8> {
    let schema = Schema::new(fields.clone());
    let fb = IpcSchemaEncoder::new().schema_to_fb(&schema);
    fb.finished_data().to_vec()
}

fn deserialize_fields(data: &[u8]) -> AdminResult<arrow::datatypes::Fields> {
    let ipc_schema = root_as_schema(data).map_err(|inner| AdminError::InvalidArgument {
        resource: "topic",
        message: format!("invalid topic schema: {}", inner),
    })?;
    let schema = fb_to_schema(ipc_schema);
    Ok(schema.fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_tenant_conversion() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let domain_tenant = Tenant::new(tenant_name.clone());

        // Domain to protobuf
        let pb_tenant = pb::Tenant::from(domain_tenant.clone());
        assert_eq!(pb_tenant.name, "tenants/test-tenant");

        // Protobuf to domain
        let converted_tenant = Tenant::try_from(pb_tenant).unwrap();
        assert_eq!(converted_tenant, domain_tenant);
    }

    #[test]
    fn test_namespace_conversion() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let secret_name = SecretName::new("test-secret").unwrap();
        let options = NamespaceOptions::new(secret_name.clone());
        let domain_namespace = Namespace::new(namespace_name.clone(), options);

        // Domain to protobuf
        let pb_namespace = pb::Namespace::from(domain_namespace.clone());
        assert_eq!(
            pb_namespace.name,
            "tenants/test-tenant/namespaces/test-namespace"
        );
        assert_eq!(pb_namespace.flush_size_bytes, ByteSize::mb(8).as_u64());
        assert_eq!(pb_namespace.flush_interval_millis, 250);
        assert_eq!(
            pb_namespace.default_object_store_config,
            "secrets/test-secret"
        );
        assert_eq!(pb_namespace.frozen_object_store_config, None);

        // Protobuf to domain
        let converted_namespace = Namespace::try_from(pb_namespace).unwrap();
        assert_eq!(converted_namespace, domain_namespace);
    }

    #[test]
    fn test_topic_conversion() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let fields = vec![Field::new("test", DataType::Utf8, false)];
        let options = TopicOptions::new_with_partition_key(fields.clone(), Some(0));
        let domain_topic = Topic::new(topic_name.clone(), options);

        // Domain to protobuf
        let pb_topic = pb::Topic::from(domain_topic.clone());
        assert_eq!(
            pb_topic.name,
            "tenants/test-tenant/namespaces/test-namespace/topics/test-topic"
        );
        assert_eq!(pb_topic.partition_key, Some(0));
        assert!(!pb_topic.fields.is_empty());

        // Protobuf to domain
        let converted_topic = Topic::try_from(pb_topic).unwrap();
        assert_eq!(converted_topic.name, domain_topic.name);
        assert_eq!(converted_topic.partition_key, domain_topic.partition_key);
        assert_eq!(converted_topic.fields.len(), domain_topic.fields.len());
    }

    #[test]
    fn test_list_tenants_request_conversion() {
        let domain_request = ListTenantsRequest {
            page_size: Some(50),
            page_token: Some("token123".to_string()),
        };

        // Domain to protobuf
        let pb_request = pb::ListTenantsRequest::from(domain_request.clone());
        assert_eq!(pb_request.page_size.unwrap(), 50);
        assert_eq!(pb_request.page_token.clone().unwrap(), "token123");

        // Protobuf to domain
        let converted_request = ListTenantsRequest::from(pb_request);
        assert_eq!(converted_request, domain_request);
    }

    #[test]
    fn test_list_tenants_request_defaults() {
        let domain_request = ListTenantsRequest::default();

        // Domain to protobuf
        let pb_request = pb::ListTenantsRequest::from(domain_request.clone());
        assert_eq!(pb_request.page_size, Some(100));
        assert_eq!(pb_request.page_token, None);

        // Protobuf to domain
        let converted_request = ListTenantsRequest::from(pb_request);
        assert_eq!(converted_request.page_size, Some(100));
        assert_eq!(converted_request.page_token, None);
    }

    #[test]
    fn test_list_tenants_response_conversion() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let tenant = Tenant::new(tenant_name);
        let domain_response = ListTenantsResponse {
            tenants: vec![tenant],
            next_page_token: Some("next-token".to_string()),
        };

        // Domain to protobuf
        let pb_response = pb::ListTenantsResponse::from(domain_response.clone());
        assert_eq!(pb_response.tenants.len(), 1);
        assert_eq!(pb_response.next_page_token, "next-token");

        // Protobuf to domain
        let converted_response = ListTenantsResponse::try_from(pb_response).unwrap();
        assert_eq!(
            converted_response.tenants.len(),
            domain_response.tenants.len()
        );
        assert_eq!(
            converted_response.next_page_token,
            domain_response.next_page_token
        );
    }

    #[test]
    fn test_invalid_tenant_name_conversion() {
        let pb_tenant = pb::Tenant {
            name: "invalid-format".to_string(),
        };

        let result = Tenant::try_from(pb_tenant);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_namespace_name_conversion() {
        let pb_namespace = pb::Namespace {
            name: "invalid-format".to_string(),
            flush_size_bytes: 1024,
            flush_interval_millis: 250,
            default_object_store_config: "secrets/test".to_string(),
            frozen_object_store_config: None,
        };

        let result = Namespace::try_from(pb_namespace);
        assert!(result.is_err());
    }

    #[test]
    fn test_arrow_schema_serialization() {
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ];

        let serialized = serialize_fields(&fields.into());
        assert!(!serialized.is_empty());

        let deserialized = deserialize_fields(&serialized).unwrap();
        assert_eq!(deserialized.len(), 3);
        assert_eq!(deserialized[0].name(), "id");
        assert_eq!(deserialized[1].name(), "name");
        assert_eq!(deserialized[2].name(), "value");
    }
}
