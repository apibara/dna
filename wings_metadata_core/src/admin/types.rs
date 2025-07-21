//! Data types for admin operations.

use std::{sync::Arc, time::Duration};

use arrow::datatypes::{FieldRef, Fields, Schema, SchemaRef};
use bytesize::ByteSize;

use crate::resource_type;

// Define admin-specific resource types
resource_type!(Tenant, "tenants");
resource_type!(Namespace, "namespaces", Tenant);
resource_type!(Topic, "topics", Namespace);
resource_type!(Secret, "secrets");

/// A tenant in the Wings system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tenant {
    /// The tenant name.
    pub name: TenantName,
}

impl Tenant {
    /// Create a new tenant with the given name.
    pub fn new(name: TenantName) -> Self {
        Self { name }
    }
}

/// A namespace belonging to a tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    /// The namespace name.
    pub name: NamespaceName,
    /// The size at which the current segment is flushed to object storage.
    pub flush_size: ByteSize,
    /// The maximum interval at which the current segment is flushed to object storage.
    pub flush_interval: Duration,
    /// The default object store configuration for the namespace.
    pub default_object_store_config: SecretName,
    /// If specified, use this configuration to store data for long term storage.
    pub frozen_object_store_config: Option<SecretName>,
}

pub type NamespaceRef = Arc<Namespace>;

impl Namespace {
    /// Create a new namespace with the given name and options.
    pub fn new(name: NamespaceName, options: NamespaceOptions) -> Self {
        Self {
            name,
            flush_size: options.flush_size,
            flush_interval: options.flush_interval,
            default_object_store_config: options.default_object_store_config,
            frozen_object_store_config: options.frozen_object_store_config,
        }
    }
}

/// A topic belonging to a namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    /// The topic name.
    pub name: TopicName,
    /// The fields in the topic messages.
    pub fields: Fields,
    /// The index of the field that is used to partition the topic.
    pub partition_key: Option<usize>,
}

pub type TopicRef = Arc<Topic>;

impl Topic {
    /// Create a new topic with the given name and options.
    pub fn new(name: TopicName, options: TopicOptions) -> Self {
        Self {
            name,
            fields: options.fields,
            partition_key: options.partition_key,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(self.fields.clone()))
    }

    pub fn schema_without_partition_column(&self) -> SchemaRef {
        let Some(partition_index) = self.partition_key else {
            return self.schema();
        };
        let fields = self.fields.filter_leaves(|idx, _| idx != partition_index);
        Arc::new(Schema::new(fields))
    }

    pub fn partition_column(&self) -> Option<&FieldRef> {
        self.partition_key.map(|idx| &self.fields[idx])
    }
}

/// Options for creating a namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceOptions {
    /// The size at which the current segment is flushed to object storage.
    pub flush_size: ByteSize,
    /// The maximum interval at which the current segment is flushed to object storage.
    pub flush_interval: Duration,
    /// The default object store configuration for the namespace.
    pub default_object_store_config: SecretName,
    /// If specified, use this configuration to store data for long term storage.
    pub frozen_object_store_config: Option<SecretName>,
}

impl NamespaceOptions {
    /// Create new namespace options with the given default object store config.
    pub fn new(default_object_store_config: SecretName) -> Self {
        Self {
            flush_size: ByteSize::mb(8),
            flush_interval: Duration::from_millis(250),
            default_object_store_config,
            frozen_object_store_config: None,
        }
    }
}

/// Options for creating a topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicOptions {
    /// The fields in the topic messages.
    pub fields: Fields,
    /// The index of the field that is used to partition the topic.
    pub partition_key: Option<usize>,
}

impl TopicOptions {
    pub fn new(fields: impl Into<Fields>) -> Self {
        Self {
            fields: fields.into(),
            partition_key: None,
        }
    }

    pub fn new_with_partition_key(fields: impl Into<Fields>, partition_key: Option<usize>) -> Self {
        Self {
            fields: fields.into(),
            partition_key,
        }
    }
}

/// Request to list tenants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTenantsRequest {
    /// The number of tenants to return.
    /// Default: 100, Maximum: 1000.
    pub page_size: Option<i32>,
    /// The continuation token.
    pub page_token: Option<String>,
}

impl Default for ListTenantsRequest {
    fn default() -> Self {
        Self {
            page_size: Some(100),
            page_token: None,
        }
    }
}

/// Response from listing tenants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTenantsResponse {
    /// The tenants.
    pub tenants: Vec<Tenant>,
    /// The continuation token.
    pub next_page_token: Option<String>,
}

/// Request to list namespaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListNamespacesRequest {
    /// The parent tenant.
    pub parent: TenantName,
    /// The number of namespaces to return.
    /// Default: 100, Maximum: 1000.
    pub page_size: Option<i32>,
    /// The continuation token.
    pub page_token: Option<String>,
}

impl ListNamespacesRequest {
    /// Create a new request for the given parent tenant.
    pub fn new(parent: TenantName) -> Self {
        Self {
            parent,
            page_size: Some(100),
            page_token: None,
        }
    }
}

/// Response from listing namespaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListNamespacesResponse {
    /// The namespaces.
    pub namespaces: Vec<Namespace>,
    /// The continuation token.
    pub next_page_token: Option<String>,
}

/// Request to list topics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTopicsRequest {
    /// The parent namespace.
    pub parent: NamespaceName,
    /// The number of topics to return.
    /// Default: 100, Maximum: 1000.
    pub page_size: Option<i32>,
    /// The continuation token.
    pub page_token: Option<String>,
}

impl ListTopicsRequest {
    /// Create a new request for the given parent namespace.
    pub fn new(parent: NamespaceName) -> Self {
        Self {
            parent,
            page_size: Some(100),
            page_token: None,
        }
    }
}

/// Response from listing topics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTopicsResponse {
    /// The topics.
    pub topics: Vec<Topic>,
    /// The continuation token.
    pub next_page_token: Option<String>,
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use super::*;

    #[test]
    fn test_tenant_creation() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let tenant = Tenant::new(tenant_name.clone());

        assert_eq!(tenant.name, tenant_name);
        assert_eq!(tenant.name.id(), "test-tenant");
        assert_eq!(tenant.name.name(), "tenants/test-tenant");
    }

    #[test]
    fn test_namespace_creation() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name.clone()).unwrap();
        let options = NamespaceOptions::new(SecretName::new("test-config").unwrap());
        let namespace = Namespace::new(namespace_name.clone(), options.clone());

        assert_eq!(namespace.name, namespace_name);
        assert_eq!(namespace.name.id(), "test-namespace");
        assert_eq!(namespace.name.parent(), &tenant_name);
        assert_eq!(
            namespace.name.name(),
            "tenants/test-tenant/namespaces/test-namespace"
        );
        assert_eq!(namespace.flush_size, options.flush_size);
        assert_eq!(namespace.flush_interval, options.flush_interval);
        assert_eq!(
            namespace.default_object_store_config,
            options.default_object_store_config
        );
        assert_eq!(
            namespace.frozen_object_store_config,
            options.frozen_object_store_config
        );
    }

    #[test]
    fn test_topic_creation() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name.clone()).unwrap();
        let options = TopicOptions::new(vec![Field::new("test", DataType::Utf8, false)]);
        let topic = Topic::new(topic_name.clone(), options);

        assert_eq!(topic.name, topic_name);
        assert_eq!(topic.name.id(), "test-topic");
        assert_eq!(topic.name.parent(), &namespace_name);
        assert_eq!(
            topic.name.name(),
            "tenants/test-tenant/namespaces/test-namespace/topics/test-topic"
        );
        assert_eq!(topic.fields.len(), 1);
        assert_eq!(topic.partition_key, None);
    }

    #[test]
    fn test_list_namespaces_request() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let request = ListNamespacesRequest::new(tenant_name.clone());

        assert_eq!(request.parent, tenant_name);
        assert_eq!(request.page_size, Some(100));
        assert_eq!(request.page_token, None);
    }

    #[test]
    fn test_list_topics_request() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let request = ListTopicsRequest::new(namespace_name.clone());

        assert_eq!(request.parent, namespace_name);
        assert_eq!(request.page_size, Some(100));
        assert_eq!(request.page_token, None);
    }

    #[test]
    fn test_secret_name() {
        let secret_name = SecretName::new("test-secret").unwrap();

        assert_eq!(secret_name.id(), "test-secret");
        assert_eq!(secret_name.name(), "secrets/test-secret");
    }

    #[test]
    fn test_namespace_options_with_secrets() {
        let default_secret = SecretName::new("default-config").unwrap();
        let frozen_secret = SecretName::new("frozen-config").unwrap();

        let mut options = NamespaceOptions::new(default_secret.clone());
        options.flush_size = ByteSize::mb(16);
        options.flush_interval = Duration::from_millis(500);
        options.frozen_object_store_config = Some(frozen_secret.clone());

        assert_eq!(options.flush_size, ByteSize::mb(16));
        assert_eq!(options.flush_interval, Duration::from_millis(500));
        assert_eq!(options.default_object_store_config, default_secret);
        assert_eq!(options.frozen_object_store_config, Some(frozen_secret));
    }

    #[test]
    fn test_namespace_options_new_method() {
        let secret_name = SecretName::new("my-secret").unwrap();
        let options = NamespaceOptions::new(secret_name.clone());

        assert_eq!(options.default_object_store_config, secret_name);

        assert_eq!(options.flush_size, ByteSize::mb(8));
        assert_eq!(options.flush_interval, Duration::from_millis(250));
        assert_eq!(options.frozen_object_store_config, None);
    }

    #[test]
    fn test_topic_options_with_partition_key() {
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ];

        let options = TopicOptions::new_with_partition_key(fields.clone(), Some(1));

        assert_eq!(options.fields.len(), 3);
        assert_eq!(options.partition_key, Some(1));
    }

    #[test]
    fn test_topic_options_without_partition_key() {
        let fields = vec![Field::new("data", DataType::Utf8, false)];
        let options = TopicOptions::new(fields.clone());

        assert_eq!(options.fields.len(), 1);
        assert_eq!(options.partition_key, None);
    }

    #[test]
    fn test_topic_with_partition_key() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name.clone()).unwrap();

        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("message", DataType::Utf8, false),
        ];
        let options = TopicOptions::new_with_partition_key(fields, Some(0));
        let topic = Topic::new(topic_name.clone(), options);

        assert_eq!(topic.name, topic_name);
        assert_eq!(topic.fields.len(), 2);
        assert_eq!(topic.partition_key, Some(0));
    }
}
