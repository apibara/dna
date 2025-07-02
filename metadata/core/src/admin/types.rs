//! Data types for admin operations.

use crate::resource_type;
use arrow::datatypes::Fields;
use bytesize::ByteSize;
use std::time::Duration;

// Define admin-specific resource types
resource_type!(Tenant, "tenants");
resource_type!(Namespace, "namespaces", Tenant);
resource_type!(Topic, "topics", Namespace);

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
}

impl Namespace {
    /// Create a new namespace with the given name and options.
    pub fn new(name: NamespaceName, options: NamespaceOptions) -> Self {
        Self {
            name,
            flush_size: options.flush_size,
            flush_interval: options.flush_interval,
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
    /// The indices of the fields that are used to partition the topic.
    pub partition_keys: Vec<usize>,
}

impl Topic {
    /// Create a new topic with the given name and options.
    pub fn new(name: TopicName, options: TopicOptions) -> Self {
        Self {
            name,
            fields: options.fields,
            partition_keys: options.partition_keys,
        }
    }
}

/// Options for creating a namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceOptions {
    /// The size at which the current segment is flushed to object storage.
    pub flush_size: ByteSize,
    /// The maximum interval at which the current segment is flushed to object storage.
    pub flush_interval: Duration,
}

impl Default for NamespaceOptions {
    fn default() -> Self {
        Self {
            flush_size: ByteSize::mb(8),
            flush_interval: Duration::from_millis(250),
        }
    }
}

/// Options for creating a topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicOptions {
    /// The fields in the topic messages.
    pub fields: Fields,
    /// The indices of the fields that are used to partition the topic.
    pub partition_keys: Vec<usize>,
}

impl TopicOptions {
    pub fn new(fields: impl Into<Fields>) -> Self {
        Self {
            fields: fields.into(),
            partition_keys: Vec::new(),
        }
    }

    pub fn new_with_partition_keys(fields: impl Into<Fields>, partition_keys: Vec<usize>) -> Self {
        Self {
            fields: fields.into(),
            partition_keys,
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
        let tenant_name = TenantName::new("test-tenant");
        let tenant = Tenant::new(tenant_name.clone());

        assert_eq!(tenant.name, tenant_name);
        assert_eq!(tenant.name.id(), "test-tenant");
        assert_eq!(tenant.name.name(), "tenants/test-tenant");
    }

    #[test]
    fn test_namespace_creation() {
        let tenant_name = TenantName::new("test-tenant");
        let namespace_name = NamespaceName::new("test-namespace", tenant_name.clone());
        let options = NamespaceOptions::default();
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
    }

    #[test]
    fn test_topic_creation() {
        let tenant_name = TenantName::new("test-tenant");
        let namespace_name = NamespaceName::new("test-namespace", tenant_name);
        let topic_name = TopicName::new("test-topic", namespace_name.clone());
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
        assert_eq!(topic.partition_keys.len(), 0);
    }

    #[test]
    fn test_list_namespaces_request() {
        let tenant_name = TenantName::new("test-tenant");
        let request = ListNamespacesRequest::new(tenant_name.clone());

        assert_eq!(request.parent, tenant_name);
        assert_eq!(request.page_size, Some(100));
        assert_eq!(request.page_token, None);
    }

    #[test]
    fn test_list_topics_request() {
        let tenant_name = TenantName::new("test-tenant");
        let namespace_name = NamespaceName::new("test-namespace", tenant_name);
        let request = ListTopicsRequest::new(namespace_name.clone());

        assert_eq!(request.parent, namespace_name);
        assert_eq!(request.page_size, Some(100));
        assert_eq!(request.page_token, None);
    }
}
