//! Data types for admin operations.

use crate::resource_type;

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
}

impl Namespace {
    /// Create a new namespace with the given name.
    pub fn new(name: NamespaceName) -> Self {
        Self { name }
    }
}

/// A topic belonging to a namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    /// The topic name.
    pub name: TopicName,
}

impl Topic {
    /// Create a new topic with the given name.
    pub fn new(name: TopicName) -> Self {
        Self { name }
    }
}

/// Request to create a new tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTenantRequest {
    /// The tenant ID.
    pub tenant_id: String,
    /// The tenant metadata.
    pub tenant: Tenant,
}

/// Request to get a tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetTenantRequest {
    /// The tenant name.
    pub name: TenantName,
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

/// Request to delete a tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTenantRequest {
    /// The tenant name.
    pub name: TenantName,
}

/// Request to create a new namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateNamespaceRequest {
    /// The tenant that owns the namespace.
    pub parent: TenantName,
    /// The namespace ID.
    pub namespace_id: String,
    /// The namespace metadata.
    pub namespace: Namespace,
}

/// Request to get a namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetNamespaceRequest {
    /// The namespace name.
    pub name: NamespaceName,
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

/// Request to delete a namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteNamespaceRequest {
    /// The namespace name.
    pub name: NamespaceName,
}

/// Request to create a new topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicRequest {
    /// The namespace that owns the topic.
    pub parent: NamespaceName,
    /// The topic ID.
    pub topic_id: String,
    /// The topic metadata.
    pub topic: Topic,
}

/// Request to get a topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetTopicRequest {
    /// The topic name.
    pub name: TopicName,
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

/// Request to delete a topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicRequest {
    /// The topic name.
    pub name: TopicName,
    /// If set to true, also delete data associated with the topic.
    pub force: bool,
}

#[cfg(test)]
mod tests {
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
        let namespace = Namespace::new(namespace_name.clone());

        assert_eq!(namespace.name, namespace_name);
        assert_eq!(namespace.name.id(), "test-namespace");
        assert_eq!(namespace.name.parent(), &tenant_name);
        assert_eq!(
            namespace.name.name(),
            "tenants/test-tenant/namespaces/test-namespace"
        );
    }

    #[test]
    fn test_topic_creation() {
        let tenant_name = TenantName::new("test-tenant");
        let namespace_name = NamespaceName::new("test-namespace", tenant_name);
        let topic_name = TopicName::new("test-topic", namespace_name.clone());
        let topic = Topic::new(topic_name.clone());

        assert_eq!(topic.name, topic_name);
        assert_eq!(topic.name.id(), "test-topic");
        assert_eq!(topic.name.parent(), &namespace_name);
        assert_eq!(
            topic.name.name(),
            "tenants/test-tenant/namespaces/test-namespace/topics/test-topic"
        );
    }

    #[test]
    fn test_create_tenant_request() {
        let tenant_name = TenantName::new("test-tenant");
        let tenant = Tenant::new(tenant_name.clone());
        let request = CreateTenantRequest {
            tenant_id: "test-tenant".to_string(),
            tenant,
        };

        assert_eq!(request.tenant_id, "test-tenant");
        assert_eq!(request.tenant.name, tenant_name);
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

    #[test]
    fn test_delete_topic_request() {
        let tenant_name = TenantName::new("test-tenant");
        let namespace_name = NamespaceName::new("test-namespace", tenant_name);
        let topic_name = TopicName::new("test-topic", namespace_name);
        let request = DeleteTopicRequest {
            name: topic_name.clone(),
            force: true,
        };

        assert_eq!(request.name, topic_name);
        assert_eq!(request.force, true);
    }
}
