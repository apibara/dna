//! In-memory implementation of the Admin trait.
//!
//! This implementation stores all data in memory and is suitable for testing
//! and development purposes. It uses a RwLock for thread-safe access.
//!

use std::collections::HashMap;
use tokio::sync::RwLock;

use crate::admin::{
    Admin, AdminError, AdminResult, ListNamespacesRequest, ListNamespacesResponse,
    ListTenantsRequest, ListTenantsResponse, ListTopicsRequest, ListTopicsResponse, Namespace,
    NamespaceName, NamespaceOptions, Tenant, TenantName, Topic, TopicName, TopicOptions,
};
use async_trait::async_trait;

/// In-memory storage for admin data.
#[derive(Debug, Default)]
struct AdminStore {
    /// Map of tenant ID to tenant data.
    tenants: HashMap<String, Tenant>,
    /// Map of namespace name to namespace data.
    namespaces: HashMap<String, Namespace>,
    /// Map of topic name to topic data.
    topics: HashMap<String, Topic>,
}

/// In-memory implementation of the Admin trait.
///
/// This implementation stores all metadata in memory and is suitable for
/// testing and development. All data is lost when the service is stopped.
#[derive(Debug)]
pub struct InMemoryAdminService {
    store: RwLock<AdminStore>,
}

impl InMemoryAdminService {
    /// Create a new in-memory admin service.
    pub fn new() -> Self {
        Self {
            store: RwLock::new(AdminStore::default()),
        }
    }
}

impl Default for InMemoryAdminService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Admin for InMemoryAdminService {
    // Tenant operations

    async fn create_tenant(&self, name: TenantName) -> AdminResult<Tenant> {
        let mut store = self.store.write().await;

        let tenant_id = name.id().to_string();

        if store.tenants.contains_key(&tenant_id) {
            return Err(error_stack::Report::new(AdminError::AlreadyExists {
                resource: "tenant",
                message: tenant_id.clone(),
            }));
        }

        let tenant = Tenant::new(name);
        store.tenants.insert(tenant_id, tenant.clone());

        Ok(tenant)
    }

    async fn get_tenant(&self, name: TenantName) -> AdminResult<Tenant> {
        let store = self.store.read().await;

        let tenant_id = name.id();
        store.tenants.get(tenant_id).cloned().ok_or_else(|| {
            error_stack::Report::new(AdminError::NotFound {
                resource: "tenant",
                message: tenant_id.to_string(),
            })
        })
    }

    async fn list_tenants(&self, request: ListTenantsRequest) -> AdminResult<ListTenantsResponse> {
        let store = self.store.read().await;

        let page_size = request.page_size.unwrap_or(100).clamp(1, 1000) as usize;
        let page_token = request.page_token.as_deref().unwrap_or("");

        // For simplicity, we'll use the tenant ID as the page token
        // In a real implementation, you'd want a more sophisticated pagination system
        let mut tenant_ids: Vec<_> = store.tenants.keys().collect();
        tenant_ids.sort();

        let start_index = if page_token.is_empty() {
            0
        } else {
            tenant_ids
                .iter()
                .position(|id| *id == page_token)
                .map(|pos| pos + 1)
                .unwrap_or(0)
        };

        let end_index = (start_index + page_size).min(tenant_ids.len());
        let page_tenant_ids = &tenant_ids[start_index..end_index];

        let tenants: Vec<Tenant> = page_tenant_ids
            .iter()
            .filter_map(|id| store.tenants.get(*id).cloned())
            .collect();

        let next_page_token = if end_index < tenant_ids.len() {
            Some(tenant_ids[end_index - 1].clone())
        } else {
            None
        };

        Ok(ListTenantsResponse {
            tenants,
            next_page_token,
        })
    }

    async fn delete_tenant(&self, name: TenantName) -> AdminResult<()> {
        let mut store = self.store.write().await;

        let tenant_id = name.id();

        if !store.tenants.contains_key(tenant_id) {
            return Err(error_stack::Report::new(AdminError::NotFound {
                resource: "tenant",
                message: tenant_id.to_string(),
            }));
        }

        let has_namespaces = store
            .namespaces
            .values()
            .any(|namespace| namespace.name.parent().id() == tenant_id);

        if has_namespaces {
            return Err(error_stack::Report::new(AdminError::InvalidArgument {
                resource: "tenant",
                message: format!("{} has namespaces and cannot be deleted", tenant_id),
            })
            .attach_printable("tenant has namespaces and cannot be deleted"));
        }

        store.tenants.remove(tenant_id);
        Ok(())
    }

    // Namespace operations

    async fn create_namespace(
        &self,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> AdminResult<Namespace> {
        let mut store = self.store.write().await;

        let namespace_key = name.name();
        let tenant_id = name.parent().id();

        if !store.tenants.contains_key(tenant_id) {
            return Err(error_stack::Report::new(AdminError::NotFound {
                resource: "tenant",
                message: tenant_id.to_string(),
            })
            .attach_printable("parent tenant must exist before creating namespace"));
        }

        if store.namespaces.contains_key(&namespace_key) {
            return Err(error_stack::Report::new(AdminError::AlreadyExists {
                resource: "namespace",
                message: name.id().to_string(),
            }));
        }

        let namespace = Namespace::new(name, options);
        store.namespaces.insert(namespace_key, namespace.clone());

        Ok(namespace)
    }

    async fn get_namespace(&self, name: NamespaceName) -> AdminResult<Namespace> {
        let store = self.store.read().await;

        let namespace_key = name.name();
        store
            .namespaces
            .get(&namespace_key)
            .cloned()
            .ok_or_else(|| {
                error_stack::Report::new(AdminError::NotFound {
                    resource: "namespace",
                    message: name.id().to_string(),
                })
            })
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> AdminResult<ListNamespacesResponse> {
        let store = self.store.read().await;

        let tenant_id = request.parent.id();

        if !store.tenants.contains_key(tenant_id) {
            return Err(error_stack::Report::new(AdminError::NotFound {
                resource: "tenant",
                message: tenant_id.to_string(),
            })
            .attach_printable("parent tenant must exist to list namespaces"));
        }

        let page_size = request.page_size.unwrap_or(100).clamp(1, 1000) as usize;
        let page_token = request.page_token.as_deref().unwrap_or("");

        let mut namespace_keys: Vec<_> = store
            .namespaces
            .keys()
            .filter(|key| {
                if let Ok(ns_name) = NamespaceName::parse(key) {
                    ns_name.parent().id() == tenant_id
                } else {
                    false
                }
            })
            .collect();
        namespace_keys.sort();

        let start_index = if page_token.is_empty() {
            0
        } else {
            namespace_keys
                .iter()
                .position(|key| *key == page_token)
                .map(|pos| pos + 1)
                .unwrap_or(0)
        };

        let end_index = (start_index + page_size).min(namespace_keys.len());
        let page_namespace_keys = &namespace_keys[start_index..end_index];

        let namespaces: Vec<Namespace> = page_namespace_keys
            .iter()
            .filter_map(|key| store.namespaces.get(*key).cloned())
            .collect();

        let next_page_token = if end_index < namespace_keys.len() {
            Some(namespace_keys[end_index - 1].clone())
        } else {
            None
        };

        Ok(ListNamespacesResponse {
            namespaces,
            next_page_token,
        })
    }

    async fn delete_namespace(&self, name: NamespaceName) -> AdminResult<()> {
        let mut store = self.store.write().await;

        let namespace_key = name.name();

        if !store.namespaces.contains_key(&namespace_key) {
            return Err(error_stack::Report::new(AdminError::NotFound {
                resource: "namespace",
                message: name.id().to_string(),
            }));
        }

        let has_topics = store
            .topics
            .values()
            .any(|topic| topic.name.parent().name() == namespace_key);

        if has_topics {
            return Err(error_stack::Report::new(AdminError::InvalidArgument {
                resource: "namespace",
                message: format!("{} has topics and cannot be deleted", name.id()),
            })
            .attach_printable("namespace has topics and cannot be deleted"));
        }

        store.namespaces.remove(&namespace_key);
        Ok(())
    }

    // Topic operations

    async fn create_topic(&self, name: TopicName, options: TopicOptions) -> AdminResult<Topic> {
        let mut store = self.store.write().await;

        let topic_key = name.name();
        let namespace_key = name.parent().name();

        if !store.namespaces.contains_key(&namespace_key) {
            return Err(error_stack::Report::new(AdminError::NotFound {
                resource: "namespace",
                message: name.parent().id().to_string(),
            })
            .attach_printable("parent namespace must exist before creating topic"));
        }

        if store.topics.contains_key(&topic_key) {
            return Err(error_stack::Report::new(AdminError::AlreadyExists {
                resource: "topic",
                message: name.id().to_string(),
            }));
        }

        if let Some(key_index) = options.partition_key {
            if key_index >= options.fields.len() {
                return Err(error_stack::Report::new(AdminError::InvalidArgument {
                    resource: "topic",
                    message: format!(
                        "partition key index {} is out of bounds for fields (length: {})",
                        key_index,
                        options.fields.len()
                    ),
                }));
            }
        }

        let topic = Topic::new(name, options);
        store.topics.insert(topic_key, topic.clone());

        Ok(topic)
    }

    async fn get_topic(&self, name: TopicName) -> AdminResult<Topic> {
        let store = self.store.read().await;

        let topic_key = name.name();
        store.topics.get(&topic_key).cloned().ok_or_else(|| {
            error_stack::Report::new(AdminError::NotFound {
                resource: "topic",
                message: name.id().to_string(),
            })
        })
    }

    async fn list_topics(&self, request: ListTopicsRequest) -> AdminResult<ListTopicsResponse> {
        let store = self.store.read().await;

        let namespace_key = request.parent.name();

        if !store.namespaces.contains_key(&namespace_key) {
            return Err(error_stack::Report::new(AdminError::NotFound {
                resource: "namespace",
                message: request.parent.id().to_string(),
            })
            .attach_printable("parent namespace must exist to list topics"));
        }

        let page_size = request.page_size.unwrap_or(100).clamp(1, 1000) as usize;
        let page_token = request.page_token.as_deref().unwrap_or("");

        let mut topic_keys: Vec<_> = store
            .topics
            .keys()
            .filter(|key| {
                if let Ok(topic_name) = TopicName::parse(key) {
                    topic_name.parent().name() == namespace_key
                } else {
                    false
                }
            })
            .collect();
        topic_keys.sort();

        let start_index = if page_token.is_empty() {
            0
        } else {
            topic_keys
                .iter()
                .position(|key| *key == page_token)
                .map(|pos| pos + 1)
                .unwrap_or(0)
        };

        let end_index = (start_index + page_size).min(topic_keys.len());
        let page_topic_keys = &topic_keys[start_index..end_index];

        let topics: Vec<Topic> = page_topic_keys
            .iter()
            .filter_map(|key| store.topics.get(*key).cloned())
            .collect();

        let next_page_token = if end_index < topic_keys.len() {
            Some(topic_keys[end_index - 1].clone())
        } else {
            None
        };

        Ok(ListTopicsResponse {
            topics,
            next_page_token,
        })
    }

    async fn delete_topic(&self, name: TopicName, _force: bool) -> AdminResult<()> {
        let mut store = self.store.write().await;

        let topic_key = name.name();

        if !store.topics.contains_key(&topic_key) {
            return Err(error_stack::Report::new(AdminError::NotFound {
                resource: "topic",
                message: name.id().to_string(),
            }));
        }

        store.topics.remove(&topic_key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use super::*;
    use crate::admin::{NamespaceName, SecretName, TenantName};

    async fn create_test_tenant(service: &InMemoryAdminService, tenant_id: &str) -> Tenant {
        let tenant_name = TenantName::new(tenant_id).unwrap();
        service.create_tenant(tenant_name).await.unwrap()
    }

    async fn create_test_namespace(
        service: &InMemoryAdminService,
        tenant_id: &str,
        namespace_id: &str,
    ) -> Namespace {
        let tenant_name = TenantName::new(tenant_id).unwrap();
        let namespace_name = NamespaceName::new(namespace_id, tenant_name).unwrap();
        service
            .create_namespace(
                namespace_name,
                NamespaceOptions::new(SecretName::new("test-config").unwrap()),
            )
            .await
            .unwrap()
    }

    async fn create_test_topic(
        service: &InMemoryAdminService,
        tenant_id: &str,
        namespace_id: &str,
        topic_id: &str,
    ) -> Topic {
        let tenant_name = TenantName::new(tenant_id).unwrap();
        let namespace_name = NamespaceName::new(namespace_id, tenant_name).unwrap();
        let topic_name = TopicName::new(topic_id, namespace_name).unwrap();
        let topic_options = TopicOptions::new(vec![Field::new("key", DataType::Utf8, false)]);
        service
            .create_topic(topic_name, topic_options)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_create_tenant() {
        let service = InMemoryAdminService::new();
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let tenant = service.create_tenant(tenant_name.clone()).await.unwrap();

        assert_eq!(tenant.name, tenant_name);
        assert_eq!(tenant.name.id(), "test-tenant");
        assert_eq!(tenant.name.name(), "tenants/test-tenant");
    }

    #[tokio::test]
    async fn test_create_tenant_already_exists() {
        let service = InMemoryAdminService::new();
        let tenant_name = TenantName::new("test-tenant").unwrap();

        service.create_tenant(tenant_name.clone()).await.unwrap();

        let result = service.create_tenant(tenant_name).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::AlreadyExists {
                resource: "tenant",
                ..
            }
        ));
    }

    // Removed test_create_tenant_mismatched_id since API no longer allows this scenario

    #[tokio::test]
    async fn test_get_tenant() {
        let service = InMemoryAdminService::new();
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let created_tenant = service.create_tenant(tenant_name.clone()).await.unwrap();

        let retrieved_tenant = service.get_tenant(tenant_name).await.unwrap();
        assert_eq!(created_tenant, retrieved_tenant);
    }

    #[tokio::test]
    async fn test_get_tenant_not_found() {
        let service = InMemoryAdminService::new();
        let tenant_name = TenantName::new("nonexistent").unwrap();

        let result = service.get_tenant(tenant_name).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::NotFound {
                resource: "tenant",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_list_tenants_empty() {
        let service = InMemoryAdminService::new();
        let request = ListTenantsRequest::default();
        let response = service.list_tenants(request).await.unwrap();

        assert!(response.tenants.is_empty());
        assert!(response.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_list_tenants_with_data() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "tenant-a").await;
        create_test_tenant(&service, "tenant-b").await;
        create_test_tenant(&service, "tenant-c").await;

        let request = ListTenantsRequest::default();
        let response = service.list_tenants(request).await.unwrap();

        assert_eq!(response.tenants.len(), 3);
        assert_eq!(response.tenants[0].name.id(), "tenant-a");
        assert_eq!(response.tenants[1].name.id(), "tenant-b");
        assert_eq!(response.tenants[2].name.id(), "tenant-c");
    }

    #[tokio::test]
    async fn test_list_tenants_pagination() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "tenant-a").await;
        create_test_tenant(&service, "tenant-b").await;
        create_test_tenant(&service, "tenant-c").await;

        let request = ListTenantsRequest {
            page_size: Some(2),
            page_token: None,
        };
        let response = service.list_tenants(request).await.unwrap();

        assert_eq!(response.tenants.len(), 2);
        assert_eq!(response.tenants[0].name.id(), "tenant-a");
        assert_eq!(response.tenants[1].name.id(), "tenant-b");
        assert!(response.next_page_token.is_some());

        let request = ListTenantsRequest {
            page_size: Some(2),
            page_token: response.next_page_token,
        };
        let response = service.list_tenants(request).await.unwrap();

        assert_eq!(response.tenants.len(), 1);
        assert_eq!(response.tenants[0].name.id(), "tenant-c");
        assert!(response.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_delete_tenant() {
        let service = InMemoryAdminService::new();
        let tenant_name = TenantName::new("test-tenant").unwrap();
        service.create_tenant(tenant_name.clone()).await.unwrap();

        service.delete_tenant(tenant_name.clone()).await.unwrap();

        // Verify tenant is deleted
        let result = service.get_tenant(tenant_name).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::NotFound {
                resource: "tenant",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_delete_tenant_not_found() {
        let service = InMemoryAdminService::new();
        let tenant_name = TenantName::new("nonexistent").unwrap();

        let result = service.delete_tenant(tenant_name).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::NotFound {
                resource: "tenant",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_delete_tenant_with_namespaces() {
        let service = InMemoryAdminService::new();
        let tenant_name = TenantName::new("test-tenant").unwrap();
        service.create_tenant(tenant_name.clone()).await.unwrap();

        // Manually add a namespace to simulate the tenant having namespaces
        // (since we haven't implemented namespace creation yet)
        let namespace_name = NamespaceName::new("test-namespace", tenant_name.clone()).unwrap();
        let namespace = Namespace::new(
            namespace_name.clone(),
            NamespaceOptions::new(SecretName::new("test-config").unwrap()),
        );
        {
            let mut store = service.store.write().await;
            store.namespaces.insert(namespace_name.name(), namespace);
        }

        let result = service.delete_tenant(tenant_name).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::InvalidArgument {
                resource: "tenant",
                ..
            }
        ));
    }

    // Namespace tests

    #[tokio::test]
    async fn test_create_namespace() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;

        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name.clone()).unwrap();
        let namespace = service
            .create_namespace(
                namespace_name.clone(),
                NamespaceOptions::new(SecretName::new("test-config").unwrap()),
            )
            .await
            .unwrap();

        assert_eq!(namespace.name, namespace_name);
        assert_eq!(namespace.name.id(), "test-namespace");
        assert_eq!(namespace.name.parent(), &tenant_name);
        assert_eq!(
            namespace.name.name(),
            "tenants/test-tenant/namespaces/test-namespace"
        );
    }

    #[tokio::test]
    async fn test_create_namespace_tenant_not_found() {
        let service = InMemoryAdminService::new();
        let tenant_name = TenantName::new("nonexistent-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();

        let result = service
            .create_namespace(
                namespace_name,
                NamespaceOptions::new(SecretName::new("test-config").unwrap()),
            )
            .await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::NotFound {
                resource: "tenant",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_create_namespace_already_exists() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;

        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();

        service
            .create_namespace(
                namespace_name.clone(),
                NamespaceOptions::new(SecretName::new("test-config").unwrap()),
            )
            .await
            .unwrap();

        // Try to create the same namespace again
        let result = service
            .create_namespace(
                namespace_name,
                NamespaceOptions::new(SecretName::new("test-config").unwrap()),
            )
            .await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::AlreadyExists {
                resource: "namespace",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_get_namespace() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;

        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let created_namespace = service
            .create_namespace(
                namespace_name.clone(),
                NamespaceOptions::new(SecretName::new("test-config").unwrap()),
            )
            .await
            .unwrap();

        let retrieved_namespace = service.get_namespace(namespace_name).await.unwrap();
        assert_eq!(created_namespace, retrieved_namespace);
    }

    #[tokio::test]
    async fn test_get_namespace_not_found() {
        let service = InMemoryAdminService::new();
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("nonexistent", tenant_name).unwrap();

        let result = service.get_namespace(namespace_name).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::NotFound {
                resource: "namespace",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_list_namespaces_empty() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;

        let request = ListNamespacesRequest::new(TenantName::new("test-tenant").unwrap());
        let response = service.list_namespaces(request).await.unwrap();

        assert!(response.namespaces.is_empty());
        assert!(response.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_list_namespaces_with_data() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;
        create_test_namespace(&service, "test-tenant", "namespace-a").await;
        create_test_namespace(&service, "test-tenant", "namespace-b").await;
        create_test_namespace(&service, "test-tenant", "namespace-c").await;

        let request = ListNamespacesRequest::new(TenantName::new("test-tenant").unwrap());
        let response = service.list_namespaces(request).await.unwrap();

        assert_eq!(response.namespaces.len(), 3);
        let names: Vec<_> = response.namespaces.iter().map(|ns| ns.name.id()).collect();
        assert_eq!(names, vec!["namespace-a", "namespace-b", "namespace-c"]);
    }

    #[tokio::test]
    async fn test_list_namespaces_tenant_not_found() {
        let service = InMemoryAdminService::new();
        let request = ListNamespacesRequest::new(TenantName::new("nonexistent-tenant").unwrap());

        let result = service.list_namespaces(request).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::NotFound {
                resource: "tenant",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_list_namespaces_multiple_tenants() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "tenant-a").await;
        create_test_tenant(&service, "tenant-b").await;
        create_test_namespace(&service, "tenant-a", "namespace-1").await;
        create_test_namespace(&service, "tenant-a", "namespace-2").await;
        create_test_namespace(&service, "tenant-b", "namespace-3").await;

        // List namespaces for tenant-a
        let request = ListNamespacesRequest::new(TenantName::new("tenant-a").unwrap());
        let response = service.list_namespaces(request).await.unwrap();

        assert_eq!(response.namespaces.len(), 2);
        let names: Vec<_> = response.namespaces.iter().map(|ns| ns.name.id()).collect();
        assert_eq!(names, vec!["namespace-1", "namespace-2"]);

        // List namespaces for tenant-b
        let request = ListNamespacesRequest::new(TenantName::new("tenant-b").unwrap());
        let response = service.list_namespaces(request).await.unwrap();

        assert_eq!(response.namespaces.len(), 1);
        assert_eq!(response.namespaces[0].name.id(), "namespace-3");
    }

    #[tokio::test]
    async fn test_delete_namespace() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;

        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        service
            .create_namespace(
                namespace_name.clone(),
                NamespaceOptions::new(SecretName::new("test-config").unwrap()),
            )
            .await
            .unwrap();

        service
            .delete_namespace(namespace_name.clone())
            .await
            .unwrap();

        // Verify namespace is deleted
        let result = service.get_namespace(namespace_name).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::NotFound {
                resource: "namespace",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_delete_namespace_not_found() {
        let service = InMemoryAdminService::new();
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("nonexistent", tenant_name).unwrap();

        let result = service.delete_namespace(namespace_name).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::NotFound {
                resource: "namespace",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_delete_namespace_with_topics() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;

        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        service
            .create_namespace(
                namespace_name.clone(),
                NamespaceOptions::new(SecretName::new("test-config").unwrap()),
            )
            .await
            .unwrap();

        // Manually add a topic to simulate the namespace having topics
        // (since we haven't implemented topic creation yet)
        use crate::admin::TopicName;
        let topic_name = TopicName::new("test-topic", namespace_name.clone()).unwrap();
        let topic_options = TopicOptions::new(vec![Field::new("key", DataType::Utf8, false)]);
        let topic = service
            .create_topic(topic_name.clone(), topic_options)
            .await
            .unwrap();
        {
            let mut store = service.store.write().await;
            store.topics.insert(topic_name.name(), topic);
        }

        let result = service.delete_namespace(namespace_name).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::InvalidArgument {
                resource: "namespace",
                ..
            }
        ));
    }

    // Topic tests

    #[tokio::test]
    async fn test_create_topic() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;
        create_test_namespace(&service, "test-tenant", "test-namespace").await;

        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name.clone()).unwrap();
        let topic_options = TopicOptions::new(vec![Field::new("key", DataType::Utf8, false)]);
        let topic = service
            .create_topic(topic_name.clone(), topic_options)
            .await
            .unwrap();

        assert_eq!(topic.name, topic_name);
        assert_eq!(topic.name.id(), "test-topic");
        assert_eq!(topic.name.parent(), &namespace_name);
        assert_eq!(
            topic.name.name(),
            "tenants/test-tenant/namespaces/test-namespace/topics/test-topic"
        );
    }

    #[tokio::test]
    async fn test_create_topic_namespace_not_found() {
        let service = InMemoryAdminService::new();
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("nonexistent-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();

        let topic_options = TopicOptions::new(vec![Field::new("key", DataType::Utf8, false)]);
        let result = service.create_topic(topic_name, topic_options).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::NotFound {
                resource: "namespace",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_create_topic_already_exists() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;
        create_test_namespace(&service, "test-tenant", "test-namespace").await;

        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();

        let topic_options = TopicOptions::new(vec![Field::new("key", DataType::Utf8, false)]);
        service
            .create_topic(topic_name.clone(), topic_options.clone())
            .await
            .unwrap();

        // Try to create the same topic again
        let result = service.create_topic(topic_name, topic_options).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::AlreadyExists {
                resource: "topic",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_get_topic() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;
        create_test_namespace(&service, "test-tenant", "test-namespace").await;

        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let topic_options = TopicOptions::new(vec![Field::new("key", DataType::Utf8, false)]);
        let created_topic = service
            .create_topic(topic_name.clone(), topic_options)
            .await
            .unwrap();

        let retrieved_topic = service.get_topic(topic_name).await.unwrap();
        assert_eq!(created_topic, retrieved_topic);
    }

    #[tokio::test]
    async fn test_get_topic_not_found() {
        let service = InMemoryAdminService::new();
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("nonexistent", namespace_name).unwrap();

        let result = service.get_topic(topic_name).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::NotFound {
                resource: "topic",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_list_topics_empty() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;
        create_test_namespace(&service, "test-tenant", "test-namespace").await;

        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let request = ListTopicsRequest::new(namespace_name);
        let response = service.list_topics(request).await.unwrap();

        assert!(response.topics.is_empty());
        assert!(response.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_list_topics_with_data() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;
        create_test_namespace(&service, "test-tenant", "test-namespace").await;
        create_test_topic(&service, "test-tenant", "test-namespace", "topic-a").await;
        create_test_topic(&service, "test-tenant", "test-namespace", "topic-b").await;
        create_test_topic(&service, "test-tenant", "test-namespace", "topic-c").await;

        let tenant_name = TenantName::new_unchecked("test-tenant");
        let namespace_name = NamespaceName::new_unchecked("test-namespace", tenant_name);
        let request = ListTopicsRequest::new(namespace_name);
        let response = service.list_topics(request).await.unwrap();

        assert_eq!(response.topics.len(), 3);
        let names: Vec<_> = response
            .topics
            .iter()
            .map(|topic| topic.name.id())
            .collect();
        assert_eq!(names, vec!["topic-a", "topic-b", "topic-c"]);
    }

    #[tokio::test]
    async fn test_list_topics_namespace_not_found() {
        let service = InMemoryAdminService::new();
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("nonexistent-namespace", tenant_name).unwrap();
        let request = ListTopicsRequest::new(namespace_name);

        let result = service.list_topics(request).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::NotFound {
                resource: "namespace",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_list_topics_multiple_namespaces() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;
        create_test_namespace(&service, "test-tenant", "namespace-a").await;
        create_test_namespace(&service, "test-tenant", "namespace-b").await;
        create_test_topic(&service, "test-tenant", "namespace-a", "topic-1").await;
        create_test_topic(&service, "test-tenant", "namespace-a", "topic-2").await;
        create_test_topic(&service, "test-tenant", "namespace-b", "topic-3").await;

        // List topics for namespace-a
        let tenant_name = TenantName::new_unchecked("test-tenant");
        let namespace_a = NamespaceName::new_unchecked("namespace-a", tenant_name.clone());
        let request = ListTopicsRequest::new(namespace_a);
        let response = service.list_topics(request).await.unwrap();

        assert_eq!(response.topics.len(), 2);
        let names: Vec<_> = response
            .topics
            .iter()
            .map(|topic| topic.name.id())
            .collect();
        assert_eq!(names, vec!["topic-1", "topic-2"]);

        // List topics for namespace-b
        let namespace_b = NamespaceName::new_unchecked("namespace-b", tenant_name);
        let request = ListTopicsRequest::new(namespace_b);
        let response = service.list_topics(request).await.unwrap();

        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].name.id(), "topic-3");
    }

    #[tokio::test]
    async fn test_delete_topic() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;
        create_test_namespace(&service, "test-tenant", "test-namespace").await;

        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name.clone()).unwrap();
        let topic_options = TopicOptions::new(vec![Field::new("key", DataType::Utf8, false)]);
        service
            .create_topic(topic_name.clone(), topic_options)
            .await
            .unwrap();

        service
            .delete_topic(topic_name.clone(), false)
            .await
            .unwrap();

        // Verify topic is deleted
        let result = service.get_topic(topic_name).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::NotFound {
                resource: "topic",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_delete_topic_not_found() {
        let service = InMemoryAdminService::new();
        let tenant_name = TenantName::new_unchecked("test-tenant");
        let namespace_name = NamespaceName::new_unchecked("test-namespace", tenant_name);
        let topic_name = TopicName::new_unchecked("nonexistent", namespace_name);

        let result = service.delete_topic(topic_name, false).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::NotFound {
                resource: "topic",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_delete_topic_with_force() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;
        create_test_namespace(&service, "test-tenant", "test-namespace").await;

        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name.clone()).unwrap();
        let topic_options = TopicOptions::new(vec![Field::new("key", DataType::Utf8, false)]);

        service
            .create_topic(topic_name.clone(), topic_options)
            .await
            .unwrap();

        service
            .delete_topic(topic_name.clone(), true)
            .await
            .unwrap();

        // Verify topic is deleted
        let result = service.get_topic(topic_name).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::NotFound {
                resource: "topic",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_create_topic_with_valid_partition_key() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;
        create_test_namespace(&service, "test-tenant", "test-namespace").await;

        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();

        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ];
        let topic_options = TopicOptions::new_with_partition_key(fields, Some(0));

        let topic = service
            .create_topic(topic_name.clone(), topic_options)
            .await
            .unwrap();

        assert_eq!(topic.name, topic_name);
        assert_eq!(topic.fields.len(), 2);
        assert_eq!(topic.partition_key, Some(0));
    }

    #[tokio::test]
    async fn test_create_topic_with_invalid_partition_key() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;
        create_test_namespace(&service, "test-tenant", "test-namespace").await;

        let tenant_name = TenantName::new_unchecked("test-tenant");
        let namespace_name = NamespaceName::new_unchecked("test-namespace", tenant_name);
        let topic_name = TopicName::new_unchecked("test-topic", namespace_name);

        let fields = vec![Field::new("name", DataType::Utf8, false)];
        let topic_options = TopicOptions::new_with_partition_key(fields, Some(5)); // Invalid index

        let result = service.create_topic(topic_name, topic_options).await;
        assert!(matches!(
            result.unwrap_err().current_context(),
            AdminError::InvalidArgument {
                resource: "topic",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_create_topic_without_partition_key() {
        let service = InMemoryAdminService::new();
        create_test_tenant(&service, "test-tenant").await;
        create_test_namespace(&service, "test-tenant", "test-namespace").await;

        let tenant_name = TenantName::new_unchecked("test-tenant");
        let namespace_name = NamespaceName::new_unchecked("test-namespace", tenant_name);
        let topic_name = TopicName::new_unchecked("test-topic", namespace_name);

        let fields = vec![Field::new("data", DataType::Utf8, false)];
        let topic_options = TopicOptions::new(fields);

        let topic = service
            .create_topic(topic_name.clone(), topic_options)
            .await
            .unwrap();

        assert_eq!(topic.name, topic_name);
        assert_eq!(topic.fields.len(), 1);
        assert_eq!(topic.partition_key, None);
    }
}
