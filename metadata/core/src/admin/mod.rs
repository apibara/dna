//! The metadata admin trait and related types.

pub mod error;
pub mod types;

pub use error::{AdminError, AdminResult};
pub use types::*;

use async_trait::async_trait;

/// The Admin trait provides methods for managing tenants, namespaces, and topics.
///
/// This trait models metadata operations and will be implemented by:
/// - `InMemoryAdminService`: stores everything in memory for testing and development
/// - `RemoteAdminService`: communicates with a remote admin service via gRPC
#[async_trait]
pub trait Admin: Send + Sync {
    // Tenant operations

    /// Create a new tenant.
    async fn create_tenant(&self, request: CreateTenantRequest) -> AdminResult<Tenant>;

    /// Return the specified tenant.
    async fn get_tenant(&self, request: GetTenantRequest) -> AdminResult<Tenant>;

    /// List all tenants.
    async fn list_tenants(&self, request: ListTenantsRequest) -> AdminResult<ListTenantsResponse>;

    /// Delete a tenant.
    ///
    /// The request fails if the tenant has any namespace.
    async fn delete_tenant(&self, request: DeleteTenantRequest) -> AdminResult<()>;

    // Namespace operations

    /// Create a new namespace belonging to a tenant.
    async fn create_namespace(&self, request: CreateNamespaceRequest) -> AdminResult<Namespace>;

    /// Return the specified namespace.
    async fn get_namespace(&self, request: GetNamespaceRequest) -> AdminResult<Namespace>;

    /// List all namespaces belonging to a tenant.
    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> AdminResult<ListNamespacesResponse>;

    /// Delete a namespace.
    ///
    /// The request fails if the namespace has any topic.
    async fn delete_namespace(&self, request: DeleteNamespaceRequest) -> AdminResult<()>;

    // Topic operations

    /// Create a new topic belonging to a namespace.
    async fn create_topic(&self, request: CreateTopicRequest) -> AdminResult<Topic>;

    /// Return the specified topic.
    async fn get_topic(&self, request: GetTopicRequest) -> AdminResult<Topic>;

    /// List all topics belonging to a namespace.
    async fn list_topics(&self, request: ListTopicsRequest) -> AdminResult<ListTopicsResponse>;

    /// Delete a topic.
    ///
    /// This operation may take a long time to complete as it involves deleting
    /// data from object storage when force is true.
    async fn delete_topic(&self, request: DeleteTopicRequest) -> AdminResult<()>;
}
