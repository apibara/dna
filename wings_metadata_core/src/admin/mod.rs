//! The metadata admin trait and related types.

pub mod error;
pub mod memory;
pub mod proto;
pub mod remote;
pub mod server;
pub mod types;

pub use error::{AdminError, AdminResult};
pub use memory::InMemoryAdminService;
pub use remote::RemoteAdminService;
pub use server::AdminService;
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
    async fn create_tenant(&self, name: TenantName) -> AdminResult<Tenant>;

    /// Return the specified tenant.
    async fn get_tenant(&self, name: TenantName) -> AdminResult<Tenant>;

    /// List all tenants.
    async fn list_tenants(&self, request: ListTenantsRequest) -> AdminResult<ListTenantsResponse>;

    /// Delete a tenant.
    ///
    /// The request fails if the tenant has any namespace.
    async fn delete_tenant(&self, name: TenantName) -> AdminResult<()>;

    // Namespace operations

    /// Create a new namespace belonging to a tenant.
    async fn create_namespace(
        &self,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> AdminResult<Namespace>;

    /// Return the specified namespace.
    async fn get_namespace(&self, name: NamespaceName) -> AdminResult<Namespace>;

    /// List all namespaces belonging to a tenant.
    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> AdminResult<ListNamespacesResponse>;

    /// Delete a namespace.
    ///
    /// The request fails if the namespace has any topic.
    async fn delete_namespace(&self, name: NamespaceName) -> AdminResult<()>;

    // Topic operations

    /// Create a new topic belonging to a namespace.
    async fn create_topic(&self, name: TopicName, options: TopicOptions) -> AdminResult<Topic>;

    /// Return the specified topic.
    async fn get_topic(&self, name: TopicName) -> AdminResult<Topic>;

    /// List all topics belonging to a namespace.
    async fn list_topics(&self, request: ListTopicsRequest) -> AdminResult<ListTopicsResponse>;

    /// Delete a topic.
    ///
    /// This operation may take a long time to complete as it involves deleting
    /// data from object storage.
    async fn delete_topic(&self, name: TopicName, force: bool) -> AdminResult<()>;
}
