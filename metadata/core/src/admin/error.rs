use thiserror::Error;

use crate::resource::ResourceError;

/// Errors that can occur during admin operations.
#[derive(Error, Debug)]
pub enum AdminError {
    #[error("tenant not found: {tenant_id}")]
    TenantNotFound { tenant_id: String },
    #[error("tenant already exists: {tenant_id}")]
    TenantAlreadyExists { tenant_id: String },
    #[error("tenant has namespaces and cannot be deleted: {tenant_id}")]
    TenantNotEmpty { tenant_id: String },

    #[error("namespace not found: {namespace_id}")]
    NamespaceNotFound { namespace_id: String },
    #[error("namespace already exists: {namespace_id}")]
    NamespaceAlreadyExists { namespace_id: String },
    #[error("namespace has topics and cannot be deleted: {namespace_id}")]
    NamespaceNotEmpty { namespace_id: String },

    #[error("topic not found: {topic_id}")]
    TopicNotFound { topic_id: String },
    #[error("topic already exists: {topic_id}")]
    TopicAlreadyExists { topic_id: String },

    #[error("invalid resource name: {name}")]
    InvalidResourceName { name: String },
    #[error("invalid page size: {size}, must be between 1 and 1000")]
    InvalidPageSize { size: i32 },
    #[error("invalid page token: {token}")]
    InvalidPageToken { token: String },

    #[error("internal error")]
    Internal,
    #[error("operation not supported")]
    NotSupported,

    #[error("invalid resource name")]
    InvalidResource(#[from] ResourceError),
}

pub type AdminResult<T> = error_stack::Result<T, AdminError>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::ResourceError;

    #[test]
    fn test_resource_error_conversion() {
        let resource_error = ResourceError::InvalidFormat {
            expected: "tenants/{id}".to_string(),
            actual: "invalid".to_string(),
        };

        let admin_error: AdminError = resource_error.into();
        assert!(matches!(admin_error, AdminError::InvalidResource(_)));

        let error_message = admin_error.to_string();
        assert!(error_message.contains("invalid resource name"));
    }

    #[test]
    fn test_resource_error_from_parse() {
        // This would be how it's used in practice
        let result = "invalid-name".parse::<crate::admin::TenantName>();
        let admin_result: AdminResult<crate::admin::TenantName> =
            result.map_err(|e| error_stack::Report::new(AdminError::from(e)));

        assert!(admin_result.is_err());
    }
}
