use thiserror::Error;

use crate::resource::ResourceError;

/// Errors that can occur during admin operations.
#[derive(Error, Debug)]
pub enum AdminError {
    #[error("{resource} not found: {message}")]
    NotFound {
        resource: &'static str,
        message: String,
    },
    #[error("{resource} already exists: {message}")]
    AlreadyExists {
        resource: &'static str,
        message: String,
    },
    #[error("invalid {resource} argument: {message}")]
    InvalidArgument {
        resource: &'static str,
        message: String,
    },

    #[error("invalid resource name")]
    InvalidResourceName(#[from] ResourceError),
    #[error("internal error: {message}")]
    Internal { message: String },
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
        assert!(matches!(admin_error, AdminError::InvalidResourceName(_)));

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
