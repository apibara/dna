//! Type-safe resource identifiers for Wings metadata.
//!
//! This module provides a macro to generate type-safe resource types that prevent
//! mixing up different resource identifiers and ensure proper hierarchical relationships.

use thiserror::Error;

/// Errors that can occur when parsing resource names.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ResourceError {
    #[error("invalid resource name format: expected '{expected}' but got '{actual}'")]
    InvalidFormat { expected: String, actual: String },
    #[error("invalid resource name: '{name}' does not match expected pattern")]
    InvalidName { name: String },
    #[error("missing parent resource in name: '{name}'")]
    MissingParent { name: String },
}

pub type ResourceResult<T> = Result<T, ResourceError>;

/// Macro to generate type-safe resource types.
///
/// This macro generates structs for resource names that ensure type safety and
/// proper hierarchical relationships between resources.
///
/// # Examples
///
/// ```rust
/// use wings_metadata_core::resource_type;
///
/// // Generate a root resource type
/// resource_type!(Tenant, "tenants");
///
/// // Generate a child resource type
/// resource_type!(Namespace, "namespaces", Tenant);
/// resource_type!(Topic, "topics", Namespace);
/// ```
#[macro_export]
macro_rules! resource_type {
    // Root resource (no parent)
    ($name:ident, $prefix:literal) => {
        paste::paste! {
            #[doc = "Type-safe identifier for a " $name " resource."]
            #[derive(Debug, Clone, PartialEq, Eq, Hash)]
            pub struct [<$name Name>] {
                /// The resource ID.
                pub id: String,
            }

            impl [<$name Name>] {
                #[doc = "Create a new " $name " resource identifier."]
                pub fn new(id: impl Into<String>) -> Self {
                    Self {
                        id: id.into(),
                    }
                }

                #[doc = "Get the full resource name."]
                pub fn name(&self) -> String {
                    format!("{}/{}", $prefix, self.id)
                }

                #[doc = "Parse a resource name into a " $name " identifier."]
                pub fn parse(name: &str) -> $crate::resource::ResourceResult<Self> {
                    let expected_prefix = concat!($prefix, "/");
                    name.strip_prefix(expected_prefix)
                        .map(|id| Self::new(id.to_string()))
                        .ok_or_else(|| $crate::resource::ResourceError::InvalidFormat {
                            expected: format!("{}/{{id}}", $prefix),
                            actual: name.to_string(),
                        })
                }

                #[doc = "Get the resource ID."]
                pub fn id(&self) -> &str {
                    &self.id
                }
            }

            impl std::fmt::Display for [<$name Name>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}", self.name())
                }
            }

            impl std::str::FromStr for [<$name Name>] {
                type Err = $crate::resource::ResourceError;

                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    Self::parse(s)
                }
            }
        }
    };

    // Child resource (with parent)
    ($name:ident, $prefix:literal, $parent:ident) => {
        paste::paste! {
            #[doc = "Type-safe identifier for a " $name " resource."]
            #[derive(Debug, Clone, PartialEq, Eq, Hash)]
            pub struct [<$name Name>] {
                /// The resource ID.
                pub id: String,
                /// The parent resource.
                pub parent: [<$parent Name>],
            }

            impl [<$name Name>] {
                #[doc = "Create a new " $name " resource identifier."]
                pub fn new(id: impl Into<String>, parent: [<$parent Name>]) -> Self {
                    Self {
                        id: id.into(),
                        parent,
                    }
                }

                #[doc = "Get the full resource name."]
                pub fn name(&self) -> String {
                    format!("{}/{}/{}", self.parent.name(), $prefix, self.id)
                }

                #[doc = "Parse a resource name into a " $name " identifier."]
                pub fn parse(name: &str) -> $crate::resource::ResourceResult<Self> {
                    let parts: Vec<&str> = name.split('/').collect();
                    if parts.len() >= 4 && parts[parts.len() - 2] == $prefix {
                        let id = parts[parts.len() - 1].to_string();
                        let parent_parts = &parts[..parts.len() - 2];
                        let parent_name = parent_parts.join("/");
                        [<$parent Name>]::parse(&parent_name)
                            .map(|parent| Self::new(id, parent))
                            .map_err(|_| $crate::resource::ResourceError::MissingParent {
                                name: name.to_string(),
                            })
                    } else {
                        Err($crate::resource::ResourceError::InvalidFormat {
                            expected: format!("{{parent}}/{}/{{id}}", $prefix),
                            actual: name.to_string(),
                        })
                    }
                }

                #[doc = "Get the resource ID."]
                pub fn id(&self) -> &str {
                    &self.id
                }

                #[doc = "Get the parent resource."]
                pub fn parent(&self) -> &[<$parent Name>] {
                    &self.parent
                }
            }

            impl std::fmt::Display for [<$name Name>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}", self.name())
                }
            }

            impl std::str::FromStr for [<$name Name>] {
                type Err = $crate::resource::ResourceError;

                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    Self::parse(s)
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test resource types
    resource_type!(TestTenant, "tenants");
    resource_type!(TestNamespace, "namespaces", TestTenant);
    resource_type!(TestTopic, "topics", TestNamespace);

    #[test]
    fn test_tenant_name() {
        let tenant = TestTenantName::new("test-tenant");
        assert_eq!(tenant.id(), "test-tenant");
        assert_eq!(tenant.name(), "tenants/test-tenant");
        assert_eq!(tenant.to_string(), "tenants/test-tenant");

        let parsed = TestTenantName::parse("tenants/test-tenant").unwrap();
        assert_eq!(parsed, tenant);

        let from_str: TestTenantName = "tenants/test-tenant".parse().unwrap();
        assert_eq!(from_str, tenant);
    }

    #[test]
    fn test_namespace_name() {
        let tenant = TestTenantName::new("test-tenant");
        let namespace = TestNamespaceName::new("test-namespace", tenant.clone());

        assert_eq!(namespace.id(), "test-namespace");
        assert_eq!(namespace.parent(), &tenant);
        assert_eq!(
            namespace.name(),
            "tenants/test-tenant/namespaces/test-namespace"
        );
        assert_eq!(
            namespace.to_string(),
            "tenants/test-tenant/namespaces/test-namespace"
        );

        let parsed =
            TestNamespaceName::parse("tenants/test-tenant/namespaces/test-namespace").unwrap();
        assert_eq!(parsed, namespace);

        let from_str: TestNamespaceName = "tenants/test-tenant/namespaces/test-namespace"
            .parse()
            .unwrap();
        assert_eq!(from_str, namespace);
    }

    #[test]
    fn test_topic_name() {
        let tenant = TestTenantName::new("test-tenant");
        let namespace = TestNamespaceName::new("test-namespace", tenant);
        let topic = TestTopicName::new("test-topic", namespace.clone());

        assert_eq!(topic.id(), "test-topic");
        assert_eq!(topic.parent(), &namespace);
        assert_eq!(
            topic.name(),
            "tenants/test-tenant/namespaces/test-namespace/topics/test-topic"
        );
        assert_eq!(
            topic.to_string(),
            "tenants/test-tenant/namespaces/test-namespace/topics/test-topic"
        );

        let parsed =
            TestTopicName::parse("tenants/test-tenant/namespaces/test-namespace/topics/test-topic")
                .unwrap();
        assert_eq!(parsed, topic);

        let from_str: TestTopicName =
            "tenants/test-tenant/namespaces/test-namespace/topics/test-topic"
                .parse()
                .unwrap();
        assert_eq!(from_str, topic);
    }

    #[test]
    fn test_parsing_errors() {
        // Test invalid tenant name
        let result = TestTenantName::parse("invalid");
        assert!(matches!(result, Err(ResourceError::InvalidFormat { .. })));

        let result = TestTenantName::parse("namespaces/test");
        assert!(matches!(result, Err(ResourceError::InvalidFormat { .. })));

        // Test invalid namespace name
        let result = TestNamespaceName::parse("tenants/test");
        assert!(matches!(result, Err(ResourceError::InvalidFormat { .. })));

        let result = TestNamespaceName::parse("tenants/test/topics/test");
        assert!(matches!(result, Err(ResourceError::InvalidFormat { .. })));

        // Test invalid topic name
        let result = TestTopicName::parse("tenants/test/namespaces/test");
        assert!(matches!(result, Err(ResourceError::InvalidFormat { .. })));

        let result = TestTopicName::parse("invalid/path");
        assert!(matches!(result, Err(ResourceError::InvalidFormat { .. })));
    }

    #[test]
    fn test_from_str_errors() {
        let result: Result<TestTenantName, _> = "invalid".parse();
        assert!(result.is_err());

        let result: Result<TestNamespaceName, _> = "tenants/test".parse();
        assert!(result.is_err());

        let result: Result<TestTopicName, _> = "invalid/path".parse();
        assert!(result.is_err());
    }
}
