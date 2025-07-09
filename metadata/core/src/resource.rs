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
    #[error(
        "invalid resource id: '{id}' - must be at least 1 character long, start with lowercase letter, and contain only lowercase letters, numbers, hyphens, and underscores"
    )]
    InvalidResourceId { id: String },
}

pub type ResourceResult<T> = Result<T, ResourceError>;

/// Validate a resource ID according to Wings naming conventions.
///
/// Valid resource IDs must:
/// - Be at least 1 character long
/// - Start with a lowercase letter [a-z]
/// - Contain only lowercase letters, numbers, hyphens (-), and underscores (_)
pub fn validate_resource_id(id: &str) -> ResourceResult<()> {
    if id.is_empty() {
        return Err(ResourceError::InvalidResourceId { id: id.to_string() });
    }

    let mut chars = id.chars();

    // First character must be a lowercase letter
    if let Some(first_char) = chars.next() {
        if !first_char.is_ascii_lowercase() {
            return Err(ResourceError::InvalidResourceId { id: id.to_string() });
        }
    }

    // Remaining characters must be lowercase letters, numbers, hyphens, or underscores
    for ch in chars {
        if !ch.is_ascii_lowercase() && !ch.is_ascii_digit() && ch != '-' && ch != '_' {
            return Err(ResourceError::InvalidResourceId { id: id.to_string() });
        }
    }

    Ok(())
}

/// Macro to generate type-safe resource types.
///
/// This macro generates structs for resource names that ensure type safety and
/// proper hierarchical relationships between resources.
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
                pub fn new(id: impl Into<String>) -> $crate::resource::ResourceResult<Self> {
                    let id = id.into();
                    $crate::resource::validate_resource_id(&id)?;
                    Ok(Self { id })
                }

                #[doc = "Create a new " $name " resource identifier without validation."]
                #[doc = ""]
                #[doc = "# Panics"]
                #[doc = ""]
                #[doc = "Panics if the resource ID is invalid."]
                pub fn new_unchecked(id: impl Into<String>) -> Self {
                    let id = id.into();
                    $crate::resource::validate_resource_id(&id)
                        .expect("resource id must be valid");
                    Self { id }
                }

                #[doc = "Get the full resource name."]
                pub fn name(&self) -> String {
                    format!("{}/{}", $prefix, self.id)
                }

                #[doc = "Parse a resource name into a " $name " identifier."]
                pub fn parse(name: &str) -> $crate::resource::ResourceResult<Self> {
                    let expected_prefix = concat!($prefix, "/");
                    if let Some(id) = name.strip_prefix(expected_prefix) {
                        $crate::resource::validate_resource_id(id)?;
                        Ok(Self { id: id.to_string() })
                    } else {
                        Err($crate::resource::ResourceError::InvalidFormat {
                            expected: format!("{}/{{id}}", $prefix),
                            actual: name.to_string(),
                        })
                    }
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
                pub fn new(id: impl Into<String>, parent: [<$parent Name>]) -> $crate::resource::ResourceResult<Self> {
                    let id = id.into();
                    $crate::resource::validate_resource_id(&id)?;
                    Ok(Self { id, parent })
                }

                #[doc = "Create a new " $name " resource identifier without validation."]
                #[doc = ""]
                #[doc = "# Panics"]
                #[doc = ""]
                #[doc = "Panics if the resource ID is invalid."]
                pub fn new_unchecked(id: impl Into<String>, parent: [<$parent Name>]) -> Self {
                    let id = id.into();
                    $crate::resource::validate_resource_id(&id)
                        .expect("resource id must be valid");
                    Self { id, parent }
                }

                #[doc = "Get the full resource name."]
                pub fn name(&self) -> String {
                    format!("{}/{}/{}", self.parent.name(), $prefix, self.id)
                }

                #[doc = "Parse a resource name into a " $name " identifier."]
                pub fn parse(name: &str) -> $crate::resource::ResourceResult<Self> {
                    let parts: Vec<&str> = name.split('/').collect();
                    if parts.len() >= 4 && parts[parts.len() - 2] == $prefix {
                        let id = parts[parts.len() - 1];
                        $crate::resource::validate_resource_id(id)?;
                        let parent_parts = &parts[..parts.len() - 2];
                        let parent_name = parent_parts.join("/");
                        match [<$parent Name>]::parse(&parent_name) {
                            Ok(parent) => Ok(Self { id: id.to_string(), parent }),
                            Err(_) => Err($crate::resource::ResourceError::MissingParent {
                                name: name.to_string(),
                            })
                        }
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
        let tenant = TestTenantName::new("test-tenant").unwrap();
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
        let tenant = TestTenantName::new("test-tenant").unwrap();
        let namespace = TestNamespaceName::new_unchecked("test-namespace", tenant.clone());

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
        let tenant = TestTenantName::new("test-tenant").unwrap();
        let namespace = TestNamespaceName::new("test-namespace", tenant).unwrap();
        let topic = TestTopicName::new_unchecked("test-topic", namespace.clone());

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

    #[test]
    fn test_resource_id_validation() {
        // Valid IDs
        assert!(TestTenantName::new("test").unwrap().id() == "test");
        assert!(TestTenantName::new("test-tenant").unwrap().id() == "test-tenant");
        assert!(TestTenantName::new("test_tenant").unwrap().id() == "test_tenant");
        assert!(TestTenantName::new("test123").unwrap().id() == "test123");
        assert!(TestTenantName::new("a").unwrap().id() == "a");
        assert!(TestTenantName::new("test-123_abc").unwrap().id() == "test-123_abc");
    }

    #[test]
    fn test_invalid_resource_id_new_returns_error() {
        let result = TestTenantName::new("");
        assert!(matches!(
            result,
            Err(ResourceError::InvalidResourceId { .. })
        ));

        let result = TestTenantName::new("123test");
        assert!(matches!(
            result,
            Err(ResourceError::InvalidResourceId { .. })
        ));

        let result = TestTenantName::new("Test");
        assert!(matches!(
            result,
            Err(ResourceError::InvalidResourceId { .. })
        ));

        let result = TestTenantName::new("testTenant");
        assert!(matches!(
            result,
            Err(ResourceError::InvalidResourceId { .. })
        ));

        let result = TestTenantName::new("test@tenant");
        assert!(matches!(
            result,
            Err(ResourceError::InvalidResourceId { .. })
        ));

        let result = TestTenantName::new("test tenant");
        assert!(matches!(
            result,
            Err(ResourceError::InvalidResourceId { .. })
        ));
    }

    #[test]
    #[should_panic(expected = "resource id must be valid")]
    fn test_invalid_resource_id_new_unchecked_empty() {
        TestTenantName::new_unchecked("");
    }

    #[test]
    #[should_panic(expected = "resource id must be valid")]
    fn test_invalid_resource_id_new_unchecked_starts_with_number() {
        TestTenantName::new_unchecked("123test");
    }

    #[test]
    #[should_panic(expected = "resource id must be valid")]
    fn test_invalid_resource_id_new_unchecked_starts_with_uppercase() {
        TestTenantName::new_unchecked("Test");
    }

    #[test]
    #[should_panic(expected = "resource id must be valid")]
    fn test_invalid_resource_id_new_unchecked_contains_uppercase() {
        TestTenantName::new_unchecked("testTenant");
    }

    #[test]
    #[should_panic(expected = "resource id must be valid")]
    fn test_invalid_resource_id_new_unchecked_contains_special_chars() {
        TestTenantName::new_unchecked("test@tenant");
    }

    #[test]
    #[should_panic(expected = "resource id must be valid")]
    fn test_invalid_resource_id_new_unchecked_contains_space() {
        TestTenantName::new_unchecked("test tenant");
    }

    #[test]
    fn test_parse_with_invalid_resource_id() {
        let result = TestTenantName::parse("tenants/123invalid");
        assert!(matches!(
            result,
            Err(ResourceError::InvalidResourceId { .. })
        ));

        let result = TestTenantName::parse("tenants/Test");
        assert!(matches!(
            result,
            Err(ResourceError::InvalidResourceId { .. })
        ));

        let result = TestTenantName::parse("tenants/test@invalid");
        assert!(matches!(
            result,
            Err(ResourceError::InvalidResourceId { .. })
        ));

        let result = TestTenantName::parse("tenants/");
        assert!(matches!(
            result,
            Err(ResourceError::InvalidResourceId { .. })
        ));
    }
}
