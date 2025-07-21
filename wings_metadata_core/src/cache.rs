use std::{sync::Arc, time::Duration};

use crate::admin::{Admin, AdminResult, NamespaceName, NamespaceRef, TopicName, TopicRef};

pub struct CacheOptions {
    max_capacity: usize,
    time_to_live: Duration,
    time_to_idle: Duration,
}

#[derive(Clone)]
pub struct TopicCache {
    admin: Arc<dyn Admin>,
    inner: moka::future::Cache<TopicName, TopicRef>,
}

#[derive(Clone)]
pub struct NamespaceCache {
    admin: Arc<dyn Admin>,
    inner: moka::future::Cache<NamespaceName, NamespaceRef>,
}

impl TopicCache {
    pub fn new(admin: Arc<dyn Admin>) -> Self {
        Self::with_options(admin, CacheOptions::default())
    }

    pub fn with_options(admin: Arc<dyn Admin>, options: CacheOptions) -> Self {
        let inner = moka::future::Cache::builder()
            .max_capacity(options.max_capacity as u64)
            .time_to_live(options.time_to_live)
            .time_to_idle(options.time_to_idle)
            .build();

        Self { admin, inner }
    }

    pub async fn get(&self, name: TopicName) -> AdminResult<TopicRef> {
        let admin = self.admin.clone();
        let topic = self
            .inner
            .try_get_with(name.clone(), async move {
                admin.get_topic(name).await.map(Arc::new)
            })
            .await
            .map_err(|e| {
                Arc::try_unwrap(e).unwrap_or_else(|e| {
                    use crate::admin::AdminError;
                    AdminError::Internal {
                        message: format!("Failed to unwrap Arc: {}", e),
                    }
                    .into()
                })
            })?;
        Ok(topic)
    }
}

impl NamespaceCache {
    pub fn new(admin: Arc<dyn Admin>) -> Self {
        Self::with_options(admin, CacheOptions::default())
    }

    pub fn with_options(admin: Arc<dyn Admin>, options: CacheOptions) -> Self {
        let inner = moka::future::Cache::builder()
            .max_capacity(options.max_capacity as u64)
            .time_to_live(options.time_to_live)
            .time_to_idle(options.time_to_idle)
            .build();

        Self { admin, inner }
    }

    pub async fn get(&self, name: NamespaceName) -> AdminResult<NamespaceRef> {
        let admin = self.admin.clone();
        let namespace = self
            .inner
            .try_get_with(name.clone(), async move {
                admin.get_namespace(name).await.map(Arc::new)
            })
            .await
            .map_err(|e| {
                Arc::try_unwrap(e).unwrap_or_else(|e| {
                    use crate::admin::AdminError;
                    AdminError::Internal {
                        message: format!("Failed to unwrap Arc: {}", e),
                    }
                    .into()
                })
            })?;
        Ok(namespace)
    }
}

impl CacheOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_capacity(mut self, max_capacity: usize) -> Self {
        self.max_capacity = max_capacity;
        self
    }

    pub fn with_time_to_live(mut self, time_to_live: Duration) -> Self {
        self.time_to_live = time_to_live;
        self
    }

    pub fn with_time_to_idle(mut self, time_to_idle: Duration) -> Self {
        self.time_to_idle = time_to_idle;
        self
    }
}

impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            max_capacity: 1024,
            time_to_live: Duration::from_secs(30 * 60), // 30 minutes
            time_to_idle: Duration::from_secs(5 * 60),  // 5 minutes
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::memory::InMemoryAdminService;
    use crate::admin::{NamespaceOptions, SecretName, TenantName, TopicOptions};
    use arrow::datatypes::{DataType, Field};
    use std::time::Duration;

    #[tokio::test]
    async fn test_topic_cache_basic_functionality() {
        let admin = Arc::new(InMemoryAdminService::new());
        let cache = TopicCache::new(admin.clone());

        // Create test data
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name.clone()).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name.clone()).unwrap();

        admin.create_tenant(tenant_name).await.unwrap();
        admin
            .create_namespace(
                namespace_name,
                NamespaceOptions::new(SecretName::new("test-config").unwrap()),
            )
            .await
            .unwrap();

        let topic_options = TopicOptions::new(vec![Field::new("key", DataType::Utf8, false)]);
        let expected_topic = admin
            .create_topic(topic_name.clone(), topic_options)
            .await
            .unwrap();

        // Test cache get - should fetch from admin service
        let cached_topic = cache.get(topic_name.clone()).await.unwrap();
        assert_eq!(cached_topic.name.id(), expected_topic.name.id());
        assert_eq!(
            cached_topic.name.parent().id(),
            expected_topic.name.parent().id()
        );

        // Test cache get again - should return from cache
        let cached_topic_2 = cache.get(topic_name).await.unwrap();
        assert_eq!(cached_topic_2.name.id(), expected_topic.name.id());
    }

    #[tokio::test]
    async fn test_namespace_cache_basic_functionality() {
        let admin = Arc::new(InMemoryAdminService::new());
        let cache = NamespaceCache::new(admin.clone());

        // Create test data
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name.clone()).unwrap();

        admin.create_tenant(tenant_name).await.unwrap();
        let expected_namespace = admin
            .create_namespace(
                namespace_name.clone(),
                NamespaceOptions::new(SecretName::new("test-config").unwrap()),
            )
            .await
            .unwrap();

        // Test cache get - should fetch from admin service
        let cached_namespace = cache.get(namespace_name.clone()).await.unwrap();
        assert_eq!(cached_namespace.name.id(), expected_namespace.name.id());
        assert_eq!(
            cached_namespace.name.parent().id(),
            expected_namespace.name.parent().id()
        );

        // Test cache get again - should return from cache
        let cached_namespace_2 = cache.get(namespace_name).await.unwrap();
        assert_eq!(cached_namespace_2.name.id(), expected_namespace.name.id());
    }

    #[tokio::test]
    async fn test_cache_with_custom_options() {
        let admin = Arc::new(InMemoryAdminService::new());
        let options = CacheOptions::new()
            .with_max_capacity(50)
            .with_time_to_live(Duration::from_secs(60))
            .with_time_to_idle(Duration::from_secs(30));

        let cache = TopicCache::with_options(admin.clone(), options);

        // Create test data
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name.clone()).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name.clone()).unwrap();

        admin.create_tenant(tenant_name).await.unwrap();
        admin
            .create_namespace(
                namespace_name,
                NamespaceOptions::new(SecretName::new("test-config").unwrap()),
            )
            .await
            .unwrap();

        let topic_options = TopicOptions::new(vec![Field::new("key", DataType::Utf8, false)]);
        let expected_topic = admin
            .create_topic(topic_name.clone(), topic_options)
            .await
            .unwrap();

        // Test cache get works with custom options
        let cached_topic = cache.get(topic_name).await.unwrap();
        assert_eq!(cached_topic.name.id(), expected_topic.name.id());
    }

    #[tokio::test]
    async fn test_cache_error_handling() {
        let admin = Arc::new(InMemoryAdminService::new());
        let cache = TopicCache::new(admin);

        // Try to get a non-existent topic
        let tenant_name = TenantName::new("non-existent-tenant").unwrap();
        let namespace_name = NamespaceName::new("non-existent-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("non-existent-topic", namespace_name).unwrap();

        let result = cache.get(topic_name).await;
        assert!(result.is_err());
    }
}
