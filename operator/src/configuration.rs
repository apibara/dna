static CONSOLE_IMAGE: &str = "quay.io/apibara/sink-console:latest";
static MONGO_IMAGE: &str = "quay.io/apibara/sink-mongo:latest";
static PARQUET_IMAGE: &str = "quay.io/apibara/sink-parquet:latest";
static POSTGRES_IMAGE: &str = "quay.io/apibara/sink-postgres:latest";
static WEBHOOK_IMAGE: &str = "quay.io/apibara/sink-webhook:latest";

#[derive(Debug, Clone)]
pub struct Configuration {
    /// Console sink configuration.
    pub console: SinkConfiguration,
    /// MongoDB sink configuration.
    pub mongo: SinkConfiguration,
    /// Parquet sink configuration.
    pub parquet: SinkConfiguration,
    /// PostgreSQL sink configuration.
    pub postgres: SinkConfiguration,
    /// Webhook sink configuration.
    pub webhook: SinkConfiguration,
}

#[derive(Debug, Clone)]
pub struct SinkConfiguration {
    /// The container image to use for the sink container.
    pub image: String,
}

impl Default for Configuration {
    fn default() -> Self {
        let console = SinkConfiguration {
            image: CONSOLE_IMAGE.to_string(),
        };
        let mongo = SinkConfiguration {
            image: MONGO_IMAGE.to_string(),
        };
        let parquet = SinkConfiguration {
            image: PARQUET_IMAGE.to_string(),
        };
        let postgres = SinkConfiguration {
            image: POSTGRES_IMAGE.to_string(),
        };
        let webhook = SinkConfiguration {
            image: WEBHOOK_IMAGE.to_string(),
        };

        Configuration {
            console,
            mongo,
            parquet,
            postgres,
            webhook,
        }
    }
}
