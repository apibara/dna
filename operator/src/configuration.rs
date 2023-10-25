use std::collections::HashMap;

static CONSOLE_IMAGE: &str = "quay.io/apibara/sink-console:latest";
static MONGO_IMAGE: &str = "quay.io/apibara/sink-mongo:latest";
static PARQUET_IMAGE: &str = "quay.io/apibara/sink-parquet:latest";
static POSTGRES_IMAGE: &str = "quay.io/apibara/sink-postgres:latest";
static WEBHOOK_IMAGE: &str = "quay.io/apibara/sink-webhook:latest";

#[derive(Debug, Clone)]
pub struct Configuration {
    /// Sink type to image.
    pub sinks: HashMap<String, SinkConfiguration>,
}

#[derive(Debug, Clone)]
pub struct SinkConfiguration {
    /// The container image to use for the sink container.
    pub image: String,
}

impl Configuration {
    pub fn with_sinks(&mut self, sinks: HashMap<String, SinkConfiguration>) -> &mut Self {
        self.sinks = sinks;
        self
    }
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

        let sinks = HashMap::from([
            ("console".to_string(), console),
            ("mongo".to_string(), mongo),
            ("parquet".to_string(), parquet),
            ("postgres".to_string(), postgres),
            ("webhook".to_string(), webhook),
        ]);

        Configuration { sinks }
    }
}
