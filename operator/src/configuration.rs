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
