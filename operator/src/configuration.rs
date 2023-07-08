#[derive(Debug, Clone)]
pub struct Configuration {
    pub webhook: SinkWebhookConfiguration,
}

#[derive(Debug, Clone)]
pub struct SinkWebhookConfiguration {
    /// The image name to use for the webhook container.
    pub image: String,
}
