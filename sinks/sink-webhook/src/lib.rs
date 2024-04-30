mod configuration;
mod sink;

pub use self::configuration::{BodyMode, SinkWebhookConfiguration, SinkWebhookOptions};
pub use self::sink::WebhookSink;
