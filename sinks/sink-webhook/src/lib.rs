mod configuration;
mod sink;

pub use self::configuration::{SinkWebhookConfiguration, SinkWebhookOptions};
pub use self::sink::{SinkWebhookError, WebhookSink};
