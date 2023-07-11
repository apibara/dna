mod common;
pub mod webhook;
#[cfg(feature = "operator")]
pub mod webhook_controller;

pub use webhook::{SinkWebhook, SinkWebhookSpec, SinkWebhookStatus};
