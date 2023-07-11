use k8s_openapi::api::core::v1::EnvVarSource;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::common::{CommonSpec, CommonStatus};

pub type HeaderValueSource = EnvVarSource;

/// Run a sink that invokes a webhook for each batch of data.
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[kube(
    kind = "SinkWebhook",
    group = "apibara.com",
    version = "v1alpha1",
    namespaced,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp" }"#,
    printcolumn = r#"{"name": "Status", "type": "string", "jsonPath": ".status.phase" }"#,
    printcolumn = r#"{"name": "Instance", "type": "string", "jsonPath": ".status.instanceName" }"#,
    printcolumn = r#"{"name": "Restarts", "type": "number", "jsonPath": ".status.restartCount" }"#
)]
#[kube(status = "SinkWebhookStatus", shortname = "sinkwebhook")]
#[serde(rename_all = "camelCase")]
pub struct SinkWebhookSpec {
    #[serde(flatten)]
    pub common: CommonSpec,
    /// The target url to send the request to.
    pub target_url: String,
    /// Additional headers to send with the request.
    pub headers: Option<Vec<HeaderSpec>>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HeaderSpec {
    /// The header name.
    pub name: String,
    /// The header value.
    pub value: Option<String>,
    /// Source for the header value.
    pub value_from: Option<HeaderValueSource>,
}

/// Most recent status of the webhook sink.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SinkWebhookStatus {
    #[serde(flatten)]
    pub common: CommonStatus,
}
