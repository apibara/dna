use k8s_openapi::{
    api::core::v1::{EnvVar, EnvVarSource, Volume, VolumeMount},
    apimachinery::pkg::apis::meta::v1::{Condition, Time},
};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub type HeaderValueSource = EnvVarSource;

/// Run an indexer.
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    kind = "Indexer",
    group = "apibara.com",
    version = "v1alpha2",
    namespaced,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp" }"#,
    printcolumn = r#"{"name": "Status", "type": "string", "jsonPath": ".status.phase" }"#,
    printcolumn = r#"{"name": "Instance", "type": "string", "jsonPath": ".status.instanceName" }"#,
    printcolumn = r#"{"name": "Restarts", "type": "number", "jsonPath": ".status.restartCount" }"#
)]
#[kube(status = "IndexerStatus", shortname = "indexer")]
#[serde(rename_all = "camelCase")]
pub struct IndexerSpec {
    /// Indexer source code.
    pub source: IndexerSource,
    /// Sink to run.
    pub sink: Sink,
    /// List of volumes that can be mounted by containers belonging to the indexer.
    pub volumes: Option<Vec<IndexerVolume>>,
    /// List of environment variables to set in the indexer container.
    pub env: Option<Vec<EnvVar>>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum IndexerSource {
    /// Clone the indexer repository from GitHub.
    GitHub(GitHubSource),
    /// Use source code from a mounted volume.
    Volume(VolumeSource),
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq)]
pub struct GitHubSource {
    /// GitHub repository owner, e.g. `my-org`.
    pub owner: String,
    /// GitHub repository name, e.g. `my-indexer`.
    pub repo: String,
    /// GitHub repository branch name, e.g. `main`.
    pub branch: String,
    /// Run the indexer from the specified subpath of the repository, e.g. `/packages/indexer`.
    pub subpath: Option<String>,
    /// Additional flags to pass to `git clone`.
    pub git_flags: Option<Vec<String>>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq)]
pub struct VolumeSource {
    /// Path to the indexer source code, e.g. `/myvolume`.
    ///
    /// Use this option with the `volumes` field to mount a volume containing the indexer source
    /// code.
    pub path: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq)]
pub struct Sink {
    /// Container image with the sink.
    #[serde(flatten)]
    pub sink: SinkType,
    /// Path to the script to run.
    pub script: String,
    /// Arguments passed to the sink.
    pub args: Option<Vec<String>>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase", untagged)]
pub enum SinkType {
    Type { r#type: String },
    Image { image: String },
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq)]
pub struct IndexerVolume {
    /// Volume to mount.
    pub volume: Volume,
    /// Volume mount specification.
    pub volume_mount: VolumeMount,
}

/// Most recent status of the indexer.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IndexerStatus {
    /// Conditions of the indexer.
    pub conditions: Option<Vec<Condition>>,
    /// The name of the container running the indexer.
    pub instance_name: Option<String>,
    /// Service name exposing the indexer's status.
    pub status_service_name: Option<String>,
    /// Current phase of the indexer.
    pub phase: Option<String>,
    /// Number of times the indexer container has restarted.
    pub restart_count: Option<i32>,
    /// Creation timestamp of the indexer's pod.
    pub pod_created: Option<Time>,
}
