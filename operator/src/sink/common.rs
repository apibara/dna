use std::collections::BTreeMap;

use k8s_openapi::{
    api::core::v1::{ConfigMapKeySelector, EnvVarSource, LocalObjectReference, SecretKeySelector},
    apimachinery::pkg::apis::meta::v1::Condition,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub type MetadataValueSource = EnvVarSource;

/// Common configuration between all sinks.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct CommonSpec {
    pub persistence: Option<PersistenceSpec>,
    pub stream: StreamSpec,
    pub image: Option<ImageSpec>,
    pub inherited_metadata: Option<InheritedMetadataSpec>,
    pub log_level: Option<LogLevel>,
}

/// Configure the image used to run the sink.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct ImageSpec {
    /// Name of the container image to run.
    pub name: Option<String>,
    /// Pull policy for the container image. Onef of `Always`, `Never` or `IfNotPresent`.
    ///
    /// More info: https://kubernetes.io/docs/concepts/containers/images#updating-images
    pub pull_policy: Option<String>,
    /// List of references to secrets in the same namespace to use for pulling any of the images.
    ///
    /// More info: https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod
    pub pull_secrets: Option<Vec<LocalObjectReference>>,
}

/// Metadata that will be inherited by all resources created by the sink.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct InheritedMetadataSpec {
    /// Annotations to add to the resources.
    pub annotations: Option<BTreeMap<String, String>>,
    /// Labels to add to the resources.
    pub labels: Option<BTreeMap<String, String>>,
}

/// Persist the sink state to etcd.
///
/// The persistence layer is also used to ensure only one instance of the sink is running at the
/// time.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct PersistenceSpec {
    /// Etcd cluster connection url.
    pub persist_to_etcd: String,
    /// Unique sink id.
    pub sink_id: String,
}

/// DNA stream configuration.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct StreamSpec {
    /// URL of the stream. Must start with `http` or `https`.
    pub stream_url: String,
    /// Filter to select which data to stream.
    pub filter: Option<FilterSource>,
    /// Transform script to change the shape of the data.
    pub transform: Option<TransformSource>,
    /// The type of network.
    pub network: NetworkTypeSpec,
    /// Number of blocks in each historical batch.
    pub batch_size: Option<u64>,
    /// Maximum message size.
    ///
    /// Accepts size in human readable form, e.g. 1kb, 1MB, 1GB.
    pub max_message_size: Option<String>,
    /// Data finality.
    pub finality: Option<FinalitySpec>,
    /// Start streaming data from a specific block.
    pub starting_block: Option<u64>,
    /// Bearer token used to authenticate with the stream.
    pub auth_token: Option<String>,
    /// Metadata to add to the stream.
    pub metadata: Option<Vec<MetadataSpec>>,
}

/// Use a filter to select which data to stream.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct FilterSource {
    /// Filter from ConfigMap.
    pub config_map_key_ref: Option<ConfigMapKeySelector>,
    /// Filter from Secret.
    pub secret_key_ref: Option<SecretKeySelector>,
}

/// Use a transform script to change the shape of the data.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct TransformSource {
    /// Transform from ConfigMap.
    pub config_map_key_ref: Option<ConfigMapKeySelector>,
    /// Transform from Secret.
    pub secret_key_ref: Option<SecretKeySelector>,
}

/// Data finality.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum FinalitySpec {
    /// Stream finalized blocks.
    Finalized,
    /// Stream finalized and accepted blocks.
    Accepted,
    /// Stream finalized, accepted and pending blocks.
    Pending,
}

/// Type of network.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum NetworkTypeSpec {
    /// Starknet L2 and appchains.
    Starknet,
}

/// Log level for the containers.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warning,
    Info,
    Debug,
    Trace,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct MetadataSpec {
    /// The metadata name.
    pub name: String,
    /// The metadata value.
    pub value: Option<String>,
    /// Source for the metadata value.
    pub value_from: Option<MetadataValueSource>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq)]
pub struct CommonStatus {
    /// Conditions for the sink object.
    pub conditions: Option<Vec<Condition>>,
    /// The name of the container running the sink.
    pub instance_name: Option<String>,
    /// Current phase of the sink.
    pub phase: Option<String>,
}
