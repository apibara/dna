use k8s_openapi::{
    api::core::v1::{
        ConfigMapKeySelector, ConfigMapVolumeSource, EnvVar, EnvVarSource, KeyToPath,
        LocalObjectReference, SecretKeySelector, SecretVolumeSource, Volume, VolumeMount,
    },
    apimachinery::pkg::apis::meta::v1::{Condition, Time},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub type MetadataValueSource = EnvVarSource;
pub type AuthTokenValueSource = EnvVarSource;

/// Common configuration between all sinks.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CommonSpec {
    pub stream: StreamSpec,
    pub persistence: Option<PersistenceSpec>,
    pub image: Option<ImageSpec>,
    pub inherited_metadata: Option<InheritedMetadataSpec>,
    pub log_level: Option<LogLevel>,
}

/// Configure the image used to run the sink.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
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
    pub annotations: Option<serde_json::Map<String, serde_json::Value>>,
    /// Labels to add to the resources.
    pub labels: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Persist the sink state to etcd.
///
/// The persistence layer is also used to ensure only one instance of the sink is running at the
/// time.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PersistenceSpec {
    /// Etcd cluster connection url.
    pub persist_to_etcd: String,
    /// Unique sink id.
    pub sink_id: String,
}

/// DNA stream configuration.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StreamSpec {
    /// URL of the stream. Must start with `http` or `https`.
    pub stream_url: String,
    /// Filter to select which data to stream.
    pub filter: FilterSource,
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
    pub auth_token: Option<AuthTokenValue>,
    /// Metadata to add to the stream.
    pub metadata: Option<Vec<MetadataSpec>>,
}

/// Use a filter to select which data to stream.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FilterSource {
    /// Filter from ConfigMap.
    pub config_map_key_ref: Option<ConfigMapKeySelector>,
    /// Filter from Secret.
    pub secret_key_ref: Option<SecretKeySelector>,
}

/// Use a transform script to change the shape of the data.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
pub struct MetadataSpec {
    /// The metadata value.
    pub value: Option<String>,
    /// Source for the metadata value.
    pub value_from: Option<MetadataValueSource>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuthTokenValue {
    /// The auth token value, in plaintext.
    pub value: Option<String>,
    /// Source for the auth token value.
    pub value_from: Option<AuthTokenValueSource>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CommonStatus {
    /// Conditions for the sink object.
    pub conditions: Option<Vec<Condition>>,
    /// The name of the container running the sink.
    pub instance_name: Option<String>,
    /// Current phase of the sink.
    pub phase: Option<String>,
    /// Creation timestamp of the pod.
    pub pod_created: Option<Time>,
    /// Number of times the pod has been restarted.
    pub restart_count: Option<i32>,
}

impl CommonSpec {
    pub fn to_env_var(&self) -> Vec<EnvVar> {
        let mut env = self.stream.to_env_var();

        if let Some(persistence) = &self.persistence {
            env.extend(persistence.to_env_var());
        }

        let log_level = self.log_level.clone().unwrap_or(LogLevel::Info);
        env.push(log_level.to_env_var());

        env
    }
}

impl PersistenceSpec {
    pub fn to_env_var(&self) -> Vec<EnvVar> {
        vec![
            EnvVar {
                name: "PERSIST_TO_ETCD".to_string(),
                value: Some(self.persist_to_etcd.clone()),
                ..EnvVar::default()
            },
            EnvVar {
                name: "SINK_ID".to_string(),
                value: Some(self.sink_id.clone()),
                ..EnvVar::default()
            },
        ]
    }
}

impl StreamSpec {
    pub fn to_env_var(&self) -> Vec<EnvVar> {
        let mut env = vec![
            EnvVar {
                name: "STREAM_URL".to_string(),
                value: Some(self.stream_url.clone()),
                ..EnvVar::default()
            },
            self.network.to_env_var(),
        ];

        if let Some(batch_size) = self.batch_size {
            env.push(EnvVar {
                name: "BATCH_SIZE".to_string(),
                value: Some(batch_size.to_string()),
                ..EnvVar::default()
            });
        }

        if let Some(max_message_size) = &self.max_message_size {
            env.push(EnvVar {
                name: "MAX_MESSAGE_SIZE".to_string(),
                value: Some(max_message_size.clone()),
                ..EnvVar::default()
            });
        }

        if let Some(finality) = &self.finality {
            env.push(finality.to_env_var());
        }

        if let Some(starting_block) = self.starting_block {
            env.push(EnvVar {
                name: "STARTING_BLOCK".to_string(),
                value: Some(starting_block.to_string()),
                ..EnvVar::default()
            });
        }

        if let Some(auth_token) = &self.auth_token {
            env.push(auth_token.to_env_var());
        }

        if let Some(metadata) = &self.metadata {
            // We want to source different metadata values from different sources.
            // We can't do this with a single env var, so we create one env var per metadata value.
            // Then we interpolate the env vars in the METADATA env var.
            let variables: Vec<_> = metadata
                .iter()
                .enumerate()
                .map(|(i, m)| m.to_env_var(i))
                .collect();
            let metadata_value = variables
                .iter()
                .map(|v| format!("$({})", v.name))
                .collect::<Vec<_>>()
                .join(",");
            env.extend(variables);
            env.push(EnvVar {
                name: "METADATA".to_string(),
                value: Some(metadata_value),
                ..EnvVar::default()
            });
        }

        env
    }

    pub fn filter_data(&self) -> Option<(Volume, VolumeMount, EnvVar)> {
        if let Some(key_ref) = &self.filter.config_map_key_ref {
            return Some(data_from_config_map_key_ref(
                "data-filter",
                "FILTER",
                "/data/filter",
                key_ref,
            ));
        }
        if let Some(key_ref) = &self.filter.secret_key_ref {
            return Some(data_from_secret_key_ref(
                "data-filter",
                "FILTER",
                "/data/filter",
                key_ref,
            ));
        }
        None
    }

    pub fn transform_data(&self) -> Option<(Volume, VolumeMount, EnvVar)> {
        let Some(transform) = &self.transform else { return None; };
        if let Some(key_ref) = &transform.config_map_key_ref {
            return Some(data_from_config_map_key_ref(
                "data-transform",
                "TRANSFORM",
                "/data/transform",
                key_ref,
            ));
        }
        if let Some(key_ref) = &transform.secret_key_ref {
            return Some(data_from_secret_key_ref(
                "data-transform",
                "TRANSFORM",
                "/data/transform",
                key_ref,
            ));
        }
        None
    }
}

impl NetworkTypeSpec {
    pub fn to_env_var(&self) -> EnvVar {
        match self {
            NetworkTypeSpec::Starknet => EnvVar {
                name: "NETWORK_STARKNET".to_string(),
                value: Some("true".to_string()),
                ..EnvVar::default()
            },
        }
    }
}

impl FinalitySpec {
    pub fn to_env_var(&self) -> EnvVar {
        match self {
            FinalitySpec::Finalized => EnvVar {
                name: "FINALITY_FINALIZED".to_string(),
                value: Some("true".to_string()),
                ..EnvVar::default()
            },
            FinalitySpec::Accepted => EnvVar {
                name: "FINALITY_ACCEPTED".to_string(),
                value: Some("true".to_string()),
                ..EnvVar::default()
            },
            FinalitySpec::Pending => EnvVar {
                name: "FINALITY_PENDING".to_string(),
                value: Some("true".to_string()),
                ..EnvVar::default()
            },
        }
    }
}

impl AuthTokenValue {
    pub fn to_env_var(&self) -> EnvVar {
        EnvVar {
            name: "AUTH_TOKEN".to_string(),
            value: self.value.clone(),
            value_from: self.value_from.clone(),
        }
    }
}

impl MetadataSpec {
    pub fn to_env_var(&self, index: usize) -> EnvVar {
        EnvVar {
            name: format!("METADATA_{index}"),
            value: self.value.clone(),
            value_from: self.value_from.clone(),
        }
    }
}

impl LogLevel {
    pub fn to_env_var(&self) -> EnvVar {
        let value = serde_json::to_string(&self).expect("LogLevel as string");
        EnvVar {
            name: "RUST_LOG".to_string(),
            value: Some(value),
            ..EnvVar::default()
        }
    }
}

fn data_from_config_map_key_ref(
    name: &str,
    env_var: &str,
    mount_path: &str,
    key_ref: &ConfigMapKeySelector,
) -> (Volume, VolumeMount, EnvVar) {
    let volume = Volume {
        name: name.to_string(),
        config_map: Some(ConfigMapVolumeSource {
            name: key_ref.name.clone(),
            items: Some(vec![KeyToPath {
                key: key_ref.key.clone(),
                path: key_ref.key.clone(),
                ..KeyToPath::default()
            }]),
            ..ConfigMapVolumeSource::default()
        }),
        ..Volume::default()
    };

    let volume_mount = VolumeMount {
        name: name.to_string(),
        mount_path: mount_path.to_string(),
        read_only: Some(true),
        ..VolumeMount::default()
    };

    let env_var = EnvVar {
        name: env_var.to_string(),
        value: Some(format!("{}/{}", mount_path, key_ref.key)),
        ..EnvVar::default()
    };

    (volume, volume_mount, env_var)
}

fn data_from_secret_key_ref(
    name: &str,
    env_var: &str,
    mount_path: &str,
    key_ref: &SecretKeySelector,
) -> (Volume, VolumeMount, EnvVar) {
    let volume = Volume {
        name: name.to_string(),
        secret: Some(SecretVolumeSource {
            secret_name: key_ref.name.clone(),
            items: Some(vec![KeyToPath {
                key: key_ref.key.clone(),
                path: key_ref.key.clone(),
                ..KeyToPath::default()
            }]),
            ..SecretVolumeSource::default()
        }),
        ..Volume::default()
    };

    let volume_mount = VolumeMount {
        name: name.to_string(),
        mount_path: mount_path.to_string(),
        read_only: Some(true),
        ..VolumeMount::default()
    };

    let env_var = EnvVar {
        name: env_var.to_string(),
        value: Some(format!("{}/{}", mount_path, key_ref.key)),
        ..EnvVar::default()
    };

    (volume, volume_mount, env_var)
}
