use std::collections::{BTreeMap, HashMap};

use apibara_operator::crd::{
    CustomSink, GitHubSource, Indexer as KubeIndexer, IndexerSource, IndexerSpec, IndexerVolume,
    Sink, VolumeSource,
};
use apibara_runner_common::runner::v1::{source, CreateIndexerRequest, Indexer};
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, EnvVar, EnvVarSource, Secret, SecretKeySelector, Volume,
    VolumeMount,
};
use kube::core::ObjectMeta;

/// Kubernetes resources for an indexer.
pub struct KubeResources {
    pub indexer_id: String,
    pub indexer: KubeIndexer,
    pub env_secret: Secret,
    pub script_config_map: Option<ConfigMap>,
}

impl KubeResources {
    pub fn from_request(request: CreateIndexerRequest) -> Result<Self, tonic::Status> {
        let indexer = request
            .indexer
            .ok_or_else(|| tonic::Status::invalid_argument("indexer is required"))?;

        let indexer_id = if request.indexer_id.is_empty() {
            return Err(tonic::Status::invalid_argument("indexer_id is required"));
        } else if !hostname_validator::is_valid(&request.indexer_id) {
            return Err(tonic::Status::invalid_argument(
                "indexer_id must be a valid IETF RFC 1123 hostname",
            ));
        } else {
            request.indexer_id
        };

        let source = indexer
            .source
            .ok_or_else(|| tonic::Status::invalid_argument("indexer.source is required"))?;

        let source = source
            .source
            .ok_or_else(|| tonic::Status::invalid_argument("indexer.source is required"))?;

        let (source, script, script_config_map, script_volume) = match source {
            source::Source::Filesystem(_) => {
                return Err(tonic::Status::invalid_argument(
                    "source.filesystem is not supported by Kubernetes runner",
                ))
            }
            source::Source::Github(github) => {
                let source = GitHubSource {
                    owner: github.owner.clone(),
                    repo: github.repo.clone(),
                    branch: github.branch.clone(),
                    subpath: github.subdir.clone(),
                    git_flags: None,
                };
                let script = github.script;
                (IndexerSource::GitHub(source), script, None, None)
            }
            source::Source::Script(script) => {
                let config_map = ConfigMap {
                    metadata: ObjectMeta {
                        name: Some(script_config_map_name(&indexer_id)),
                        ..ObjectMeta::default()
                    },
                    data: Some({
                        let mut data = std::collections::BTreeMap::new();
                        data.insert(script.filename.clone(), script.contents.clone());
                        data
                    }),
                    ..ConfigMap::default()
                };

                let volume = Volume {
                    name: "script-source".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: Some(config_map.metadata.name.clone().unwrap()),
                        ..ConfigMapVolumeSource::default()
                    }),
                    ..Volume::default()
                };

                let volume_mount = VolumeMount {
                    name: "script-source".to_string(),
                    mount_path: "/script".to_string(),
                    ..VolumeMount::default()
                };

                let indexer_volume = IndexerVolume {
                    volume_mount,
                    volume,
                };

                let source = VolumeSource {
                    path: "/script".to_string(),
                };

                let script = script.filename;

                (
                    IndexerSource::Volume(source),
                    script,
                    Some(config_map),
                    Some(indexer_volume),
                )
            }
        };

        let sink = CustomSink {
            image: "quay.io/apibara/cli:latest".to_string(),
            script,
            args: None,
        };

        let (env_vars, env_secret) = {
            let mut env_vars = Vec::new();
            let mut secret_data = BTreeMap::new();
            let secret_name = env_secret_name(&indexer_id);
            for (k, v) in indexer.environment.iter() {
                secret_data.insert(k.clone(), v.clone());
                env_vars.push(EnvVar {
                    name: k.clone(),
                    value_from: Some(EnvVarSource {
                        secret_key_ref: Some(SecretKeySelector {
                            name: Some(secret_name.clone()),
                            key: k.clone(),
                            ..SecretKeySelector::default()
                        }),
                        ..EnvVarSource::default()
                    }),
                    ..EnvVar::default()
                });
            }

            let secret = Secret {
                metadata: ObjectMeta {
                    name: Some(env_secret_name(&indexer_id)),
                    ..ObjectMeta::default()
                },
                string_data: Some(secret_data),
                ..Default::default()
            };

            (env_vars, secret)
        };

        let labels = {
            let mut labels = BTreeMap::new();
            for (k, v) in indexer.labels.iter() {
                labels.insert(k.clone(), v.clone());
            }
            labels
        };

        let indexer_spec = IndexerSpec {
            source,
            sink: Sink::Custom(sink),
            volumes: script_volume.map(|v| vec![v]),
            env: Some(env_vars),
        };

        let indexer_manifest = KubeIndexer {
            metadata: ObjectMeta {
                name: Some(indexer_id.clone()),
                labels: Some(labels),
                ..ObjectMeta::default()
            },
            spec: indexer_spec,
            status: None,
        };

        Ok(KubeResources {
            indexer_id,
            indexer: indexer_manifest,
            env_secret,
            script_config_map,
        })
    }

    pub fn into_indexer(self) -> Result<Indexer, tonic::Status> {
        let labels = self
            .indexer
            .metadata
            .labels
            .map(|labels| {
                let mut res = HashMap::new();
                for (k, v) in labels.iter() {
                    res.insert(k.clone(), v.clone());
                }
                res
            })
            .unwrap_or_default();

        let environment = self
            .env_secret
            .string_data
            .map(|data| {
                let mut res = HashMap::new();
                for (k, v) in data.iter() {
                    res.insert(k.clone(), v.clone());
                }
                res
            })
            .unwrap_or_default();

        let indexer = Indexer {
            name: indexer_resource_name(&self.indexer_id),
            labels,
            environment,
            ..Indexer::default()
        };

        Ok(indexer)
    }
}

fn script_config_map_name(indexer_id: &str) -> String {
    format!("{}-script", indexer_id)
}

fn env_secret_name(indexer_id: &str) -> String {
    format!("{}-env", indexer_id)
}

fn indexer_resource_name(indexer_id: &str) -> String {
    format!("indexers/{}", indexer_id)
}

fn parse_resource_name(name: &str) -> Option<String> {
    name.split_once("/").and_then(|(res, name)| {
        if res == "indexers" {
            Some(name.to_string())
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use apibara_operator::crd::IndexerSource;
    use apibara_runner_common::runner::v1::{
        source, CreateIndexerRequest, GitHub, Indexer, Script, Source,
    };
    use kube::ResourceExt;

    use crate::kube_resources::KubeResources;

    #[test]
    fn test_github_source() {
        let github = GitHub {
            owner: "apibara".to_string(),
            repo: "dna".to_string(),
            branch: "main".to_string(),
            subdir: Some("examples/console".to_string()),
            script: "starknet_to_console.js".to_string(),
        };
        let source = source::Source::Github(github);
        let source = Source {
            source: Some(source),
        };

        let request = CreateIndexerRequest {
            indexer_id: "test".to_string(),
            indexer: Some(Indexer {
                source: Some(source),
                environment: HashMap::from([
                    ("key-1".to_string(), "value-1".to_string()),
                    ("key-2".to_string(), "value-2".to_string()),
                ]),
                labels: HashMap::from([("label-1".to_string(), "value-1".to_string())]),
                ..Indexer::default()
            }),
        };

        let resources = KubeResources::from_request(request).unwrap();

        assert!(resources.script_config_map.is_none());
        let env = resources.env_secret;
        let indexer = resources.indexer;

        assert_eq!(env.name_any(), "test-env");
        assert_eq!(env.string_data.expect("env string data").len(), 2);

        let source = indexer.spec.source;
        let IndexerSource::GitHub(github) = source else {
            panic!("Expected GitHub source");
        };
        assert_eq!(github.owner, "apibara");
        assert_eq!(github.repo, "dna");
        assert_eq!(github.branch, "main");
        assert_eq!(github.subpath, Some("examples/console".to_string()));
    }

    #[test]
    fn test_script_source() {
        let script = Script {
            filename: "script.js".to_string(),
            contents: "export default function t(x) { returns x }".to_string(),
        };
        let source = source::Source::Script(script);
        let source = Source {
            source: Some(source),
        };

        let request = CreateIndexerRequest {
            indexer_id: "test".to_string(),
            indexer: Some(Indexer {
                source: Some(source),
                ..Indexer::default()
            }),
        };

        let resources = KubeResources::from_request(request).unwrap();
        let indexer = resources.indexer;
        let config_map = resources.script_config_map.expect("script config map");

        let source = indexer.spec.source;
        let IndexerSource::Volume(volume_source) = source else {
            panic!("Expected Volume source");
        };

        let volumes = indexer.spec.volumes.expect("volumes");
        assert_eq!(volumes.len(), 1);
        let volume = &volumes[0];
        assert_eq!(volume_source.path, volume.volume_mount.mount_path);
        assert!(config_map.data.unwrap().get("script.js").is_some());
    }
}
