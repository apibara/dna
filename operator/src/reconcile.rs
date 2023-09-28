use std::{sync::Arc, time::Duration};

use k8s_openapi::{
    api,
    apimachinery::pkg::apis::meta::{self, v1::Condition},
    chrono::{DateTime, Utc},
    Metadata,
};
use kube::{
    api::{DeleteParams, Patch, PatchParams},
    core::Resource,
    runtime::{controller::Action, finalizer},
    Api, ResourceExt,
};
use serde_json::json;
use tracing::{info, instrument, warn};

use crate::{
    context::{Context, OperatorError},
    crd::{Indexer, IndexerSource, IndexerStatus, Sink},
};

static INDEXER_FINALIZER: &str = "indexer.apibara.com";
static GIT_CLONE_IMAGE: &str = "docker.io/alpine/git:latest";

impl Indexer {
    #[instrument(skip_all)]
    async fn reconcile(&self, ctx: Arc<Context>) -> Result<Action, OperatorError> {
        use api::core::v1::Pod;

        let ns = self.namespace().ok_or(OperatorError::CrdInNamespaced)?;
        let name = self.name_any();

        let indexers: Api<Indexer> = Api::namespaced(ctx.client.clone(), &ns);
        let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), &ns);

        // Check if the indexer needs to be restarted.
        let (restart_increment, error_condition) = if let Some(pod_name) = self.instance_name() {
            self.maybe_delete_pod(pod_name, &pods).await?
        } else {
            (0, None)
        };

        let metadata = self.object_metadata(&ctx);
        let spec = self.pod_spec(&ctx);
        let pod_manifest = Pod {
            metadata,
            spec: Some(spec),
            ..Pod::default()
        };

        let pod = pods
            .patch(
                &name,
                &PatchParams::apply("indexer"),
                &Patch::Apply(pod_manifest),
            )
            .await?;

        let pod_scheduled_condition = Condition {
            last_transition_time: pod
                .meta()
                .creation_timestamp
                .clone()
                .unwrap_or(meta::v1::Time(DateTime::<Utc>::MIN_UTC)),
            type_: "PodScheduled".to_string(),
            message: "Pod has been scheduled".to_string(),
            observed_generation: self.meta().generation,
            reason: "PodScheduled".to_string(),
            status: "True".to_string(),
        };

        let phase = if error_condition.is_some() {
            "Error".to_string()
        } else {
            "Running".to_string()
        };

        let mut conditions = vec![pod_scheduled_condition];
        if let Some(error_condition) = error_condition {
            conditions.push(error_condition);
        }

        let restart_count = self
            .status
            .as_ref()
            .map(|status| status.restart_count.unwrap_or_default() + restart_increment);

        let status = json!({
            "status": IndexerStatus {
                pod_created: pod.meta().creation_timestamp.clone(),
                instance_name: pod.metadata().name.clone(),
                phase: Some(phase),
                conditions: Some(conditions),
                restart_count,
            }
        });

        indexers
            .patch_status(&name, &PatchParams::default(), &Patch::Merge(&status))
            .await?;

        Ok(Action::requeue(Duration::from_secs(10)))
    }

    #[instrument(skip_all)]
    async fn cleanup(&self, ctx: Arc<Context>) -> Result<Action, OperatorError> {
        use api::core::v1::Pod;

        let ns = self.namespace().ok_or(OperatorError::CrdInNamespaced)?;
        let name = self.name_any();
        let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), &ns);

        if let Some(_existing) = pods.get_opt(&name).await? {
            pods.delete(&name, &DeleteParams::default()).await?;
        }

        Ok(Action::requeue(Duration::from_secs(10)))
    }

    fn instance_name(&self) -> Option<&str> {
        self.status
            .as_ref()
            .and_then(|status| status.instance_name.as_ref())
            .map(|name| name.as_str())
    }

    /// Check the pod status and delete it if it has failed.
    ///
    /// The pod is deleted so that it can be recreated once the
    /// status of the indexer has been updated.
    async fn maybe_delete_pod(
        &self,
        pod_name: &str,
        pods: &Api<api::core::v1::Pod>,
    ) -> Result<(i32, Option<Condition>), OperatorError> {
        let Some(pod) = pods.get_opt(pod_name).await? else {
            return Ok((0, None));
        };

        let container_status = pod.status.as_ref().and_then(|status| {
            status
                .container_statuses
                .as_ref()
                .and_then(|statuses| statuses.first())
        });

        let Some(finished_at) = container_status
            .and_then(|cs| cs.state.as_ref())
            .and_then(|st| st.terminated.clone())
            .and_then(|ts| ts.finished_at) else {
                return Ok((0, None));
            };

        let elapsed = (Utc::now().time() - finished_at.0.time())
            .to_std()
            .unwrap_or_default();

        // For now, restart the indexer every minute.
        // TODO: Only restart it based on the container exit code.
        if elapsed > Duration::from_secs(60) {
            info!(pod = %pod.name_any(), "deleting pod for restart");

            pods.delete(pod_name, &Default::default()).await?;
            Ok((1, None))
        } else {
            let error_condition = Condition {
                last_transition_time: finished_at,
                type_: "PodTerminated".to_string(),
                message: "Pod has been terminated".to_string(),
                observed_generation: self.meta().generation,
                reason: "PodTerminated".to_string(),
                status: "False".to_string(),
            };

            Ok((0, Some(error_condition)))
        }
    }

    fn object_metadata(&self, _ctx: &Arc<Context>) -> meta::v1::ObjectMeta {
        use meta::v1::ObjectMeta;

        ObjectMeta {
            name: self.metadata.name.clone(),
            ..ObjectMeta::default()
        }
    }

    fn pod_spec(&self, ctx: &Arc<Context>) -> api::core::v1::PodSpec {
        use api::core::v1::PodSpec;

        // Initialize volume, volume mounts, and env vars.
        let mut volumes = Vec::new();
        let mut volume_mounts = Vec::new();
        let env = self.spec.env.clone().unwrap_or_default();

        if let Some(volumes_from_config) = self.spec.volumes.as_ref() {
            for volume in volumes_from_config {
                volumes.push(volume.volume.clone());
                volume_mounts.push(volume.volume_mount.clone());
            }
        }

        let mut init_containers = Vec::new();
        let mut containers = Vec::new();

        let (init_container, workdir) =
            self.source_container(&mut volumes, &mut volume_mounts, &env, ctx);

        if let Some(init_container) = init_container {
            init_containers.push(init_container);
        }

        let container = self.sink_container(&workdir, &volume_mounts, &env, ctx);
        containers.push(container);

        PodSpec {
            init_containers: Some(init_containers),
            containers,
            volumes: Some(volumes),
            restart_policy: Some("Never".to_string()),
            ..PodSpec::default()
        }
    }

    fn source_container(
        &self,
        volumes: &mut Vec<api::core::v1::Volume>,
        volume_mounts: &mut Vec<api::core::v1::VolumeMount>,
        env: &[api::core::v1::EnvVar],
        _ctx: &Arc<Context>,
    ) -> (Option<api::core::v1::Container>, String) {
        use api::core::v1::{Container, Volume, VolumeMount};

        match &self.spec.source {
            IndexerSource::GitHub(github) => {
                // This init container clones the indexer source code from GitHub.
                let mut args = vec!["clone".to_string(), "--depth=1".to_string()];

                if let Some(flags) = &github.git_flags {
                    args.extend_from_slice(flags);
                }

                args.extend_from_slice(&[
                    format!("--branch={}", github.branch),
                    format!("https://github.com/{}/{}.git", github.owner, github.repo),
                    "/code".to_string(),
                ]);

                // Create an emptyDir volume where the source code will be cloned.
                volumes.push(Volume {
                    name: "code".to_string(),
                    empty_dir: Some(Default::default()),
                    ..Volume::default()
                });

                volume_mounts.push(VolumeMount {
                    name: "code".to_string(),
                    mount_path: "/code".to_string(),
                    ..VolumeMount::default()
                });

                let workdir = match github.subpath {
                    None => "/code".to_string(),
                    Some(ref subpath) => format!("/code/{}", subpath),
                };

                let container = Container {
                    name: "clone-github-repo".to_string(),
                    image: Some(GIT_CLONE_IMAGE.to_string()),
                    args: Some(args),
                    volume_mounts: Some(volume_mounts.clone()),
                    env: Some(env.to_owned()),
                    ..Container::default()
                };

                (Some(container), workdir)
            }
            IndexerSource::Volume(fs) => (None, fs.path.clone()),
        }
    }

    fn sink_container(
        &self,
        workdir: &str,
        volume_mounts: &[api::core::v1::VolumeMount],
        env: &[api::core::v1::EnvVar],
        _ctx: &Arc<Context>,
    ) -> api::core::v1::Container {
        use api::core::v1::Container;

        match &self.spec.sink {
            Sink::Custom(custom) => {
                let mut args = vec!["run".to_string(), custom.script.clone()];

                if let Some(extra_args) = &custom.args {
                    args.extend_from_slice(extra_args);
                }

                Container {
                    name: "sink".to_string(),
                    image: Some(custom.image.clone()),
                    args: Some(args),
                    volume_mounts: Some(volume_mounts.to_owned()),
                    env: Some(env.to_owned()),
                    working_dir: Some(workdir.to_string()),
                    ..Container::default()
                }
            }
        }
    }
}

pub async fn reconcile_indexer(
    indexer: Arc<Indexer>,
    ctx: Arc<Context>,
) -> Result<Action, OperatorError> {
    let ns = indexer.namespace().ok_or(OperatorError::CrdInNamespaced)?;
    let indexers: Api<Indexer> = Api::namespaced(ctx.client.clone(), &ns);

    info!(
        indexer = %indexer.name_any(),
        namespace = %ns,
        "reconcile indexer"
    );

    finalizer::finalizer(&indexers, INDEXER_FINALIZER, indexer, |event| async {
        use finalizer::Event::*;
        match event {
            Apply(indexer) => indexer.reconcile(ctx.clone()).await,
            Cleanup(indexer) => indexer.cleanup(ctx.clone()).await,
        }
    })
    .await
    .map_err(|err| OperatorError::Finalizer(err.into()))
}

pub fn error_policy(_indexer: Arc<Indexer>, err: &OperatorError, _ctx: Arc<Context>) -> Action {
    warn!(err = ?err, "Error reconciling indexer");

    Action::requeue(Duration::from_secs(30))
}
