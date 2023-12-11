use std::time::Duration;

use apibara_operator::{
    crd::{GitHubSource, Indexer, IndexerSource, IndexerSpec, Sink, SinkType},
    error::OperatorError,
};
use error_stack::{Result, ResultExt};
use k8s_openapi::{
    api::{
        self,
        core::v1::{EnvVar, EnvVarSource, SecretKeySelector},
    },
    apiextensions_apiserver,
};
use kube::{
    api::{Patch, PatchParams},
    core::ObjectMeta,
    Api, Client, CustomResourceExt,
};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

// #[tokio::test]
pub async fn test_controller() -> Result<(), OperatorError> {
    let client = Client::try_default().await.change_context(OperatorError)?;

    // list namespaces to check client is working.
    let ns_api: Api<api::core::v1::Namespace> = Api::all(client.clone());
    let namespaces = ns_api
        .list(&Default::default())
        .await
        .change_context(OperatorError)?;
    assert!(!namespaces.items.is_empty());

    // check there is no crd installed, then install it.
    {
        let crd_api: Api<
            apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition,
        > = Api::all(client.clone());
        let crds = crd_api
            .list(&Default::default())
            .await
            .change_context(OperatorError)?;
        assert!(crds.items.is_empty());

        crd_api
            .create(&Default::default(), &Indexer::crd())
            .await
            .change_context(OperatorError)?;

        let crds = crd_api
            .list(&Default::default())
            .await
            .change_context(OperatorError)?;
        assert!(crds.items.len() == 1);
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    // create a console sink and check:
    // - it's created
    // - a pod with the same name is created
    //
    // notice that the kind cluster already has a secret named `apibara-api-key`
    // with the api key used to connect to the DNA cluster.
    let indexer_api: Api<Indexer> = Api::namespaced(client.clone(), "default");
    let pod_api: Api<api::core::v1::Pod> = Api::namespaced(client.clone(), "default");

    let indexers = indexer_api
        .list(&Default::default())
        .await
        .change_context(OperatorError)?;
    assert!(indexers.items.is_empty());

    let indexer_spec = IndexerSpec {
        env: Some(vec![EnvVar {
            name: "AUTH_TOKEN".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: Some("apibara-api-key".to_string()),
                    key: "production".to_string(),
                    optional: None,
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        }]),
        sink: Sink {
            sink: SinkType::Type {
                r#type: "console".to_string(),
            },
            script: "starknet_to_console.js".to_string(),
            args: None,
        },
        source: IndexerSource::GitHub(GitHubSource {
            owner: "apibara".to_string(),
            repo: "dna".to_string(),
            revision: "main".to_string(),
            subpath: Some("examples/console".to_string()),
            ..Default::default()
        }),
        volumes: None,
    };

    let indexer_manifest = Indexer {
        metadata: ObjectMeta {
            name: Some("test-indexer".to_string()),
            ..ObjectMeta::default()
        },
        spec: indexer_spec,
        status: None,
    };

    indexer_api
        .patch(
            "test-indexer",
            &PatchParams::apply("test"),
            &Patch::Apply(indexer_manifest),
        )
        .await
        .change_context(OperatorError)?;

    let indexers = indexer_api
        .list(&Default::default())
        .await
        .change_context(OperatorError)?;
    assert!(indexers.items.len() == 1);

    // no pod scheduled yet
    let pods = pod_api
        .list(&Default::default())
        .await
        .change_context(OperatorError)?;
    assert!(pods.items.is_empty());

    // start controller.
    let ct = CancellationToken::new();
    let mut controller_stream = Box::pin(
        apibara_operator::controller::create(client.clone(), Default::default(), ct.clone())
            .await
            .change_context(OperatorError)?
            .timeout(Duration::from_secs(3)),
    );

    loop {
        match controller_stream.try_next().await {
            Err(_elapsed) => break,
            Ok(item) => {
                let (obj, _action) = item
                    .transpose()
                    .change_context(OperatorError)?
                    .ok_or(OperatorError)?;
                assert_eq!(obj.name, "test-indexer");
            }
        }
    }

    // now there's one pod scheduled
    let _pod = pod_api
        .get("test-indexer")
        .await
        .change_context(OperatorError)?;

    // delete the indexer
    indexer_api
        .delete("test-indexer", &Default::default())
        .await
        .change_context(OperatorError)?;

    loop {
        match controller_stream.try_next().await {
            Err(_elapsed) => break,
            Ok(item) => {
                let (obj, _action) = item
                    .transpose()
                    .change_context(OperatorError)?
                    .ok_or(OperatorError)?;
                assert_eq!(obj.name, "test-indexer");
            }
        }
    }

    // all indexers and pods cleaned up
    let indexers = indexer_api
        .list(&Default::default())
        .await
        .change_context(OperatorError)?;
    assert!(indexers.items.is_empty());

    let pods = pod_api
        .list(&Default::default())
        .await
        .change_context(OperatorError)?;
    assert!(pods.items.is_empty());

    // terminate controller
    ct.cancel();

    Ok(())
}
