use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use error_stack::report;
use object_store::{PutMode, PutOptions, PutPayload};
use wings_metadata_core::admin::{NamespaceName, NamespaceRef};
use wings_object_store::ObjectStoreFactory;

use crate::{
    error::{IngestorError, IngestorResult},
    types::{
        NamespaceFolio, ReplyWithError, SerializedPartitionFolioMetadata,
        UploadedNamespaceFolioMetadata,
    },
};

/// Trait for generating unique IDs for folios.
pub trait FolioIdGenerator {
    fn generate_id(&self) -> String;
}

#[derive(Clone)]
pub struct FolioUploader {
    pub id_generator: Arc<dyn FolioIdGenerator>,
    pub object_store_factory: Arc<dyn ObjectStoreFactory>,
}

/// Generates unique IDs using the ULID algorithm.
#[derive(Debug, Clone)]
pub struct UlidFolioIdGenerator;

impl FolioUploader {
    pub fn new(
        id_generator: Arc<dyn FolioIdGenerator>,
        object_store_factory: Arc<dyn ObjectStoreFactory>,
    ) -> Self {
        FolioUploader {
            id_generator,
            object_store_factory,
        }
    }

    pub fn new_ulid(object_store_factory: Arc<dyn ObjectStoreFactory>) -> Self {
        FolioUploader::new(Arc::new(UlidFolioIdGenerator), object_store_factory)
    }

    pub fn new_folio_id(&self, namespace: &NamespaceName) -> String {
        let folio_id = self.id_generator.generate_id();
        format!("{}/folio/{}", namespace, folio_id)
    }

    pub async fn upload_folio(
        &self,
        folio: NamespaceFolio,
    ) -> std::result::Result<UploadedNamespaceFolioMetadata, Vec<ReplyWithError>> {
        // TODO: we probably want to add some metadata at the end of the file to make them
        // inspectable.
        let estimated_file_size = folio.partitions.iter().fold(0, |acc, p| acc + p.data.len());

        let mut content = BytesMut::with_capacity(estimated_file_size);
        let mut partition_metadata = Vec::with_capacity(folio.partitions.len());

        for partition in folio.partitions {
            let offset_bytes = content.len() as _;
            let size_bytes = partition.data.len() as _;
            content.extend_from_slice(&partition.data);
            let num_messages = partition
                .batches
                .iter()
                .fold(0, |acc, b| acc + b.num_messages);
            partition_metadata.push(SerializedPartitionFolioMetadata {
                topic_name: partition.topic_name,
                partition_value: partition.partition_value,
                num_messages,
                offset_bytes,
                size_bytes,
                batches: partition.batches,
            });
        }

        let file_ref = self.new_folio_id(&folio.namespace.name);

        match self
            .upload_to_namespace(folio.namespace.clone(), file_ref.clone(), content.freeze())
            .await
        {
            Ok(_) => Ok(UploadedNamespaceFolioMetadata {
                namespace: folio.namespace,
                file_ref,
                partitions: partition_metadata,
            }),
            Err(err) => {
                let error = err
                    .downcast_ref::<IngestorError>()
                    .cloned()
                    .unwrap_or_else(|| {
                        IngestorError::Internal(format!("failed to upload folio: {err}"))
                    });

                let replies = partition_metadata
                    .into_iter()
                    .flat_map(|p| {
                        p.batches.into_iter().map(|b| ReplyWithError {
                            reply: b.reply,
                            error: report!(error.clone()),
                        })
                    })
                    .collect::<Vec<_>>();

                Err(replies)
            }
        }
    }

    async fn upload_to_namespace(
        &self,
        namespace: NamespaceRef,
        file_ref: String,
        data: Bytes,
    ) -> IngestorResult<()> {
        let secret_name = &namespace.default_object_store_config;

        let object_store = self
            .object_store_factory
            .create_object_store(secret_name.clone())
            .await
            .map_err(|err| {
                IngestorError::Internal(format!("failed to create object store client: {err}"))
            })?;

        object_store
            .put_opts(
                &file_ref.into(),
                PutPayload::from_bytes(data),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(|err| IngestorError::Internal(format!("failed to upload folio: {err}")))?;

        Ok(())
    }
}

impl FolioIdGenerator for UlidFolioIdGenerator {
    fn generate_id(&self) -> String {
        ulid::Ulid::new().to_string()
    }
}
