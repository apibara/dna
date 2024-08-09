use error_stack::{Result, ResultExt};
use tonic::{
    metadata::{AsciiMetadataValue, KeyAndValueRef, MetadataMap},
    service::Interceptor,
};

use super::error::StreamClientError;

pub type MetadataKey = tonic::metadata::MetadataKey<tonic::metadata::Ascii>;
pub type MetadataValue = tonic::metadata::MetadataValue<tonic::metadata::Ascii>;

#[derive(Default, Clone)]
pub struct MetadataInterceptor {
    metadata: MetadataMap,
}

impl MetadataInterceptor {
    pub fn with_metadata(metadata: MetadataMap) -> Self {
        Self { metadata }
    }

    pub fn insert_meta(&mut self, key: MetadataKey, value: MetadataValue) -> Option<MetadataValue> {
        self.metadata.insert(key, value)
    }

    pub fn insert_bearer_token(
        &mut self,
        token: String,
    ) -> Result<Option<MetadataValue>, StreamClientError> {
        let token = AsciiMetadataValue::try_from(format!("Bearer {token}"))
            .change_context(StreamClientError)
            .attach_printable("failed to format authorization metadata value")?;
        Ok(self.metadata.insert("authorization", token))
    }
}

impl Interceptor for MetadataInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        let req_meta = request.metadata_mut();

        for kv in self.metadata.iter() {
            match kv {
                KeyAndValueRef::Ascii(key, value) => {
                    req_meta.insert(key, value.clone());
                }
                KeyAndValueRef::Binary(key, value) => {
                    req_meta.insert_bin(key, value.clone());
                }
            }
        }

        Ok(request)
    }
}
