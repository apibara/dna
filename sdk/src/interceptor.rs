use tonic::{
    metadata::{AsciiMetadataValue, KeyAndValueRef},
    service::Interceptor,
};

pub use tonic::metadata::{errors::InvalidMetadataValue, MetadataMap};

/// A metadata key.
pub type MetadataKey = tonic::metadata::MetadataKey<tonic::metadata::Ascii>;
/// A metadata value.
pub type MetadataValue = tonic::metadata::MetadataValue<tonic::metadata::Ascii>;

pub const AUTHORIZATION_HEADER: &str = "authorization";

#[derive(Default, Clone)]
pub struct MetadataInterceptor {
    metadata: MetadataMap,
}

impl MetadataInterceptor {
    pub fn with_metadata(metadata: MetadataMap) -> Self {
        Self { metadata }
    }

    pub fn insert_bearer_token(
        &mut self,
        token: String,
    ) -> Result<Option<MetadataValue>, InvalidMetadataValue> {
        let token = AsciiMetadataValue::try_from(format!("Bearer {token}"))?;
        Ok(self.metadata.insert(AUTHORIZATION_HEADER, token))
    }
}

impl Interceptor for MetadataInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
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
