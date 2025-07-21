//! Conversions between offset registry domain types and protobuf types.

use error_stack::{Report, ResultExt, bail, ensure};

use crate::admin::TopicName;
use crate::offset_registry::error::OffsetRegistryError;
use crate::offset_registry::types::*;
use crate::partition::PartitionValue;
use crate::protocol::wings::v1::{self as pb};

impl From<OffsetLocation> for pb::OffsetLocationResponse {
    fn from(location: OffsetLocation) -> Self {
        match location {
            OffsetLocation::Folio(folio) => pb::OffsetLocationResponse {
                location: Some(pb::offset_location_response::Location::FolioLocation(
                    folio.into(),
                )),
            },
        }
    }
}

impl TryFrom<pb::OffsetLocationResponse> for OffsetLocation {
    type Error = Report<OffsetRegistryError>;

    fn try_from(response: pb::OffsetLocationResponse) -> Result<Self, Self::Error> {
        use pb::offset_location_response::Location as ProtoLocation;

        let location = response
            .location
            .ok_or(OffsetRegistryError::InvalidArgument(
                "missing location from response".to_string(),
            ))?;

        match location {
            ProtoLocation::FolioLocation(folio) => Ok(OffsetLocation::Folio(folio.into())),
        }
    }
}

impl From<FolioLocation> for pb::FolioLocation {
    fn from(location: FolioLocation) -> Self {
        pb::FolioLocation {
            file_ref: location.file_ref,
            offset_bytes: location.offset_bytes,
            size_bytes: location.size_bytes,
            start_offset: location.start_offset,
            end_offset: location.end_offset,
        }
    }
}

impl From<pb::FolioLocation> for FolioLocation {
    fn from(location: pb::FolioLocation) -> Self {
        Self {
            file_ref: location.file_ref,
            offset_bytes: location.offset_bytes,
            size_bytes: location.size_bytes,
            start_offset: location.start_offset,
            end_offset: location.end_offset,
        }
    }
}

impl TryFrom<pb::BatchToCommit> for BatchToCommit {
    type Error = Report<OffsetRegistryError>;

    fn try_from(batch: pb::BatchToCommit) -> Result<Self, Self::Error> {
        let topic_name = TopicName::parse(&batch.topic).change_context(
            OffsetRegistryError::InvalidArgument("invalid topic name format".to_string()),
        )?;

        let partition_value = batch.partition.map(TryFrom::try_from).transpose()?;

        Ok(Self {
            topic_name,
            partition_value,
            num_messages: batch.num_messages,
            offset_bytes: batch.offset_bytes,
            batch_size_bytes: batch.batch_size_bytes,
        })
    }
}

impl From<&BatchToCommit> for pb::BatchToCommit {
    fn from(batch: &BatchToCommit) -> Self {
        pb::BatchToCommit {
            topic: batch.topic_name.to_string(),
            partition: batch.partition_value.as_ref().map(Into::into),
            num_messages: batch.num_messages,
            offset_bytes: batch.offset_bytes,
            batch_size_bytes: batch.batch_size_bytes,
        }
    }
}

impl TryFrom<pb::CommitFolioResponse> for Vec<CommittedBatch> {
    type Error = Report<OffsetRegistryError>;

    fn try_from(response: pb::CommitFolioResponse) -> Result<Self, Self::Error> {
        response
            .batches
            .into_iter()
            .map(TryFrom::try_from)
            .collect()
    }
}

impl From<CommittedBatch> for pb::CommittedBatch {
    fn from(batch: CommittedBatch) -> Self {
        Self {
            topic: batch.topic_name.to_string(),
            partition: batch.partition_value.as_ref().map(Into::into),
            start_offset: batch.start_offset,
            end_offset: batch.end_offset,
        }
    }
}

impl TryFrom<pb::CommittedBatch> for CommittedBatch {
    type Error = Report<OffsetRegistryError>;

    fn try_from(batch: pb::CommittedBatch) -> Result<Self, Self::Error> {
        let topic_name = TopicName::parse(&batch.topic).change_context(
            OffsetRegistryError::InvalidArgument("invalid topic name format".to_string()),
        )?;

        let partition_value = batch.partition.map(TryFrom::try_from).transpose()?;

        Ok(Self {
            topic_name,
            partition_value,
            start_offset: batch.start_offset,
            end_offset: batch.end_offset,
        })
    }
}

impl TryFrom<pb::PartitionValue> for PartitionValue {
    type Error = Report<OffsetRegistryError>;

    fn try_from(value: pb::PartitionValue) -> Result<Self, Self::Error> {
        use pb::partition_value::Value;

        match value.value {
            Some(Value::NullValue(_)) => Ok(PartitionValue::Null),
            Some(Value::Int8Value(v)) => {
                ensure!(
                    v >= i8::MIN as i32 && v <= i8::MAX as i32,
                    OffsetRegistryError::InvalidArgument(format!("Int8 value out of range: {v}"))
                );

                Ok(PartitionValue::Int8(v as i8))
            }
            Some(Value::Int16Value(v)) => {
                ensure!(
                    v >= i16::MIN as i32 && v <= i16::MAX as i32,
                    OffsetRegistryError::InvalidArgument(format!("Int16 value out of range: {v}"))
                );

                Ok(PartitionValue::Int16(v as i16))
            }
            Some(Value::Int32Value(v)) => Ok(PartitionValue::Int32(v)),
            Some(Value::Int64Value(v)) => Ok(PartitionValue::Int64(v)),
            Some(Value::Uint8Value(v)) => {
                ensure!(
                    v <= u8::MAX as u32,
                    OffsetRegistryError::InvalidArgument(format!("UInt8 value out of range: {v}"))
                );

                Ok(PartitionValue::UInt8(v as u8))
            }
            Some(Value::Uint16Value(v)) => {
                ensure!(
                    v <= u16::MAX as u32,
                    OffsetRegistryError::InvalidArgument(format!("UInt16 value out of range: {v}"))
                );

                Ok(PartitionValue::UInt16(v as u16))
            }
            Some(Value::Uint32Value(v)) => Ok(PartitionValue::UInt32(v)),
            Some(Value::Uint64Value(v)) => Ok(PartitionValue::UInt64(v)),
            Some(Value::StringValue(v)) => Ok(PartitionValue::String(v)),
            Some(Value::BytesValue(v)) => Ok(PartitionValue::Bytes(v)),
            Some(Value::BoolValue(v)) => Ok(PartitionValue::Boolean(v)),
            None => bail!(OffsetRegistryError::InvalidArgument(
                "Missing partition value".to_string(),
            )),
        }
    }
}

impl From<&PartitionValue> for pb::PartitionValue {
    fn from(value: &PartitionValue) -> Self {
        use pb::partition_value::Value;

        let value = match value {
            PartitionValue::Null => Value::NullValue(()),
            PartitionValue::Int8(v) => Value::Int8Value(*v as i32),
            PartitionValue::Int16(v) => Value::Int16Value(*v as i32),
            PartitionValue::Int32(v) => Value::Int32Value(*v),
            PartitionValue::Int64(v) => Value::Int64Value(*v),
            PartitionValue::UInt8(v) => Value::Uint8Value(*v as u32),
            PartitionValue::UInt16(v) => Value::Uint16Value(*v as u32),
            PartitionValue::UInt32(v) => Value::Uint32Value(*v),
            PartitionValue::UInt64(v) => Value::Uint64Value(*v),
            PartitionValue::String(v) => Value::StringValue(v.to_string()),
            PartitionValue::Bytes(v) => Value::BytesValue(v.clone()),
            PartitionValue::Boolean(v) => Value::BoolValue(*v),
        };

        pb::PartitionValue { value: Some(value) }
    }
}
