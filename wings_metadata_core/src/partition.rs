//! Partition value types for Wings metadata.
//!
//! This module provides a restricted set of types that can be used as partition keys.
//! Unlike DataFusion's ScalarValue, PartitionValue only allows a limited set of types
//! that are suitable for partitioning data.

use std::convert::TryFrom;

use arrow::datatypes::DataType;
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors that can occur when converting partition values.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum PartitionValueError {
    #[error("unsupported scalar value type for partitioning: {scalar_type}")]
    UnsupportedScalarType { scalar_type: String },
    #[error("invalid partition value: {message}")]
    InvalidValue { message: String },
}

/// A restricted set of values that can be used for partitioning.
///
/// This enum only allows types that are suitable for partitioning data,
/// providing a more constrained alternative to DataFusion's ScalarValue.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PartitionValue {
    /// Null value
    Null,

    // Signed integers
    /// 8-bit signed integer
    Int8(i8),
    /// 16-bit signed integer
    Int16(i16),
    /// 32-bit signed integer
    Int32(i32),
    /// 64-bit signed integer
    Int64(i64),

    // Unsigned integers
    /// 8-bit unsigned integer
    UInt8(u8),
    /// 16-bit unsigned integer
    UInt16(u16),
    /// 32-bit unsigned integer
    UInt32(u32),
    /// 64-bit unsigned integer
    UInt64(u64),

    /// UTF-8 string
    String(String),
    /// Binary data
    Bytes(Vec<u8>),
    /// Boolean value
    Boolean(bool),
}

impl PartitionValue {
    /// Returns true if the value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, PartitionValue::Null)
    }

    /// Returns the type name of the partition value.
    pub fn type_name(&self) -> &'static str {
        match self {
            PartitionValue::Null => "null",
            PartitionValue::Int8(_) => "int8",
            PartitionValue::Int16(_) => "int16",
            PartitionValue::Int32(_) => "int32",
            PartitionValue::Int64(_) => "int64",
            PartitionValue::UInt8(_) => "uint8",
            PartitionValue::UInt16(_) => "uint16",
            PartitionValue::UInt32(_) => "uint32",
            PartitionValue::UInt64(_) => "uint64",
            PartitionValue::String(_) => "string",
            PartitionValue::Bytes(_) => "bytes",
            PartitionValue::Boolean(_) => "boolean",
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            PartitionValue::Null => DataType::Null,
            PartitionValue::Int8(_) => DataType::Int8,
            PartitionValue::Int16(_) => DataType::Int16,
            PartitionValue::Int32(_) => DataType::Int32,
            PartitionValue::Int64(_) => DataType::Int64,
            PartitionValue::UInt8(_) => DataType::UInt8,
            PartitionValue::UInt16(_) => DataType::UInt16,
            PartitionValue::UInt32(_) => DataType::UInt32,
            PartitionValue::UInt64(_) => DataType::UInt64,
            PartitionValue::String(_) => DataType::Utf8,
            PartitionValue::Bytes(_) => DataType::Binary,
            PartitionValue::Boolean(_) => DataType::Boolean,
        }
    }
}

impl std::fmt::Display for PartitionValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitionValue::Null => write!(f, "null"),
            PartitionValue::Int8(v) => write!(f, "{}", v),
            PartitionValue::Int16(v) => write!(f, "{}", v),
            PartitionValue::Int32(v) => write!(f, "{}", v),
            PartitionValue::Int64(v) => write!(f, "{}", v),
            PartitionValue::UInt8(v) => write!(f, "{}", v),
            PartitionValue::UInt16(v) => write!(f, "{}", v),
            PartitionValue::UInt32(v) => write!(f, "{}", v),
            PartitionValue::UInt64(v) => write!(f, "{}", v),
            PartitionValue::String(v) => write!(f, "{}", v),
            PartitionValue::Bytes(v) => write!(f, "{}", hex::encode(v)),
            PartitionValue::Boolean(v) => write!(f, "{}", v),
        }
    }
}

impl TryFrom<ScalarValue> for PartitionValue {
    type Error = PartitionValueError;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        match value {
            ScalarValue::Null => Ok(PartitionValue::Null),

            // Signed integers
            ScalarValue::Int8(Some(v)) => Ok(PartitionValue::Int8(v)),
            ScalarValue::Int16(Some(v)) => Ok(PartitionValue::Int16(v)),
            ScalarValue::Int32(Some(v)) => Ok(PartitionValue::Int32(v)),
            ScalarValue::Int64(Some(v)) => Ok(PartitionValue::Int64(v)),

            // Unsigned integers
            ScalarValue::UInt8(Some(v)) => Ok(PartitionValue::UInt8(v)),
            ScalarValue::UInt16(Some(v)) => Ok(PartitionValue::UInt16(v)),
            ScalarValue::UInt32(Some(v)) => Ok(PartitionValue::UInt32(v)),
            ScalarValue::UInt64(Some(v)) => Ok(PartitionValue::UInt64(v)),

            // String types
            ScalarValue::Utf8(Some(v)) => Ok(PartitionValue::String(v)),
            ScalarValue::LargeUtf8(Some(v)) => Ok(PartitionValue::String(v)),

            // Binary types
            ScalarValue::Binary(Some(v)) => Ok(PartitionValue::Bytes(v)),
            ScalarValue::LargeBinary(Some(v)) => Ok(PartitionValue::Bytes(v)),

            // Boolean
            ScalarValue::Boolean(Some(v)) => Ok(PartitionValue::Boolean(v)),

            // Null variants of supported types
            ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
            | ScalarValue::UInt8(None)
            | ScalarValue::UInt16(None)
            | ScalarValue::UInt32(None)
            | ScalarValue::UInt64(None)
            | ScalarValue::Utf8(None)
            | ScalarValue::LargeUtf8(None)
            | ScalarValue::Binary(None)
            | ScalarValue::LargeBinary(None)
            | ScalarValue::Boolean(None) => Ok(PartitionValue::Null),

            // Unsupported types
            _ => Err(PartitionValueError::UnsupportedScalarType {
                scalar_type: format!("{:?}", value),
            }),
        }
    }
}

impl From<PartitionValue> for ScalarValue {
    fn from(value: PartitionValue) -> Self {
        match value {
            PartitionValue::Null => ScalarValue::Null,

            // Signed integers
            PartitionValue::Int8(v) => ScalarValue::Int8(Some(v)),
            PartitionValue::Int16(v) => ScalarValue::Int16(Some(v)),
            PartitionValue::Int32(v) => ScalarValue::Int32(Some(v)),
            PartitionValue::Int64(v) => ScalarValue::Int64(Some(v)),

            // Unsigned integers
            PartitionValue::UInt8(v) => ScalarValue::UInt8(Some(v)),
            PartitionValue::UInt16(v) => ScalarValue::UInt16(Some(v)),
            PartitionValue::UInt32(v) => ScalarValue::UInt32(Some(v)),
            PartitionValue::UInt64(v) => ScalarValue::UInt64(Some(v)),

            // String
            PartitionValue::String(v) => ScalarValue::Utf8(Some(v)),

            // Binary
            PartitionValue::Bytes(v) => ScalarValue::Binary(Some(v)),

            // Boolean
            PartitionValue::Boolean(v) => ScalarValue::Boolean(Some(v)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_value_null() {
        let value = PartitionValue::Null;
        assert!(value.is_null());
        assert_eq!(value.type_name(), "null");
        assert_eq!(value.to_string(), "null");
    }

    #[test]
    fn test_partition_value_signed_integers() {
        let value = PartitionValue::Int8(42);
        assert!(!value.is_null());
        assert_eq!(value.type_name(), "int8");
        assert_eq!(value.to_string(), "42");

        let value = PartitionValue::Int16(-1000);
        assert_eq!(value.type_name(), "int16");
        assert_eq!(value.to_string(), "-1000");

        let value = PartitionValue::Int32(123456);
        assert_eq!(value.type_name(), "int32");
        assert_eq!(value.to_string(), "123456");

        let value = PartitionValue::Int64(-9876543210);
        assert_eq!(value.type_name(), "int64");
        assert_eq!(value.to_string(), "-9876543210");
    }

    #[test]
    fn test_partition_value_unsigned_integers() {
        let value = PartitionValue::UInt8(255);
        assert_eq!(value.type_name(), "uint8");
        assert_eq!(value.to_string(), "255");

        let value = PartitionValue::UInt16(65535);
        assert_eq!(value.type_name(), "uint16");
        assert_eq!(value.to_string(), "65535");

        let value = PartitionValue::UInt32(4294967295);
        assert_eq!(value.type_name(), "uint32");
        assert_eq!(value.to_string(), "4294967295");

        let value = PartitionValue::UInt64(18446744073709551615);
        assert_eq!(value.type_name(), "uint64");
        assert_eq!(value.to_string(), "18446744073709551615");
    }

    #[test]
    fn test_partition_value_string() {
        let value = PartitionValue::String("hello world".to_string());
        assert_eq!(value.type_name(), "string");
        assert_eq!(value.to_string(), "hello world");
    }

    #[test]
    fn test_partition_value_bytes() {
        let bytes = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello" in hex
        let value = PartitionValue::Bytes(bytes);
        assert_eq!(value.type_name(), "bytes");
        assert_eq!(value.to_string(), "48656c6c6f");
    }

    #[test]
    fn test_partition_value_boolean() {
        let value = PartitionValue::Boolean(true);
        assert_eq!(value.type_name(), "boolean");
        assert_eq!(value.to_string(), "true");

        let value = PartitionValue::Boolean(false);
        assert_eq!(value.type_name(), "boolean");
        assert_eq!(value.to_string(), "false");
    }

    #[test]
    fn test_try_from_scalar_value_success() {
        // Test null
        let scalar = ScalarValue::Null;
        let partition = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(partition, PartitionValue::Null);

        // Test signed integers
        let scalar = ScalarValue::Int32(Some(42));
        let partition = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(partition, PartitionValue::Int32(42));

        // Test unsigned integers
        let scalar = ScalarValue::UInt64(Some(123));
        let partition = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(partition, PartitionValue::UInt64(123));

        // Test string
        let scalar = ScalarValue::Utf8(Some("test".to_string()));
        let partition = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(partition, PartitionValue::String("test".to_string()));

        // Test large string
        let scalar = ScalarValue::LargeUtf8(Some("large_test".to_string()));
        let partition = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(partition, PartitionValue::String("large_test".to_string()));

        // Test binary
        let bytes = vec![1, 2, 3, 4];
        let scalar = ScalarValue::Binary(Some(bytes.clone()));
        let partition = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(partition, PartitionValue::Bytes(bytes));

        // Test boolean
        let scalar = ScalarValue::Boolean(Some(true));
        let partition = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(partition, PartitionValue::Boolean(true));
    }

    #[test]
    fn test_try_from_scalar_value_null_variants() {
        // Test null variants of supported types
        let scalar = ScalarValue::Int32(None);
        let partition = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(partition, PartitionValue::Null);

        let scalar = ScalarValue::Utf8(None);
        let partition = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(partition, PartitionValue::Null);

        let scalar = ScalarValue::Boolean(None);
        let partition = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(partition, PartitionValue::Null);
    }

    #[test]
    fn test_try_from_scalar_value_unsupported() {
        // Test unsupported types
        let scalar = ScalarValue::Float32(Some(std::f32::consts::PI));
        let result = PartitionValue::try_from(scalar);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            PartitionValueError::UnsupportedScalarType { .. }
        ));

        let scalar = ScalarValue::Float64(Some(std::f64::consts::E));
        let result = PartitionValue::try_from(scalar);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            PartitionValueError::UnsupportedScalarType { .. }
        ));
    }

    #[test]
    fn test_from_partition_value_to_scalar_value() {
        // Test null
        let partition = PartitionValue::Null;
        let scalar = ScalarValue::from(partition);
        assert_eq!(scalar, ScalarValue::Null);

        // Test signed integers
        let partition = PartitionValue::Int32(42);
        let scalar = ScalarValue::from(partition);
        assert_eq!(scalar, ScalarValue::Int32(Some(42)));

        // Test unsigned integers
        let partition = PartitionValue::UInt64(123);
        let scalar = ScalarValue::from(partition);
        assert_eq!(scalar, ScalarValue::UInt64(Some(123)));

        // Test string
        let partition = PartitionValue::String("test".to_string());
        let scalar = ScalarValue::from(partition);
        assert_eq!(scalar, ScalarValue::Utf8(Some("test".to_string())));

        // Test binary
        let bytes = vec![1, 2, 3, 4];
        let partition = PartitionValue::Bytes(bytes.clone());
        let scalar = ScalarValue::from(partition);
        assert_eq!(scalar, ScalarValue::Binary(Some(bytes)));

        // Test boolean
        let partition = PartitionValue::Boolean(true);
        let scalar = ScalarValue::from(partition);
        assert_eq!(scalar, ScalarValue::Boolean(Some(true)));
    }

    #[test]
    fn test_round_trip_conversion() {
        // Test that we can convert from ScalarValue to PartitionValue and back
        let original_scalars = vec![
            ScalarValue::Null,
            ScalarValue::Int8(Some(42)),
            ScalarValue::Int16(Some(-1000)),
            ScalarValue::Int32(Some(123456)),
            ScalarValue::Int64(Some(-9876543210)),
            ScalarValue::UInt8(Some(255)),
            ScalarValue::UInt16(Some(65535)),
            ScalarValue::UInt32(Some(4294967295)),
            ScalarValue::UInt64(Some(18446744073709551615)),
            ScalarValue::Utf8(Some("hello".to_string())),
            ScalarValue::Binary(Some(vec![1, 2, 3, 4])),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Boolean(Some(false)),
        ];

        for original in original_scalars {
            let partition = PartitionValue::try_from(original.clone()).unwrap();
            let back_to_scalar = ScalarValue::from(partition);

            // For string types, we normalize to Utf8
            let expected = match original {
                ScalarValue::LargeUtf8(Some(s)) => ScalarValue::Utf8(Some(s)),
                ScalarValue::LargeBinary(Some(b)) => ScalarValue::Binary(Some(b)),
                _ => original,
            };

            assert_eq!(back_to_scalar, expected);
        }
    }

    #[test]
    fn test_partition_value_hash() {
        use std::collections::HashMap;

        let mut map = HashMap::new();
        map.insert(PartitionValue::Int32(42), "forty-two");
        map.insert(PartitionValue::String("hello".to_string()), "greeting");
        map.insert(PartitionValue::Boolean(true), "truth");

        assert_eq!(map.get(&PartitionValue::Int32(42)), Some(&"forty-two"));
        assert_eq!(
            map.get(&PartitionValue::String("hello".to_string())),
            Some(&"greeting")
        );
        assert_eq!(map.get(&PartitionValue::Boolean(true)), Some(&"truth"));
    }
}
