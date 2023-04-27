pub mod v1alpha2 {
    use serde::ser::{Serialize, SerializeStruct, Serializer};

    tonic::include_proto!("apibara.node.v1alpha2");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("node_v1alpha2_descriptor");

    pub fn node_file_descriptor_set() -> &'static [u8] {
        FILE_DESCRIPTOR_SET
    }

    impl Serialize for Cursor {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut state = serializer.serialize_struct("Cursor", 2)?;
            state.serialize_field("order_key", &self.order_key)?;
            let hex_key = format!("0x{}", hex::encode(&self.unique_key));
            state.serialize_field("unique_key", &hex_key)?;
            state.end()
        }
    }

    impl Serialize for DataFinality {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let as_str = match self {
                DataFinality::DataStatusFinalized => "finalized",
                DataFinality::DataStatusAccepted => "accepted",
                DataFinality::DataStatusPending => "pending",
                DataFinality::DataStatusUnknown => "unknown",
            };
            serializer.serialize_str(as_str)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::node::v1alpha2::DataFinality;

    #[test]
    fn test_cursor_serialization() {
        let cursor = super::v1alpha2::Cursor {
            order_key: 1,
            unique_key: vec![0, 1, 2, 3],
        };
        let serialized = serde_json::to_string(&cursor).unwrap();
        assert_eq!(serialized, r#"{"order_key":1,"unique_key":"0x00010203"}"#);
    }

    #[test]
    fn test_data_finality_serialization() {
        let serialized = serde_json::to_string(&DataFinality::DataStatusUnknown).unwrap();
        assert_eq!(serialized, r#""unknown""#);
        let serialized = serde_json::to_string(&DataFinality::DataStatusPending).unwrap();
        assert_eq!(serialized, r#""pending""#);
        let serialized = serde_json::to_string(&DataFinality::DataStatusAccepted).unwrap();
        assert_eq!(serialized, r#""accepted""#);
        let serialized = serde_json::to_string(&DataFinality::DataStatusFinalized).unwrap();
        assert_eq!(serialized, r#""finalized""#);
    }
}
