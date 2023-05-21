pub mod v1alpha2 {
    use std::fmt;

    use serde::{
        de::{self, Deserialize, Deserializer, Visitor},
        ser::{Serialize, SerializeStruct, Serializer},
    };

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

    impl<'de> Deserialize<'de> for Cursor {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            // visitors are usually implemented inside the deserialize function.
            // we follow this convention.
            struct CursorVisitor;

            impl<'de> Visitor<'de> for CursorVisitor {
                type Value = Cursor;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("struct Cursor")
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: serde::de::MapAccess<'de>,
                {
                    let mut order_key = None;
                    let mut unique_key = None;
                    while let Some(key) = map.next_key()? {
                        match key {
                            "order_key" => {
                                order_key = Some(map.next_value()?);
                            }
                            "unique_key" => {
                                let hex_value: &str = map.next_value()?;
                                if !hex_value.starts_with("0x") {
                                    return Err(de::Error::invalid_value(
                                        de::Unexpected::Str(hex_value),
                                        &"a hex value with 0x prefix",
                                    ));
                                }
                                let decoded = hex::decode(&hex_value[2..]).map_err(|_| {
                                    de::Error::invalid_value(
                                        de::Unexpected::Str(hex_value),
                                        &"a hex value with 0x prefix",
                                    )
                                })?;
                                unique_key = Some(decoded);
                            }
                            field => {
                                return Err(de::Error::unknown_field(
                                    field,
                                    &["order_key", "unique_key"],
                                ))
                            }
                        }
                    }
                    let order_key =
                        order_key.ok_or_else(|| de::Error::missing_field("order_key"))?;
                    let unique_key =
                        unique_key.ok_or_else(|| de::Error::missing_field("unique_key"))?;
                    Ok(Cursor {
                        order_key,
                        unique_key,
                    })
                }
            }

            const FIELDS: &[&str] = &["order_key", "unique_key"];
            deserializer.deserialize_struct("Cursor", FIELDS, CursorVisitor)
        }
    }

    impl<'de> Deserialize<'de> for DataFinality {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            match Deserialize::deserialize(deserializer)? {
                "finalized" => Ok(DataFinality::DataStatusFinalized),
                "accepted" => Ok(DataFinality::DataStatusAccepted),
                "pending" => Ok(DataFinality::DataStatusPending),
                "unknown" => Ok(DataFinality::DataStatusUnknown),
                value => Err(de::Error::unknown_variant(
                    value,
                    &["finalized", "accepted", "pending", "unknown"],
                )),
            }
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
        let back: super::v1alpha2::Cursor = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cursor, back);
    }

    #[test]
    fn test_data_finality_serialization() {
        let serialized = serde_json::to_string(&DataFinality::DataStatusUnknown).unwrap();
        assert_eq!(serialized, r#""unknown""#);
        let back: DataFinality = serde_json::from_str(&serialized).unwrap();
        assert_eq!(back, DataFinality::DataStatusUnknown);

        let serialized = serde_json::to_string(&DataFinality::DataStatusPending).unwrap();
        assert_eq!(serialized, r#""pending""#);
        let back: DataFinality = serde_json::from_str(&serialized).unwrap();
        assert_eq!(back, DataFinality::DataStatusPending);

        let serialized = serde_json::to_string(&DataFinality::DataStatusAccepted).unwrap();
        assert_eq!(serialized, r#""accepted""#);
        let back: DataFinality = serde_json::from_str(&serialized).unwrap();
        assert_eq!(back, DataFinality::DataStatusAccepted);

        let serialized = serde_json::to_string(&DataFinality::DataStatusFinalized).unwrap();
        assert_eq!(serialized, r#""finalized""#);
        let back: DataFinality = serde_json::from_str(&serialized).unwrap();
        assert_eq!(back, DataFinality::DataStatusFinalized);
    }
}
