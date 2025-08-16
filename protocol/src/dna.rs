pub mod stream {
    use std::fmt::{self, Debug, Display};

    use serde::{
        de::{self, Deserialize, Deserializer, Visitor},
        ser::{Serialize, SerializeStruct, Serializer},
    };

    pub type DnaMessage = stream_data_response::Message;

    tonic::include_proto!("dna.v2.stream");

    pub const DNA_STREAM_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("dna_stream_v2_descriptor");

    pub fn dna_stream_file_descriptor_set() -> &'static [u8] {
        DNA_STREAM_DESCRIPTOR_SET
    }

    impl DataFinality {
        pub fn is_pending(&self) -> bool {
            matches!(self, DataFinality::Pending)
        }

        pub fn is_accepted(&self) -> bool {
            matches!(self, DataFinality::Accepted)
        }

        pub fn is_finalized(&self) -> bool {
            matches!(self, DataFinality::Finalized)
        }
    }

    impl Display for DataFinality {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                DataFinality::Unknown => write!(f, "unknown"),
                DataFinality::Pending => write!(f, "pending"),
                DataFinality::Accepted => write!(f, "accepted"),
                DataFinality::Finalized => write!(f, "finalized"),
            }
        }
    }

    impl Serialize for DataFinality {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match self {
                DataFinality::Unknown => serializer.serialize_str("unknown"),
                DataFinality::Pending => serializer.serialize_str("pending"),
                DataFinality::Accepted => serializer.serialize_str("accepted"),
                DataFinality::Finalized => serializer.serialize_str("finalized"),
            }
        }
    }

    impl<'de> Deserialize<'de> for DataFinality {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            #[derive(serde::Deserialize)]
            enum Inner {
                #[serde(alias = "unknown", alias = "UNKNOWN")]
                Unknown,
                #[serde(alias = "pending", alias = "PENDING")]
                Pending,
                #[serde(alias = "accepted", alias = "ACCEPTED")]
                Accepted,
                #[serde(alias = "finalized", alias = "FINALIZED")]
                Finalized,
            }

            match Inner::deserialize(deserializer)? {
                Inner::Unknown => Ok(DataFinality::Unknown),
                Inner::Pending => Ok(DataFinality::Pending),
                Inner::Accepted => Ok(DataFinality::Accepted),
                Inner::Finalized => Ok(DataFinality::Finalized),
            }
        }
    }

    impl Cursor {
        /// Creates a new cursor that streams data from the specified block number.
        pub fn new_with_block_number(block_number: u64) -> Option<Self> {
            if block_number == 0 {
                return None;
            }
            Some(Self {
                order_key: block_number.saturating_sub(1),
                unique_key: Vec::new(),
            })
        }

        pub fn new_finalized(order_key: u64) -> Self {
            Self {
                order_key,
                unique_key: Vec::new(),
            }
        }
    }

    impl Serialize for Cursor {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut state = serializer.serialize_struct("Cursor", 2)?;
            state.serialize_field("orderKey", &self.order_key)?;
            let hex_key = format!("0x{}", hex::encode(&self.unique_key));
            state.serialize_field("uniqueKey", &hex_key)?;
            state.end()
        }
    }

    impl fmt::Display for Cursor {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "Cursor({}, 0x{})",
                self.order_key,
                hex::encode(&self.unique_key)
            )
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
                            "orderKey" => {
                                order_key = Some(map.next_value()?);
                            }
                            "uniqueKey" => {
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
                                    &["orderKey", "uniqueKey"],
                                ))
                            }
                        }
                    }
                    let order_key =
                        order_key.ok_or_else(|| de::Error::missing_field("orderKey"))?;
                    let unique_key =
                        unique_key.ok_or_else(|| de::Error::missing_field("uniqueKey"))?;
                    Ok(Cursor {
                        order_key,
                        unique_key,
                    })
                }
            }

            const FIELDS: &[&str] = &["orderKey", "uniqueKey"];
            deserializer.deserialize_struct("Cursor", FIELDS, CursorVisitor)
        }
    }

    struct PrettyCursor<'a>(&'a Option<Cursor>);
    struct PrettyFilter<'a>(&'a Vec<u8>);

    impl Debug for StreamDataRequest {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let cursor = PrettyCursor(&self.starting_cursor);
            let filter = self.filter.iter().map(PrettyFilter).collect::<Vec<_>>();
            f.debug_struct("StreamDataRequest")
                .field("starting_cursor", &cursor)
                .field("finality", &self.finality)
                .field("filter", &filter)
                .field("heartbeat_interval", &self.heartbeat_interval)
                .finish()
        }
    }

    impl Debug for PrettyCursor<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self.0 {
                Some(cursor) => write!(f, "{}", cursor),
                None => write!(f, "None"),
            }
        }
    }

    impl Debug for PrettyFilter<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "<{} bytes>", self.0.len())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dna::stream::{Cursor, DataFinality};

    #[test]
    fn test_cursor_serialization() {
        let cursor = Cursor {
            order_key: 1,
            unique_key: vec![0, 1, 2, 3],
        };
        let serialized = serde_json::to_string(&cursor).unwrap();
        assert_eq!(serialized, r#"{"orderKey":1,"uniqueKey":"0x00010203"}"#);
        let back: Cursor = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cursor, back);
    }

    #[test]
    fn test_data_finality_serialization() {
        let serialized = serde_json::to_string(&DataFinality::Unknown).unwrap();
        assert_eq!(serialized, r#""unknown""#);
        let back: DataFinality = serde_json::from_str(&serialized).unwrap();
        assert_eq!(back, DataFinality::Unknown);
        let back: DataFinality = serde_json::from_str(r#""UNKNOWN""#).unwrap();
        assert_eq!(back, DataFinality::Unknown);

        let serialized = serde_json::to_string(&DataFinality::Pending).unwrap();
        assert_eq!(serialized, r#""pending""#);
        let back: DataFinality = serde_json::from_str(&serialized).unwrap();
        assert_eq!(back, DataFinality::Pending);
        let back: DataFinality = serde_json::from_str(r#""PENDING""#).unwrap();
        assert_eq!(back, DataFinality::Pending);

        let serialized = serde_json::to_string(&DataFinality::Accepted).unwrap();
        assert_eq!(serialized, r#""accepted""#);
        let back: DataFinality = serde_json::from_str(&serialized).unwrap();
        assert_eq!(back, DataFinality::Accepted);
        let back: DataFinality = serde_json::from_str(r#""ACCEPTED""#).unwrap();
        assert_eq!(back, DataFinality::Accepted);

        let serialized = serde_json::to_string(&DataFinality::Finalized).unwrap();
        assert_eq!(serialized, r#""finalized""#);
        let back: DataFinality = serde_json::from_str(&serialized).unwrap();
        assert_eq!(back, DataFinality::Finalized);
        let back: DataFinality = serde_json::from_str(r#""FINALIZED""#).unwrap();
        assert_eq!(back, DataFinality::Finalized);
    }
}
