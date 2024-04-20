use error_stack::{report, Result, ResultExt};
use serde::{de::Deserialize, ser::Serialize};

tonic::include_proto!("evm.v2");
tonic::include_proto!("evm.v2.serde");

#[derive(Debug)]
pub struct DecodeError;

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to decode EVM scalar")
    }
}

impl error_stack::Context for DecodeError {}

macro_rules! impl_scalar_traits {
    ($typ:ident) => {
        impl std::fmt::Display for $typ {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.to_hex())
            }
        }

        impl std::hash::Hash for $typ {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.to_bytes().hash(state);
            }
        }
        impl Serialize for $typ {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(&self.to_hex())
            }
        }

        impl<'de> Deserialize<'de> for $typ {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let s = String::deserialize(deserializer)?;
                <$typ>::from_hex(&s).map_err(serde::de::Error::custom)
            }
        }
    };
}

macro_rules! impl_scalar_helpers {
    ($typ:ident, $size:literal) => {
        impl $typ {
            pub fn from_slice(bytes: &[u8]) -> Result<Self, DecodeError> {
                // number is too big
                let size = bytes.len();
                if size > $size {
                    return Err(report!(DecodeError)).attach_printable_lazy(|| {
                        format!("{} bytes is too big", stringify!($typ))
                    });
                }
                let mut bytes_array = [0u8; $size];
                bytes_array[$size - size..].copy_from_slice(bytes);
                Ok(Self::from_bytes(&bytes_array))
            }

            pub fn from_hex(s: &str) -> Result<Self, DecodeError> {
                // must be at least 0x
                if !s.starts_with("0x") {
                    return Err(report!(DecodeError)).attach_printable_lazy(|| {
                        format!("missing 0x prefix in {}", stringify!($typ))
                    });
                }

                // hex requires the string to be even-sized. If it's not, we copy it and add a leading 0.
                let bytes = if s.len() % 2 == 1 {
                    let even_sized = format!("0{}", &s[2..]);
                    hex::decode(even_sized)
                        .change_context(DecodeError)
                        .attach_printable_lazy(|| {
                            format!("failed to decode {} scalar", stringify!($typ))
                        })?
                } else {
                    // skip 0x prefix
                    hex::decode(&s[2..])
                        .change_context(DecodeError)
                        .attach_printable_lazy(|| {
                            format!("failed to decode {} scalar", stringify!($typ))
                        })?
                };

                Self::from_slice(&bytes)
            }

            pub fn to_hex(&self) -> String {
                format!("0x{}", hex::encode(self.to_bytes()))
            }
        }
    };
}

impl_scalar_traits!(Address);
impl_scalar_helpers!(Address, 20);

impl Address {
    pub fn from_bytes(bytes: &[u8; 20]) -> Self {
        let lo_lo = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        let lo_hi = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        let hi = u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);

        Address { lo_lo, lo_hi, hi }
    }

    pub fn to_bytes(&self) -> [u8; 20] {
        let lo_lo = self.lo_lo.to_be_bytes();
        let lo_hi = self.lo_hi.to_be_bytes();
        let hi = self.hi.to_be_bytes();
        [
            lo_lo[0], lo_lo[1], lo_lo[2], lo_lo[3], lo_lo[4], lo_lo[5], lo_lo[6], lo_lo[7], //
            lo_hi[0], lo_hi[1], lo_hi[2], lo_hi[3], lo_hi[4], lo_hi[5], lo_hi[6], lo_hi[7], //
            hi[0], hi[1], hi[2], hi[3],
        ]
    }
}

impl_scalar_traits!(U128);
impl_scalar_helpers!(U128, 16);

impl U128 {
    pub fn from_bytes(bytes: &[u8; 16]) -> Self {
        let lo = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        let hi = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);

        U128 { lo, hi }
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        let lo = self.lo.to_be_bytes();
        let hi = self.hi.to_be_bytes();
        [
            lo[0], lo[1], lo[2], lo[3], lo[4], lo[5], lo[6], lo[7], //
            hi[0], hi[1], hi[2], hi[3], hi[4], hi[5], hi[6], hi[7], //
        ]
    }
}

macro_rules! impl_u256_scalar {
    ($typ:ident) => {
        impl_scalar_traits!($typ);
        impl_scalar_helpers!($typ, 32);

        impl $typ {
            pub fn from_bytes(bytes: &[u8; 32]) -> Self {
                let lo_lo = u64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                let lo_hi = u64::from_be_bytes([
                    bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                    bytes[15],
                ]);
                let hi_lo = u64::from_be_bytes([
                    bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22],
                    bytes[23],
                ]);
                let hi_hi = u64::from_be_bytes([
                    bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30],
                    bytes[31],
                ]);

                $typ {
                    lo_lo,
                    lo_hi,
                    hi_lo,
                    hi_hi,
                }
            }

            pub fn to_bytes(&self) -> [u8; 32] {
                let lo_lo = self.lo_lo.to_be_bytes();
                let lo_hi = self.lo_hi.to_be_bytes();
                let hi_lo = self.hi_lo.to_be_bytes();
                let hi_hi = self.hi_hi.to_be_bytes();
                [
                    lo_lo[0], lo_lo[1], lo_lo[2], lo_lo[3], lo_lo[4], lo_lo[5], lo_lo[6], lo_lo[7],
                    lo_hi[0], lo_hi[1], lo_hi[2], lo_hi[3], lo_hi[4], lo_hi[5], lo_hi[6], lo_hi[7],
                    hi_lo[0], hi_lo[1], hi_lo[2], hi_lo[3], hi_lo[4], hi_lo[5], hi_lo[6], hi_lo[7],
                    hi_hi[0], hi_hi[1], hi_hi[2], hi_hi[3], hi_hi[4], hi_hi[5], hi_hi[6], hi_hi[7],
                ]
            }
        }
    };
}

impl_u256_scalar!(U256);
impl_u256_scalar!(B256);

#[cfg(test)]
mod tests {}
