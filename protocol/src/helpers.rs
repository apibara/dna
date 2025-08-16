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
    };
}

pub(crate) use impl_scalar_traits;

macro_rules! impl_scalar_helpers {
    ($typ:ident, $size:literal) => {
        impl $typ {
            pub fn from_slice(bytes: &[u8]) -> Result<Self, crate::error::DecodeError> {
                // number is too big
                let size = bytes.len();
                if size > $size {
                    return Err(crate::error::DecodeError::SizeTooBig);
                }
                let mut bytes_array = [0u8; $size];
                bytes_array[$size - size..].copy_from_slice(bytes);
                Ok(Self::from_bytes(&bytes_array))
            }
        }

        impl_scalar_helpers!($typ);
    };
    ($typ:ident) => {
        impl $typ {
            pub fn from_hex(s: &str) -> Result<Self, crate::error::DecodeError> {
                use crate::error::DecodeFromHexSnafu;
                use snafu::ResultExt;
                // must be at least 0x
                if !s.starts_with("0x") {
                    return Err(crate::error::DecodeError::Missing0xPrefix);
                }

                // hex requires the string to be even-sized. If it's not, we copy it and add a leading 0.
                let bytes = if s.len() % 2 == 1 {
                    let even_sized = format!("0{}", &s[2..]);
                    hex::decode(even_sized).context(DecodeFromHexSnafu {})?
                } else {
                    // skip 0x prefix
                    hex::decode(&s[2..]).context(DecodeFromHexSnafu {})?
                };

                Self::from_slice(&bytes)
            }

            pub fn to_hex(&self) -> String {
                format!("0x{}", hex::encode(self.to_bytes()))
            }
        }
    };
}

pub(crate) use impl_scalar_helpers;

// NOTICE: The expansion to x0[..], x1[..], x2[..] should be really a macro.
macro_rules! impl_from_to_bytes {
    ($typ:ident, 16) => {
        impl $typ {
            pub fn from_bytes(bytes: &[u8; 16]) -> Self {
                let x0 = from_be_bytes_slice!(u64, bytes, 0);
                let x1 = from_be_bytes_slice!(u64, bytes, 8);
                $typ { x0, x1 }
            }

            pub fn to_bytes(&self) -> [u8; 16] {
                let x0 = self.x0.to_be_bytes();
                let x1 = self.x1.to_be_bytes();
                [
                    x0[0], x0[1], x0[2], x0[3], x0[4], x0[5], x0[6], x0[7], x1[0], x1[1], x1[2],
                    x1[3], x1[4], x1[5], x1[6], x1[7],
                ]
            }
        }
    };
    ($typ:ident, 20) => {
        impl $typ {
            pub fn from_bytes(bytes: &[u8; 20]) -> Self {
                let x0 = from_be_bytes_slice!(u64, bytes, 0);
                let x1 = from_be_bytes_slice!(u64, bytes, 8);
                let x2 = from_be_bytes_slice!(u32, bytes, 16);
                $typ { x0, x1, x2 }
            }

            pub fn to_bytes(&self) -> [u8; 20] {
                let x0 = self.x0.to_be_bytes();
                let x1 = self.x1.to_be_bytes();
                let x2 = self.x2.to_be_bytes();
                [
                    x0[0], x0[1], x0[2], x0[3], x0[4], x0[5], x0[6], x0[7], x1[0], x1[1], x1[2],
                    x1[3], x1[4], x1[5], x1[6], x1[7], x2[0], x2[1], x2[2], x2[3],
                ]
            }
        }
    };
    ($typ:ident, 32) => {
        impl $typ {
            pub fn from_bytes(bytes: &[u8; 32]) -> Self {
                let x0 = from_be_bytes_slice!(u64, bytes, 0);
                let x1 = from_be_bytes_slice!(u64, bytes, 8);
                let x2 = from_be_bytes_slice!(u64, bytes, 16);
                let x3 = from_be_bytes_slice!(u64, bytes, 24);

                $typ { x0, x1, x2, x3 }
            }

            pub fn to_bytes(&self) -> [u8; 32] {
                let x0 = self.x0.to_be_bytes();
                let x1 = self.x1.to_be_bytes();
                let x2 = self.x2.to_be_bytes();
                let x3 = self.x3.to_be_bytes();
                [
                    x0[0], x0[1], x0[2], x0[3], x0[4], x0[5], x0[6], x0[7], x1[0], x1[1], x1[2],
                    x1[3], x1[4], x1[5], x1[6], x1[7], x2[0], x2[1], x2[2], x2[3], x2[4], x2[5],
                    x2[6], x2[7], x3[0], x3[1], x3[2], x3[3], x3[4], x3[5], x3[6], x3[7],
                ]
            }
        }
    };
    ($typ:ident, 48) => {
        impl $typ {
            pub fn from_bytes(bytes: &[u8; 48]) -> Self {
                let x0 = from_be_bytes_slice!(u64, bytes, 0);
                let x1 = from_be_bytes_slice!(u64, bytes, 8);
                let x2 = from_be_bytes_slice!(u64, bytes, 16);
                let x3 = from_be_bytes_slice!(u64, bytes, 24);
                let x4 = from_be_bytes_slice!(u64, bytes, 32);
                let x5 = from_be_bytes_slice!(u64, bytes, 40);

                $typ {
                    x0,
                    x1,
                    x2,
                    x3,
                    x4,
                    x5,
                }
            }

            pub fn to_bytes(&self) -> [u8; 48] {
                let x0 = self.x0.to_be_bytes();
                let x1 = self.x1.to_be_bytes();
                let x2 = self.x2.to_be_bytes();
                let x3 = self.x3.to_be_bytes();
                let x4 = self.x4.to_be_bytes();
                let x5 = self.x5.to_be_bytes();
                [
                    x0[0], x0[1], x0[2], x0[3], x0[4], x0[5], x0[6], x0[7], x1[0], x1[1], x1[2],
                    x1[3], x1[4], x1[5], x1[6], x1[7], x2[0], x2[1], x2[2], x2[3], x2[4], x2[5],
                    x2[6], x2[7], x3[0], x3[1], x3[2], x3[3], x3[4], x3[5], x3[6], x3[7], x4[0],
                    x4[1], x4[2], x4[3], x4[4], x4[5], x4[6], x4[7], x5[0], x5[1], x5[2], x5[3],
                    x5[4], x5[5], x5[6], x5[7],
                ]
            }
        }
    };
}

pub(crate) use impl_from_to_bytes;

macro_rules! from_be_bytes_slice {
    (u32, $bytes:ident, $start:expr) => {
        u32::from_be_bytes([
            $bytes[$start],
            $bytes[$start + 1],
            $bytes[$start + 2],
            $bytes[$start + 3],
        ])
    };
    (u64, $bytes:ident, $start:expr) => {
        u64::from_be_bytes([
            $bytes[$start],
            $bytes[$start + 1],
            $bytes[$start + 2],
            $bytes[$start + 3],
            $bytes[$start + 4],
            $bytes[$start + 5],
            $bytes[$start + 6],
            $bytes[$start + 7],
        ])
    };
}

pub(crate) use from_be_bytes_slice;
