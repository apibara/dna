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
                    return Err(report!(crate::error::DecodeError)).attach_printable_lazy(|| {
                        format!("{} bytes is too big", stringify!($typ))
                    });
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
                // must be at least 0x
                if !s.starts_with("0x") {
                    return Err(report!(crate::error::DecodeError)).attach_printable_lazy(|| {
                        format!("missing 0x prefix in {}", stringify!($typ))
                    });
                }

                // hex requires the string to be even-sized. If it's not, we copy it and add a leading 0.
                let bytes = if s.len() % 2 == 1 {
                    let even_sized = format!("0{}", &s[2..]);
                    hex::decode(even_sized)
                        .change_context(crate::error::DecodeError)
                        .attach_printable_lazy(|| {
                            format!("failed to decode {} scalar", stringify!($typ))
                        })?
                } else {
                    // skip 0x prefix
                    hex::decode(&s[2..])
                        .change_context(crate::error::DecodeError)
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

pub(crate) use impl_scalar_helpers;

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

pub(crate) use impl_u256_scalar;

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
