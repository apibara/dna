use error_stack::{report, Result, ResultExt};

use crate::helpers::{impl_scalar_helpers, impl_scalar_traits, impl_u256_scalar};

tonic::include_proto!("starknet.v2");

impl_u256_scalar!(FieldElement);
