//! Fragment constants.

// Make sure the fragment IDs match the field tags in the protobuf Block message.

pub const TRANSACTION_FRAGMENT_ID: u8 = 2;
pub const TRANSACTION_FRAGMENT_NAME: &str = "transaction";

pub const VALIDATOR_FRAGMENT_ID: u8 = 3;
pub const VALIDATOR_FRAGMENT_NAME: &str = "validator";

pub const BLOB_FRAGMENT_ID: u8 = 4;
pub const BLOB_FRAGMENT_NAME: &str = "blob";

pub const INDEX_TRANSACTION_BY_FROM_ADDRESS: u8 = 0;
pub const INDEX_TRANSACTION_BY_TO_ADDRESS: u8 = 1;
pub const INDEX_TRANSACTION_BY_CREATE: u8 = 2;

pub const INDEX_VALIDATOR_BY_INDEX: u8 = 0;
pub const INDEX_VALIDATOR_BY_STATUS: u8 = 1;
