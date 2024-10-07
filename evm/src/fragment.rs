//! Fragment constants.

// Make sure the fragment IDs match the field tags in the protobuf Block message.

pub const WITHDRAWAL_FRAGMENT_ID: u8 = 2;
pub const WITHDRAWAL_FRAGMENT_NAME: &str = "withdrawal";

pub const TRANSACTION_FRAGMENT_ID: u8 = 3;
pub const TRANSACTION_FRAGMENT_NAME: &str = "transaction";

pub const RECEIPT_FRAGMENT_ID: u8 = 4;
pub const RECEIPT_FRAGMENT_NAME: &str = "receipt";

pub const LOG_FRAGMENT_ID: u8 = 5;
pub const LOG_FRAGMENT_NAME: &str = "log";

pub const INDEX_WITHDRAWAL_BY_VALIDATOR_INDEX: u8 = 0;
pub const INDEX_WITHDRAWAL_BY_ADDRESS: u8 = 1;

pub const INDEX_TRANSACTION_BY_FROM_ADDRESS: u8 = 0;
pub const INDEX_TRANSACTION_BY_TO_ADDRESS: u8 = 1;
pub const INDEX_TRANSACTION_BY_CREATE: u8 = 2;
pub const INDEX_TRANSACTION_BY_STATUS: u8 = 3;

// No receipts index.

pub const INDEX_LOG_BY_ADDRESS: u8 = 0;
pub const INDEX_LOG_BY_TOPIC0: u8 = 1;
pub const INDEX_LOG_BY_TOPIC1: u8 = 2;
pub const INDEX_LOG_BY_TOPIC2: u8 = 3;
pub const INDEX_LOG_BY_TOPIC3: u8 = 4;
pub const INDEX_LOG_BY_TOPIC_LENGTH: u8 = 5;
pub const INDEX_LOG_BY_TRANSACTION_STATUS: u8 = 6;
