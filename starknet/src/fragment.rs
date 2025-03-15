//! Fragment constants.

// Make sure the fragment IDs match the field tags in the protobuf Block message.

pub const TRANSACTION_FRAGMENT_ID: u8 = 2;
pub const TRANSACTION_FRAGMENT_NAME: &str = "transaction";

pub const RECEIPT_FRAGMENT_ID: u8 = 3;
pub const RECEIPT_FRAGMENT_NAME: &str = "receipt";

pub const EVENT_FRAGMENT_ID: u8 = 4;
pub const EVENT_FRAGMENT_NAME: &str = "event";

pub const MESSAGE_FRAGMENT_ID: u8 = 5;
pub const MESSAGE_FRAGMENT_NAME: &str = "message";

pub const STORAGE_DIFF_FRAGMENT_ID: u8 = 6;
pub const STORAGE_DIFF_FRAGMENT_NAME: &str = "storage_diff";

pub const CONTRACT_CHANGE_FRAGMENT_ID: u8 = 7;
pub const CONTRACT_CHANGE_FRAGMENT_NAME: &str = "contract_change";

pub const NONCE_UPDATE_FRAGMENT_ID: u8 = 8;
pub const NONCE_UPDATE_FRAGMENT_NAME: &str = "nonce_update";

pub const TRACE_FRAGMENT_ID: u8 = 9;
pub const TRACE_FRAGMENT_NAME: &str = "trace";

pub const INDEX_TRANSACTION_BY_STATUS: u8 = 0;
pub const INDEX_TRANSACTION_BY_TYPE: u8 = 1;

// No receipt indexes.

pub const INDEX_EVENT_BY_ADDRESS: u8 = 0;
pub const INDEX_EVENT_BY_KEY0: u8 = 1;
pub const INDEX_EVENT_BY_KEY1: u8 = 2;
pub const INDEX_EVENT_BY_KEY2: u8 = 3;
pub const INDEX_EVENT_BY_KEY3: u8 = 4;
pub const INDEX_EVENT_BY_KEY_LENGTH: u8 = 5;
pub const INDEX_EVENT_BY_TRANSACTION_STATUS: u8 = 6;

pub const INDEX_MESSAGE_BY_FROM_ADDRESS: u8 = 0;
pub const INDEX_MESSAGE_BY_TO_ADDRESS: u8 = 1;
pub const INDEX_MESSAGE_BY_TRANSACTION_STATUS: u8 = 2;

pub const INDEX_STORAGE_DIFF_BY_CONTRACT_ADDRESS: u8 = 0;

pub const INDEX_CONTRACT_CHANGE_BY_TYPE: u8 = 0;

pub const INDEX_NONCE_UPDATE_BY_CONTRACT_ADDRESS: u8 = 0;

// No trace indexes.
