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
