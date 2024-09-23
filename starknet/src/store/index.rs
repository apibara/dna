use apibara_dna_common::store::index::TaggedIndex;

use super::fragment;

/// Address -> EventIndex
///
/// Used by: EventFilter.address
pub struct IndexEventByAddress;

impl TaggedIndex for IndexEventByAddress {
    type Key = fragment::FieldElement;

    fn tag() -> u8 {
        1
    }

    fn key_size() -> usize {
        32
    }

    fn name() -> &'static str {
        "event_by_address"
    }
}

/// FieldElement -> EventIndex
///
/// Used by: EventFilter.keys
pub struct IndexEventByKey0;

impl TaggedIndex for IndexEventByKey0 {
    type Key = fragment::FieldElement;

    fn tag() -> u8 {
        2
    }

    fn key_size() -> usize {
        32
    }

    fn name() -> &'static str {
        "event_by_key_0"
    }
}

/// FieldElement -> EventIndex
///
/// Used by: EventFilter.keys
pub struct IndexEventByKey1;

impl TaggedIndex for IndexEventByKey1 {
    type Key = fragment::FieldElement;

    fn tag() -> u8 {
        3
    }

    fn key_size() -> usize {
        32
    }

    fn name() -> &'static str {
        "event_by_key_1"
    }
}

/// FieldElement -> EventIndex
///
/// Used by: EventFilter.keys
pub struct IndexEventByKey2;

impl TaggedIndex for IndexEventByKey2 {
    type Key = fragment::FieldElement;

    fn tag() -> u8 {
        4
    }

    fn key_size() -> usize {
        32
    }

    fn name() -> &'static str {
        "event_by_key_2"
    }
}

/// FieldElement -> EventIndex
///
/// Used by: EventFilter.keys
pub struct IndexEventByKey3;

impl TaggedIndex for IndexEventByKey3 {
    type Key = fragment::FieldElement;

    fn tag() -> u8 {
        5
    }

    fn key_size() -> usize {
        32
    }

    fn name() -> &'static str {
        "event_by_key_3"
    }
}

/// TransactionIndex -> EventIndex
///
/// Used by: EventFilter.include_siblings
/// Used by: MessageFilter.include_events
/// Used by: TransactionFilter.include_events
pub struct IndexEventByTransactionIndex;

impl TaggedIndex for IndexEventByTransactionIndex {
    type Key = u32;

    fn tag() -> u8 {
        6
    }

    fn key_size() -> usize {
        4
    }

    fn name() -> &'static str {
        "event_by_transaction_index"
    }
}

/// EventIndex -> TransactionIndex
///
/// Use by: EventFilter.include_transaction
/// Use by: EventFilter.include_receipt
/// Use by: EventFilter.include_messages
/// Use by: EventFilter.include_siblings
pub struct IndexTransactionByEventIndex;

impl TaggedIndex for IndexTransactionByEventIndex {
    type Key = u32;

    fn tag() -> u8 {
        7
    }

    fn key_size() -> usize {
        4
    }

    fn name() -> &'static str {
        "transaction_by_event_index"
    }
}

/// MessageIndex -> TransactionIndex
///
/// Used by: MessageFilter.include_transaction
/// Used by: MessageFilter.include_receipt
/// Used by: MessageFilter.include_events
/// Used by: MessageFilter.include_siblings
pub struct IndexTransactionByMessageIndex;

impl TaggedIndex for IndexTransactionByMessageIndex {
    type Key = u32;

    fn tag() -> u8 {
        8
    }

    fn key_size() -> usize {
        4
    }

    fn name() -> &'static str {
        "transaction_by_message_index"
    }
}

/// TransactionType -> TransactionIndex
///
/// Used by: TransactionFilter.inner
pub struct IndexTransactionByType;

impl TaggedIndex for IndexTransactionByType {
    type Key = fragment::TransactionType;

    fn tag() -> u8 {
        9
    }

    fn key_size() -> usize {
        1
    }

    fn name() -> &'static str {
        "transaction_by_type"
    }
}

/// TransactionIndex -> MessageIndex
///
/// Used by: EventFilter.include_messages
/// Used by: MessageFilter.include_siblings
/// Used by: TransactionFilter.include_messages
pub struct IndexMessageByTransactionIndex;

impl TaggedIndex for IndexMessageByTransactionIndex {
    type Key = u32;

    fn tag() -> u8 {
        10
    }

    fn key_size() -> usize {
        4
    }

    fn name() -> &'static str {
        "message_by_transaction_index"
    }
}

/// Address -> MessageIndex
///
/// Used by: MessageFilter.from_address
pub struct IndexMessageByFromAddress;

impl TaggedIndex for IndexMessageByFromAddress {
    type Key = fragment::FieldElement;

    fn tag() -> u8 {
        11
    }

    fn key_size() -> usize {
        32
    }

    fn name() -> &'static str {
        "message_by_from_address"
    }
}

/// Address -> MessageIndex
///
/// Used by: MessageFilter.to_address
pub struct IndexMessageByToAddress;

impl TaggedIndex for IndexMessageByToAddress {
    type Key = fragment::FieldElement;

    fn tag() -> u8 {
        12
    }

    fn key_size() -> usize {
        32
    }

    fn name() -> &'static str {
        "message_by_to_address"
    }
}
