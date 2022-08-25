//! Message storage related tables.

use std::marker::PhantomData;

use apibara_core::stream::Sequence;
use prost::Message;

use super::Table;

/// Table with messages by sequence.
#[derive(Debug, Clone, Copy, Default)]
pub struct MessageTable<M: Message> {
    phantom: PhantomData<M>,
}

impl<M> Table for MessageTable<M>
where
    M: Message + Default,
{
    type Key = Sequence;
    type Value = M;

    fn db_name() -> &'static str {
        "Message"
    }
}
