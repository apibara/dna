use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use prost::Message;

/// Data stream configuration.
#[derive(Debug, Clone)]
pub struct Configuration<F: Message + Default> {
    /// Number of blocks per batch.
    pub batch_size: u64,
    /// Starting cursor.
    pub starting_cursor: Option<Cursor>,
    /// Data finality.
    pub finality: Option<DataFinality>,
    /// The data filter.
    pub filter: F,
}

impl<F> Configuration<F>
where
    F: Message + Default,
{
    /// Creates a new configuration with the given fields.
    pub fn new(
        batch_size: u64,
        starting_cursor: Option<Cursor>,
        finality: Option<DataFinality>,
        filter: F,
    ) -> Self {
        Self {
            batch_size,
            starting_cursor,
            finality,
            filter,
        }
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, batch_size: u64) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the starting cursor to start at the given block.
    pub fn with_starting_cursor(mut self, cursor: Cursor) -> Self {
        self.starting_cursor = Some(cursor);
        self
    }

    /// Set the starting cursor to start at the given block.
    pub fn with_starting_block(mut self, block_number: u64) -> Self {
        self.starting_cursor = Some(Cursor {
            order_key: block_number,
            unique_key: vec![],
        });
        self
    }

    /// Set the requested data finality.
    pub fn with_finality(mut self, finality: DataFinality) -> Self {
        self.finality = Some(finality);
        self
    }

    /// Configure the data filter.
    pub fn with_filter<G>(mut self, filter_closure: G) -> Self
    where
        G: Fn(F) -> F,
    {
        self.filter = filter_closure(F::default());
        self
    }
}

impl<F> Default for Configuration<F>
where
    F: Message + Default,
{
    fn default() -> Self {
        Self {
            batch_size: 1,
            starting_cursor: None,
            finality: None,
            filter: F::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use apibara_core::{
        node::v1alpha2::DataFinality,
        starknet::v1alpha2::{FieldElement, Filter, HeaderFilter},
    };

    use super::Configuration;

    #[test]
    fn test_config() {
        let config = Configuration::<Filter>::default();
        assert_eq!(1, config.batch_size);
    }

    #[test]
    fn test_config_from() {
        let config = Configuration::<Filter>::default();
        let new_config = Configuration::<Filter>::from(config);
        assert_eq!(1, new_config.batch_size);
    }

    #[test]
    fn test_config_can_be_configured() {
        let config = Configuration::<Filter>::default()
            .with_batch_size(10)
            .with_starting_block(111)
            .with_finality(DataFinality::DataStatusAccepted)
            .with_filter(|filter| {
                filter
                    .with_header(HeaderFilter { weak: true })
                    .add_event(|event| {
                        event.with_contract_address(FieldElement::from_bytes(&[
                            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                            20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
                        ]))
                    })
            });

        assert_eq!(10, config.batch_size);
        assert_eq!(111, config.starting_cursor.unwrap().order_key);
        assert_eq!(DataFinality::DataStatusAccepted, config.finality.unwrap());
        assert_eq!(true, config.filter.header.unwrap().weak);
    }
}
