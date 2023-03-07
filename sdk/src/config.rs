use crate::pb::{
    starknet::v1alpha2::{
        EventFilter, FieldElement, Filter, HeaderFilter, StateUpdateFilter, StorageDiffFilter,
    },
    stream::v1alpha2::{Cursor, DataFinality},
};

#[derive(Debug, Clone)]
pub struct Configuration {
    pub batch_size: u64,
    pub starting_cursor: Option<Cursor>,
    pub finality: Option<i32>,
    pub filter: Option<Filter>,
}
impl Configuration {
    pub fn new(
        batch_size: u64,
        starting_cursor: Option<Cursor>,
        finality: Option<i32>,
        filter: Option<Filter>,
    ) -> Self {
        Self {
            batch_size,
            starting_cursor,
            finality,
            filter,
        }
    }

    /// Configures batch size
    pub fn with_batch_size(&mut self, batch_size: u64) -> &mut Self {
        self.batch_size = batch_size;
        self
    }

    /// Configure starting_block
    pub fn starting_at_block(&mut self, block_number: u64) -> &mut Self {
        self.starting_cursor = Some(Cursor {
            order_key: block_number,
            unique_key: vec![],
        });
        self
    }

    /// Configure DataFinality
    /// DataFinality::DataStatusUnknown
    /// DataFinality::DataStatusPending
    /// DataFinality::DataStatusAccepted
    /// DataFinality::DataStatusFinalized
    pub fn with_finality(&mut self, finality: DataFinality) -> &mut Self {
        self.finality = Some(finality.into());
        self
    }

    /// Configure whole filter directly
    pub fn with_filter<F>(&mut self, filter_closure: F) -> &mut Self
    where
        F: Fn(Filter) -> Filter,
    {
        self.filter = Some(filter_closure(Filter::default()));
        self
    }
}

impl Filter {
    /// Configure filter header
    pub fn with_header(&mut self, header: HeaderFilter) -> Self {
        self.header = Some(header);
        self.clone()
    }

    /// Add event to subscribe to
    pub fn add_event<F>(&mut self, closure: F) -> Self
    where
        F: Fn(EventFilter) -> EventFilter,
    {
        self.events.push(closure(EventFilter::default()));
        self.clone()
    }
}

impl EventFilter {
    pub fn with_contract_address(&mut self, address: FieldElement) -> Self {
        self.from_address = Some(address);
        self.clone()
    }
}

impl StateUpdateFilter {
    pub fn add_storage_diff<F>(mut self, closure: F) -> Self
    where
        F: Fn(Vec<StorageDiffFilter>) -> Vec<StorageDiffFilter>,
    {
        self.storage_diffs = closure(self.storage_diffs);
        self
    }
}

impl StorageDiffFilter {}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            batch_size: 1,
            starting_cursor: None,
            finality: None,
            filter: Some(Filter::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pb::{
        starknet::v1alpha2::{FieldElement, HeaderFilter},
        stream::v1alpha2::DataFinality,
    };

    use super::Configuration;

    #[test]
    fn test_config() {
        let config = Configuration::default();
        assert_eq!(1, config.batch_size);
    }

    #[test]
    fn test_config_from() {
        let config = Configuration::default();
        let new_config = Configuration::from(config);
        assert_eq!(1, new_config.batch_size);
    }

    #[test]
    fn test_config_can_be_configured() {
        let mut config = Configuration::default();
        config
            .with_batch_size(10)
            .starting_at_block(111)
            .with_finality(DataFinality::DataStatusAccepted)
            .with_filter(|mut filter| {
                filter.with_header(HeaderFilter { weak: true });
                filter.add_event(|mut event| {
                    event.with_contract_address(FieldElement::from_bytes(&[
                        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                        21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
                    ]))
                });
                filter
            });

        assert_eq!(10, config.batch_size);
        assert_eq!(111, config.starting_cursor.unwrap().order_key);
        assert_eq!(
            <DataFinality as Into<i32>>::into(DataFinality::DataStatusAccepted),
            config.finality.unwrap()
        );
        assert_eq!(true, config.filter.unwrap().header.unwrap().weak);
    }
}
