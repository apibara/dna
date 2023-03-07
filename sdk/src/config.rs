use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use prost::Message;

#[derive(Debug, Clone)]
pub struct Configuration<F: Message + Default> {
    pub batch_size: u64,
    pub starting_cursor: Option<Cursor>,
    pub finality: Option<i32>,
    pub filter: F,
}

impl<F> Configuration<F>
where
    F: Message + Default,
{
    pub fn new(
        batch_size: u64,
        starting_cursor: Option<Cursor>,
        finality: Option<i32>,
        filter: F,
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
    pub fn with_filter<G>(&mut self, filter_closure: G) -> &mut Self
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
        let mut config = Configuration::<Filter>::default();
        config
            .with_batch_size(10)
            .starting_at_block(111)
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
        assert_eq!(
            <DataFinality as Into<i32>>::into(DataFinality::DataStatusAccepted),
            config.finality.unwrap()
        );
        assert_eq!(true, config.filter.header.unwrap().weak);
    }
}
