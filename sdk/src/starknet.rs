//! Starknet-specific types and utilities.
//!
//! This module contains the auto-generated gRPC types that are specific to Starknet.
//!
//! ## Builders
//!
//! We provide the following builders to simplify generating the [Filter].
//!
//!  - [FilterBuilder] - the root filter builder.
//!  - [TransactionFilterBuilder] - the transaction filter builder.
//!  - [EventFilterBuilder] - the event filter builder.
//!  - [MessageToL1FilterBuilder] - the message to L1 filter builder.
//!  - [StorageDiffFilterBuilder] - the storage diff filter builder.
//!  - [ContractChangeFilterBuilder] - the contract change filter builder.
//!  - [NonceUpdateFilterBuilder] - the nonce update filter builder.
pub use apibara_dna_protocol::starknet::*;

/// Builder for [Filter].
#[derive(Debug, Clone)]
pub struct FilterBuilder {
    inner: Filter,
}

/// Builder for [TransactionFilter].
///
/// Generally speaking, you can filter transactions by their type and status.
///
/// You can also "join" the transaction with its:
///
///  - receipt
///  - events
///  - messages to L1
///  - trace (if enabled in the server deployment)
///
/// By default, no join is performed.
#[derive(Debug, Clone)]
pub struct TransactionFilterBuilder {
    inner: TransactionFilter,
}

/// Builder for [EventFilter].
///
/// You can also "join" the event with its:
///
///  - receipt
///  - transaction
///  - siblings (other events emitted by the same transaction)
///  - messages to L1
///  - transaction's trace (if enabled in the server deployment)
///
/// By default, no join is performed.
#[derive(Debug, Clone)]
pub struct EventFilterBuilder {
    inner: EventFilter,
}

/// Builder for [MessageToL1Filter].
///
/// You can also "join" the message with its:
///
///  - receipt
///  - transaction
///  - events emitted by the same transaction
///  - siblings (other messages emitted by the same transaction)
///  - transaction's trace (if enabled in the server deployment)
///
/// By default, no join is performed.
#[derive(Debug, Clone)]
pub struct MessageToL1FilterBuilder {
    inner: MessageToL1Filter,
}

/// Builder for [StorageDiffFilter].
#[derive(Debug, Clone)]
pub struct StorageDiffFilterBuilder {
    inner: StorageDiffFilter,
}

/// Builder for [ContractChangeFilter].
#[derive(Debug, Clone)]
pub struct ContractChangeFilterBuilder {
    inner: ContractChangeFilter,
}

/// Builder for [NonceUpdateFilter].
#[derive(Debug, Clone)]
pub struct NonceUpdateFilterBuilder {
    inner: NonceUpdateFilter,
}

impl FilterBuilder {
    /// Creates a new [FilterBuilder].
    ///
    /// The default header filter is set to [HeaderFilter::OnDataOrOnNewBlock].
    /// All other fields are empty.
    pub fn new() -> Self {
        let inner = Filter {
            header: HeaderFilter::OnDataOrOnNewBlock as i32,
            ..Default::default()
        };
        Self { inner }
    }

    /// Sets the header filter.
    pub fn with_header(mut self, header: HeaderFilter) -> Self {
        self.inner.header = header as i32;
        self
    }

    /// Adds a [TransactionFilter].
    ///
    /// Use [TransactionFilterBuilder] to create a [TransactionFilter].
    pub fn add_transaction(mut self, transaction: TransactionFilter) -> Self {
        self.inner.transactions.push(transaction);
        self
    }

    /// Adds a [EventFilter].
    ///
    /// Use [EventFilterBuilder] to create a [EventFilter].
    pub fn add_event(mut self, event: EventFilter) -> Self {
        self.inner.events.push(event);
        self
    }

    /// Adds a [MessageToL1Filter].
    ///
    /// Use [MessageToL1FilterBuilder] to create a [MessageToL1Filter].
    pub fn add_message(mut self, message: MessageToL1Filter) -> Self {
        self.inner.messages.push(message);
        self
    }

    /// Adds a [StorageDiffFilter].
    ///
    /// Use [StorageDiffFilterBuilder] to create a [StorageDiffFilter].
    pub fn add_storage_diff(mut self, storage_diff: StorageDiffFilter) -> Self {
        self.inner.storage_diffs.push(storage_diff);
        self
    }

    /// Adds a [ContractChangeFilter].
    ///
    /// Use [ContractChangeFilterBuilder] to create a [ContractChangeFilter].
    pub fn add_contract_change(mut self, contract_change: ContractChangeFilter) -> Self {
        self.inner.contract_changes.push(contract_change);
        self
    }

    /// Adds a [NonceUpdateFilter].
    ///
    /// Use [NonceUpdateFilterBuilder] to create a [NonceUpdateFilter].
    pub fn add_nonce_update(mut self, nonce_update: NonceUpdateFilter) -> Self {
        self.inner.nonce_updates.push(nonce_update);
        self
    }

    /// Builds the [Filter].
    pub fn build(self) -> Filter {
        self.inner
    }
}

impl Default for FilterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionFilterBuilder {
    /// Requests transactions of any type.
    pub fn all_transaction_types() -> Self {
        let inner = TransactionFilter {
            inner: None,
            ..Default::default()
        };
        Self { inner }
    }

    /// Requests only "invoke v0" transactions.
    pub fn only_invoke_v0() -> Self {
        use transaction_filter::Inner;
        let inner = TransactionFilter {
            inner: Some(Inner::InvokeV0(InvokeTransactionV0Filter::default())),
            ..Default::default()
        };
        Self { inner }
    }

    /// Requests only "invoke v1" transactions.
    pub fn only_invoke_v1() -> Self {
        use transaction_filter::Inner;
        let inner = TransactionFilter {
            inner: Some(Inner::InvokeV1(InvokeTransactionV1Filter::default())),
            ..Default::default()
        };
        Self { inner }
    }

    /// Requests only "invoke v3" transactions.
    pub fn only_invoke_v3() -> Self {
        use transaction_filter::Inner;
        let inner = TransactionFilter {
            inner: Some(Inner::InvokeV3(InvokeTransactionV3Filter::default())),
            ..Default::default()
        };
        Self { inner }
    }

    /// Requests only "deploy" transactions.
    pub fn only_deploy() -> Self {
        use transaction_filter::Inner;
        let inner = TransactionFilter {
            inner: Some(Inner::Deploy(DeployTransactionFilter::default())),
            ..Default::default()
        };
        Self { inner }
    }

    /// Requests only "declare v0" transactions.
    pub fn only_declare_v0() -> Self {
        use transaction_filter::Inner;
        let inner = TransactionFilter {
            inner: Some(Inner::DeclareV0(DeclareV0TransactionFilter::default())),
            ..Default::default()
        };
        Self { inner }
    }

    /// Requests only "declare v1" transactions.
    pub fn only_declare_v1() -> Self {
        use transaction_filter::Inner;
        let inner = TransactionFilter {
            inner: Some(Inner::DeclareV1(DeclareV1TransactionFilter::default())),
            ..Default::default()
        };
        Self { inner }
    }

    /// Requests only "declare v2" transactions.
    pub fn only_declare_v2() -> Self {
        use transaction_filter::Inner;
        let inner = TransactionFilter {
            inner: Some(Inner::DeclareV2(DeclareV2TransactionFilter::default())),
            ..Default::default()
        };
        Self { inner }
    }

    /// Requests only "declare v3" transactions.
    pub fn only_declare_v3() -> Self {
        use transaction_filter::Inner;
        let inner = TransactionFilter {
            inner: Some(Inner::DeclareV3(DeclareV3TransactionFilter::default())),
            ..Default::default()
        };
        Self { inner }
    }

    /// Requests only "l1 handler" transactions.
    pub fn only_l1_handler() -> Self {
        use transaction_filter::Inner;
        let inner = TransactionFilter {
            inner: Some(Inner::L1Handler(L1HandlerTransactionFilter::default())),
            ..Default::default()
        };
        Self { inner }
    }

    /// Requests only "deploy account v1" transactions.
    pub fn only_deploy_account_v1() -> Self {
        use transaction_filter::Inner;
        let inner = TransactionFilter {
            inner: Some(Inner::DeployAccountV1(
                DeployAccountV1TransactionFilter::default(),
            )),
            ..Default::default()
        };
        Self { inner }
    }

    /// Requests only "deploy account v3" transactions.
    pub fn only_deploy_account_v3() -> Self {
        use transaction_filter::Inner;
        let inner = TransactionFilter {
            inner: Some(Inner::DeployAccountV3(
                DeployAccountV3TransactionFilter::default(),
            )),
            ..Default::default()
        };
        Self { inner }
    }

    /// Assign a new id to the filter.
    ///
    /// Data produced by this filter will be associated with the given id.
    pub fn with_id(mut self, id: u32) -> Self {
        self.inner.id = id;
        self
    }

    /// Requests only transactions with the given status.
    pub fn with_transaction_status(mut self, status: TransactionStatusFilter) -> Self {
        self.inner.set_transaction_status(status);
        self
    }

    /// Requests to include the transaction's receipt.
    pub fn with_receipt(mut self) -> Self {
        self.inner.include_receipt = Some(true);
        self
    }

    /// Requests to include the transaction's events.
    pub fn with_events(mut self) -> Self {
        self.inner.include_events = Some(true);
        self
    }

    /// Requests to include the transaction's messages to L1.
    pub fn with_messages(mut self) -> Self {
        self.inner.include_messages = Some(true);
        self
    }

    /// Requests to include the transaction's trace.
    pub fn with_trace(mut self) -> Self {
        self.inner.include_trace = Some(true);
        self
    }

    /// Returns the [TransactionFilter].
    pub fn build(self) -> TransactionFilter {
        self.inner
    }
}

impl EventFilterBuilder {
    /// Returns a new [EventFilterBuilder] that includes all contracts and events.
    ///
    /// This is the default when no contract address and keys are specified.
    pub fn all_contracts_and_events() -> Self {
        let inner = EventFilter::default();
        Self { inner }
    }

    /// Returns a new [EventFilterBuilder] that includes only events emitted by the given contract.
    pub fn single_contract(address: FieldElement) -> Self {
        let inner = EventFilter {
            address: Some(address),
            ..Default::default()
        };
        Self { inner }
    }

    /// Returns events emitted by the specified contract address.
    pub fn with_address(mut self, address: FieldElement) -> Self {
        self.inner.address = Some(address);
        self
    }

    /// Returns events with the specified keys.
    ///
    /// On Starknet, the first key is the hash of the event's name.
    /// All other keys are the "indexed" fields of the event.
    ///
    /// The iterator should return [Option::None] if you need _all_ values for that key.
    ///
    /// The `strict` parameter determines how the DNA server matches keys of different lengths.
    ///
    ///  - if `strict` is `false`, this filter is essentially a prefix match.
    ///  - if `strict` is `true`, this filter is an exact match.
    ///
    /// ## Example
    ///
    /// Imagine a `Transfer` event with the following indexed fields: from, to.
    ///
    ///  - `[Some(TRANSFER_HASH), Some(FROM_ADDR), Some(TO_ADDR)]` - filters all transfer events from and to the specified addresses.
    ///  - `[Some(TRANSFER_HASH), Some(FROM_ADDR), None]` - filters all transfer events from the specified address.
    ///  - `[Some(TRANSFER_HASH), None, Some(TO_ADDR)]` - filters all transfer events to the specified address.
    ///  - `[Some(TRANSFER_HASH), None, None]` - filters all transfer events.
    ///
    /// If you set `strict` to `false`, the last example can be rewritten as `[Some(TRANSFER_HASH)]`.
    pub fn with_keys(
        mut self,
        strict: bool,
        keys: impl IntoIterator<Item = Option<FieldElement>>,
    ) -> Self {
        self.inner.keys = keys.into_iter().map(|key| Key { value: key }).collect();
        self.inner.strict = Some(strict);
        self
    }

    /// Assign a new id to the filter.
    ///
    /// Data produced by this filter will be associated with the given id.
    pub fn with_id(mut self, id: u32) -> Self {
        self.inner.id = id;
        self
    }

    /// Requests only events emitted by transactions with the given status.
    pub fn with_transaction_status(mut self, status: TransactionStatusFilter) -> Self {
        self.inner.set_transaction_status(status);
        self
    }

    /// Requests to include the transaction.
    pub fn with_transaction(mut self) -> Self {
        self.inner.include_transaction = Some(true);
        self
    }

    /// Requests to include the transaction's receipt.
    pub fn with_receipt(mut self) -> Self {
        self.inner.include_receipt = Some(true);
        self
    }

    /// Requests to include the transaction's events.
    pub fn with_siblings(mut self) -> Self {
        self.inner.include_siblings = Some(true);
        self
    }

    /// Requests to include the transaction's messages to L1.
    pub fn with_messages(mut self) -> Self {
        self.inner.include_messages = Some(true);
        self
    }

    /// Requests to include the transaction's trace.
    pub fn with_transaction_trace(mut self) -> Self {
        self.inner.include_transaction_trace = Some(true);
        self
    }

    /// Returns the [EventFilter].
    pub fn build(self) -> EventFilter {
        self.inner
    }
}

impl MessageToL1FilterBuilder {
    /// Returns a [MessageToL1FilterBuilder] that includes all messages.
    pub fn all_messages() -> Self {
        let inner = MessageToL1Filter::default();
        Self { inner }
    }

    /// Requests only messages sent from the given address.
    pub fn with_from_address(mut self, from_address: FieldElement) -> Self {
        self.inner.from_address = Some(from_address);
        self
    }

    /// Requests only messages sent to the given address.
    pub fn with_to_address(mut self, to_address: FieldElement) -> Self {
        self.inner.to_address = Some(to_address);
        self
    }

    /// Assign a new id to the filter.
    ///
    /// Data produced by this filter will be associated with the given id.
    pub fn with_id(mut self, id: u32) -> Self {
        self.inner.id = id;
        self
    }

    /// Requests only events emitted by transactions with the given status.
    pub fn with_transaction_status(mut self, status: TransactionStatusFilter) -> Self {
        self.inner.set_transaction_status(status);
        self
    }

    /// Requests to include the transaction's receipt.
    pub fn with_receipt(mut self) -> Self {
        self.inner.include_receipt = Some(true);
        self
    }

    /// Requests to include the messages emitted by the same transaction.
    pub fn with_siblings(mut self) -> Self {
        self.inner.include_siblings = Some(true);
        self
    }

    /// Requests to include the transaction's events.
    pub fn with_events(mut self) -> Self {
        self.inner.include_events = Some(true);
        self
    }

    /// Requests to include the transaction's trace.
    pub fn with_transaction_trace(mut self) -> Self {
        self.inner.include_transaction_trace = Some(true);
        self
    }

    /// Returns the [MessageToL1Filter].
    pub fn build(self) -> MessageToL1Filter {
        self.inner
    }
}

impl StorageDiffFilterBuilder {
    /// Returns a [StorageDiffFilterBuilder] that includes all contracts.
    pub fn all_contracts() -> Self {
        let inner = StorageDiffFilter::default();
        Self { inner }
    }

    /// Returns a new [StorageDiffFilterBuilder] that includes only changes to the specified contract.
    pub fn single_contract(address: FieldElement) -> Self {
        let inner = StorageDiffFilter {
            contract_address: Some(address),
            ..Default::default()
        };
        Self { inner }
    }

    /// Returns storage diffs for the specified contract address.
    pub fn with_address(mut self, address: FieldElement) -> Self {
        self.inner.contract_address = Some(address);
        self
    }

    /// Assign a new id to the filter.
    ///
    /// Data produced by this filter will be associated with the given id.
    pub fn with_id(mut self, id: u32) -> Self {
        self.inner.id = id;
        self
    }

    /// Returns the [StorageDiffFilter].
    pub fn build(self) -> StorageDiffFilter {
        self.inner
    }
}

impl ContractChangeFilterBuilder {
    /// Returns a [ContractChangeFilterBuilder] that includes all changes.
    pub fn all_changes() -> Self {
        let inner = ContractChangeFilter::default();
        Self { inner }
    }

    /// Returns a [ContractChangeFilterBuilder] that includes only declared class changes.
    pub fn declared_class() -> Self {
        use contract_change_filter::Change;
        let inner = ContractChangeFilter {
            change: Some(Change::DeclaredClass(DeclaredClassFilter::default())),
            ..Default::default()
        };
        Self { inner }
    }

    /// Returns a [ContractChangeFilterBuilder] that includes only replaced class changes.
    pub fn replaced_class() -> Self {
        use contract_change_filter::Change;
        let inner = ContractChangeFilter {
            change: Some(Change::ReplacedClass(ReplacedClassFilter::default())),
            ..Default::default()
        };
        Self { inner }
    }

    /// Returns a [ContractChangeFilterBuilder] that includes only deployed contract changes.
    pub fn deployed_contract() -> Self {
        use contract_change_filter::Change;
        let inner = ContractChangeFilter {
            change: Some(Change::DeployedContract(DeployedContractFilter::default())),
            ..Default::default()
        };
        Self { inner }
    }

    /// Assign a new id to the filter.
    ///
    /// Data produced by this filter will be associated with the given id.
    pub fn with_id(mut self, id: u32) -> Self {
        self.inner.id = id;
        self
    }

    /// Returns the [ContractChangeFilter].
    pub fn build(self) -> ContractChangeFilter {
        self.inner
    }
}

impl NonceUpdateFilterBuilder {
    /// Returns a [NonceUpdateFilterBuilder] that includes all contracts.
    pub fn all_contracts() -> Self {
        let inner = NonceUpdateFilter::default();
        Self { inner }
    }

    /// Returns a new [NonceUpdateFilterBuilder] that includes only changes to the specified contract.
    pub fn single_contract(address: FieldElement) -> Self {
        let inner = NonceUpdateFilter {
            contract_address: Some(address),
            ..Default::default()
        };
        Self { inner }
    }

    /// Returns nonce updates for the specified contract address.
    pub fn with_address(mut self, address: FieldElement) -> Self {
        self.inner.contract_address = Some(address);
        self
    }

    /// Assign a new id to the filter.
    ///
    /// Data produced by this filter will be associated with the given id.
    pub fn with_id(mut self, id: u32) -> Self {
        self.inner.id = id;
        self
    }

    /// Returns the [NonceUpdateFilter].
    pub fn build(self) -> NonceUpdateFilter {
        self.inner
    }
}
