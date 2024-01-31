// automatically generated by the FlatBuffers compiler, do not modify
// @generated
extern crate alloc;
extern crate flatbuffers;
use self::flatbuffers::{EndianScalar, Follow};
use super::*;
use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cmp::Ordering;
use core::mem;
pub enum TransactionOffset {}
#[derive(Copy, Clone, PartialEq)]

pub struct Transaction<'a> {
    pub _tab: flatbuffers::Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for Transaction<'a> {
    type Inner = Transaction<'a>;
    #[inline]
    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: flatbuffers::Table::new(buf, loc),
        }
    }
}

impl<'a> Transaction<'a> {
    pub const VT_HASH: flatbuffers::VOffsetT = 4;
    pub const VT_NONCE: flatbuffers::VOffsetT = 6;
    pub const VT_TRANSACTION_INDEX: flatbuffers::VOffsetT = 8;
    pub const VT_FROM: flatbuffers::VOffsetT = 10;
    pub const VT_TO: flatbuffers::VOffsetT = 12;
    pub const VT_VALUE: flatbuffers::VOffsetT = 14;
    pub const VT_GAS_PRICE: flatbuffers::VOffsetT = 16;
    pub const VT_GAS: flatbuffers::VOffsetT = 18;
    pub const VT_MAX_FEE_PER_GAS: flatbuffers::VOffsetT = 20;
    pub const VT_MAX_PRIORITY_FEE_PER_GAS: flatbuffers::VOffsetT = 22;
    pub const VT_INPUT: flatbuffers::VOffsetT = 24;
    pub const VT_SIGNATURE: flatbuffers::VOffsetT = 26;
    pub const VT_CHAIN_ID: flatbuffers::VOffsetT = 28;
    pub const VT_ACCESS_LIST: flatbuffers::VOffsetT = 30;
    pub const VT_TRANSACTION_TYPE: flatbuffers::VOffsetT = 32;

    pub const fn get_fully_qualified_name() -> &'static str {
        "Transaction"
    }

    #[inline]
    pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
        Transaction { _tab: table }
    }
    #[allow(unused_mut)]
    pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
        _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
        args: &'args TransactionArgs<'args>,
    ) -> flatbuffers::WIPOffset<Transaction<'bldr>> {
        let mut builder = TransactionBuilder::new(_fbb);
        builder.add_chain_id(args.chain_id);
        builder.add_transaction_index(args.transaction_index);
        builder.add_nonce(args.nonce);
        builder.add_transaction_type(args.transaction_type);
        if let Some(x) = args.access_list {
            builder.add_access_list(x);
        }
        if let Some(x) = args.signature {
            builder.add_signature(x);
        }
        if let Some(x) = args.input {
            builder.add_input(x);
        }
        if let Some(x) = args.max_priority_fee_per_gas {
            builder.add_max_priority_fee_per_gas(x);
        }
        if let Some(x) = args.max_fee_per_gas {
            builder.add_max_fee_per_gas(x);
        }
        if let Some(x) = args.gas {
            builder.add_gas(x);
        }
        if let Some(x) = args.gas_price {
            builder.add_gas_price(x);
        }
        if let Some(x) = args.value {
            builder.add_value(x);
        }
        if let Some(x) = args.to {
            builder.add_to(x);
        }
        if let Some(x) = args.from {
            builder.add_from(x);
        }
        if let Some(x) = args.hash {
            builder.add_hash(x);
        }
        builder.finish()
    }

    #[inline]
    pub fn hash(&self) -> Option<&'a B256> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe { self._tab.get::<B256>(Transaction::VT_HASH, None) }
    }
    #[inline]
    pub fn nonce(&self) -> u64 {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<u64>(Transaction::VT_NONCE, Some(0))
                .unwrap()
        }
    }
    #[inline]
    pub fn transaction_index(&self) -> u64 {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<u64>(Transaction::VT_TRANSACTION_INDEX, Some(0))
                .unwrap()
        }
    }
    #[inline]
    pub fn from(&self) -> Option<&'a Address> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe { self._tab.get::<Address>(Transaction::VT_FROM, None) }
    }
    #[inline]
    pub fn to(&self) -> Option<&'a Address> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe { self._tab.get::<Address>(Transaction::VT_TO, None) }
    }
    #[inline]
    pub fn value(&self) -> Option<&'a U256> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe { self._tab.get::<U256>(Transaction::VT_VALUE, None) }
    }
    #[inline]
    pub fn gas_price(&self) -> Option<&'a U128> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe { self._tab.get::<U128>(Transaction::VT_GAS_PRICE, None) }
    }
    #[inline]
    pub fn gas(&self) -> Option<&'a U256> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe { self._tab.get::<U256>(Transaction::VT_GAS, None) }
    }
    #[inline]
    pub fn max_fee_per_gas(&self) -> Option<&'a U128> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe { self._tab.get::<U128>(Transaction::VT_MAX_FEE_PER_GAS, None) }
    }
    #[inline]
    pub fn max_priority_fee_per_gas(&self) -> Option<&'a U128> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<U128>(Transaction::VT_MAX_PRIORITY_FEE_PER_GAS, None)
        }
    }
    #[inline]
    pub fn input(&self) -> Option<flatbuffers::Vector<'a, u8>> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(
                    Transaction::VT_INPUT,
                    None,
                )
        }
    }
    #[inline]
    pub fn signature(&self) -> Option<Signature<'a>> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<flatbuffers::ForwardsUOffset<Signature>>(Transaction::VT_SIGNATURE, None)
        }
    }
    #[inline]
    pub fn chain_id(&self) -> u64 {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<u64>(Transaction::VT_CHAIN_ID, Some(0))
                .unwrap()
        }
    }
    #[inline]
    pub fn access_list(
        &self,
    ) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<AccessListItem<'a>>>> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab.get::<flatbuffers::ForwardsUOffset<
                flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<AccessListItem>>,
            >>(Transaction::VT_ACCESS_LIST, None)
        }
    }
    #[inline]
    pub fn transaction_type(&self) -> u32 {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<u32>(Transaction::VT_TRANSACTION_TYPE, Some(0))
                .unwrap()
        }
    }
}

impl flatbuffers::Verifiable for Transaction<'_> {
    #[inline]
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        use self::flatbuffers::Verifiable;
        v.visit_table(pos)?
            .visit_field::<B256>("hash", Self::VT_HASH, false)?
            .visit_field::<u64>("nonce", Self::VT_NONCE, false)?
            .visit_field::<u64>("transaction_index", Self::VT_TRANSACTION_INDEX, false)?
            .visit_field::<Address>("from", Self::VT_FROM, false)?
            .visit_field::<Address>("to", Self::VT_TO, false)?
            .visit_field::<U256>("value", Self::VT_VALUE, false)?
            .visit_field::<U128>("gas_price", Self::VT_GAS_PRICE, false)?
            .visit_field::<U256>("gas", Self::VT_GAS, false)?
            .visit_field::<U128>("max_fee_per_gas", Self::VT_MAX_FEE_PER_GAS, false)?
            .visit_field::<U128>(
                "max_priority_fee_per_gas",
                Self::VT_MAX_PRIORITY_FEE_PER_GAS,
                false,
            )?
            .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(
                "input",
                Self::VT_INPUT,
                false,
            )?
            .visit_field::<flatbuffers::ForwardsUOffset<Signature>>(
                "signature",
                Self::VT_SIGNATURE,
                false,
            )?
            .visit_field::<u64>("chain_id", Self::VT_CHAIN_ID, false)?
            .visit_field::<flatbuffers::ForwardsUOffset<
                flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<AccessListItem>>,
            >>("access_list", Self::VT_ACCESS_LIST, false)?
            .visit_field::<u32>("transaction_type", Self::VT_TRANSACTION_TYPE, false)?
            .finish();
        Ok(())
    }
}
pub struct TransactionArgs<'a> {
    pub hash: Option<&'a B256>,
    pub nonce: u64,
    pub transaction_index: u64,
    pub from: Option<&'a Address>,
    pub to: Option<&'a Address>,
    pub value: Option<&'a U256>,
    pub gas_price: Option<&'a U128>,
    pub gas: Option<&'a U256>,
    pub max_fee_per_gas: Option<&'a U128>,
    pub max_priority_fee_per_gas: Option<&'a U128>,
    pub input: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
    pub signature: Option<flatbuffers::WIPOffset<Signature<'a>>>,
    pub chain_id: u64,
    pub access_list: Option<
        flatbuffers::WIPOffset<
            flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<AccessListItem<'a>>>,
        >,
    >,
    pub transaction_type: u32,
}
impl<'a> Default for TransactionArgs<'a> {
    #[inline]
    fn default() -> Self {
        TransactionArgs {
            hash: None,
            nonce: 0,
            transaction_index: 0,
            from: None,
            to: None,
            value: None,
            gas_price: None,
            gas: None,
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            input: None,
            signature: None,
            chain_id: 0,
            access_list: None,
            transaction_type: 0,
        }
    }
}

pub struct TransactionBuilder<'a: 'b, 'b> {
    fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
    start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}
impl<'a: 'b, 'b> TransactionBuilder<'a, 'b> {
    #[inline]
    pub fn add_hash(&mut self, hash: &B256) {
        self.fbb_
            .push_slot_always::<&B256>(Transaction::VT_HASH, hash);
    }
    #[inline]
    pub fn add_nonce(&mut self, nonce: u64) {
        self.fbb_.push_slot::<u64>(Transaction::VT_NONCE, nonce, 0);
    }
    #[inline]
    pub fn add_transaction_index(&mut self, transaction_index: u64) {
        self.fbb_
            .push_slot::<u64>(Transaction::VT_TRANSACTION_INDEX, transaction_index, 0);
    }
    #[inline]
    pub fn add_from(&mut self, from: &Address) {
        self.fbb_
            .push_slot_always::<&Address>(Transaction::VT_FROM, from);
    }
    #[inline]
    pub fn add_to(&mut self, to: &Address) {
        self.fbb_
            .push_slot_always::<&Address>(Transaction::VT_TO, to);
    }
    #[inline]
    pub fn add_value(&mut self, value: &U256) {
        self.fbb_
            .push_slot_always::<&U256>(Transaction::VT_VALUE, value);
    }
    #[inline]
    pub fn add_gas_price(&mut self, gas_price: &U128) {
        self.fbb_
            .push_slot_always::<&U128>(Transaction::VT_GAS_PRICE, gas_price);
    }
    #[inline]
    pub fn add_gas(&mut self, gas: &U256) {
        self.fbb_
            .push_slot_always::<&U256>(Transaction::VT_GAS, gas);
    }
    #[inline]
    pub fn add_max_fee_per_gas(&mut self, max_fee_per_gas: &U128) {
        self.fbb_
            .push_slot_always::<&U128>(Transaction::VT_MAX_FEE_PER_GAS, max_fee_per_gas);
    }
    #[inline]
    pub fn add_max_priority_fee_per_gas(&mut self, max_priority_fee_per_gas: &U128) {
        self.fbb_.push_slot_always::<&U128>(
            Transaction::VT_MAX_PRIORITY_FEE_PER_GAS,
            max_priority_fee_per_gas,
        );
    }
    #[inline]
    pub fn add_input(&mut self, input: flatbuffers::WIPOffset<flatbuffers::Vector<'b, u8>>) {
        self.fbb_
            .push_slot_always::<flatbuffers::WIPOffset<_>>(Transaction::VT_INPUT, input);
    }
    #[inline]
    pub fn add_signature(&mut self, signature: flatbuffers::WIPOffset<Signature<'b>>) {
        self.fbb_
            .push_slot_always::<flatbuffers::WIPOffset<Signature>>(
                Transaction::VT_SIGNATURE,
                signature,
            );
    }
    #[inline]
    pub fn add_chain_id(&mut self, chain_id: u64) {
        self.fbb_
            .push_slot::<u64>(Transaction::VT_CHAIN_ID, chain_id, 0);
    }
    #[inline]
    pub fn add_access_list(
        &mut self,
        access_list: flatbuffers::WIPOffset<
            flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<AccessListItem<'b>>>,
        >,
    ) {
        self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(
            Transaction::VT_ACCESS_LIST,
            access_list,
        );
    }
    #[inline]
    pub fn add_transaction_type(&mut self, transaction_type: u32) {
        self.fbb_
            .push_slot::<u32>(Transaction::VT_TRANSACTION_TYPE, transaction_type, 0);
    }
    #[inline]
    pub fn new(_fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>) -> TransactionBuilder<'a, 'b> {
        let start = _fbb.start_table();
        TransactionBuilder {
            fbb_: _fbb,
            start_: start,
        }
    }
    #[inline]
    pub fn finish(self) -> flatbuffers::WIPOffset<Transaction<'a>> {
        let o = self.fbb_.end_table(self.start_);
        flatbuffers::WIPOffset::new(o.value())
    }
}

impl core::fmt::Debug for Transaction<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let mut ds = f.debug_struct("Transaction");
        ds.field("hash", &self.hash());
        ds.field("nonce", &self.nonce());
        ds.field("transaction_index", &self.transaction_index());
        ds.field("from", &self.from());
        ds.field("to", &self.to());
        ds.field("value", &self.value());
        ds.field("gas_price", &self.gas_price());
        ds.field("gas", &self.gas());
        ds.field("max_fee_per_gas", &self.max_fee_per_gas());
        ds.field("max_priority_fee_per_gas", &self.max_priority_fee_per_gas());
        ds.field("input", &self.input());
        ds.field("signature", &self.signature());
        ds.field("chain_id", &self.chain_id());
        ds.field("access_list", &self.access_list());
        ds.field("transaction_type", &self.transaction_type());
        ds.finish()
    }
}