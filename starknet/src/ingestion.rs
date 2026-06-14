use std::{sync::Arc, time::Duration};

use apibara_dna_common::{
    chain::{BlockInfo, PendingBlockInfo},
    fragment::{
        Block, BodyFragment, HeaderFragment, Index, IndexFragment, IndexGroupFragment, Join,
        JoinFragment, JoinGroupFragment,
    },
    index::{BitmapIndexBuilder, ScalarValue},
    ingestion::{
        BlockIngestion, BoxedNewHeadsStream, BoxedPendingBlockUpdateStream, IngestionError,
    },
    join::{JoinToManyIndexBuilder, JoinToOneIndexBuilder},
    Cursor, Hash,
};
use apibara_dna_protocol::starknet;
use error_stack::{Report, Result, ResultExt};
// use futures::StreamExt;
use prost::Message;
use tokio::sync::Mutex;
use tokio_stream::StreamExt as TokioStreamExt;
use tracing::{info, Instrument};

use crate::{
    filter::{ContractChangeType, TransactionType},
    fragment::{
        CONTRACT_CHANGE_FRAGMENT_ID, CONTRACT_CHANGE_FRAGMENT_NAME, EVENT_FRAGMENT_ID,
        EVENT_FRAGMENT_NAME, INDEX_CONTRACT_CHANGE_BY_TYPE, INDEX_EVENT_BY_ADDRESS,
        INDEX_EVENT_BY_KEY0, INDEX_EVENT_BY_KEY1, INDEX_EVENT_BY_KEY2, INDEX_EVENT_BY_KEY3,
        INDEX_EVENT_BY_KEY_LENGTH, INDEX_EVENT_BY_TRANSACTION_STATUS,
        INDEX_MESSAGE_BY_FROM_ADDRESS, INDEX_MESSAGE_BY_TO_ADDRESS,
        INDEX_MESSAGE_BY_TRANSACTION_STATUS, INDEX_NONCE_UPDATE_BY_CONTRACT_ADDRESS,
        INDEX_STORAGE_DIFF_BY_CONTRACT_ADDRESS, INDEX_TRANSACTION_BY_STATUS,
        INDEX_TRANSACTION_BY_TYPE, MESSAGE_FRAGMENT_ID, MESSAGE_FRAGMENT_NAME,
        NONCE_UPDATE_FRAGMENT_ID, NONCE_UPDATE_FRAGMENT_NAME, RECEIPT_FRAGMENT_ID,
        RECEIPT_FRAGMENT_NAME, STORAGE_DIFF_FRAGMENT_ID, STORAGE_DIFF_FRAGMENT_NAME,
        TRACE_FRAGMENT_ID, TRACE_FRAGMENT_NAME, TRANSACTION_FRAGMENT_ID, TRANSACTION_FRAGMENT_NAME,
    },
    live::{LiveAssemblerInsert, StarknetLiveAssembler},
    proto::{convert_block_header, convert_pre_confirmed_block_header, ModelExt},
    provider::{
        models, BlockExt, BlockId, StarknetLiveTransactionsStream, StarknetProvider,
        StarknetProviderError, StarknetProviderErrorExt,
    },
    NewHeadsStream,
};

#[derive(Clone, Debug)]
pub struct StarknetBlockIngestionOptions {
    pub ingest_pending: bool,
    pub ingest_traces: bool,
    pub live_ingestion_enabled: bool,
}

pub struct StarknetBlockIngestion {
    provider: StarknetProvider,
    ws_url: Option<String>,
    options: StarknetBlockIngestionOptions,
    live_assembler: Arc<Mutex<StarknetLiveAssembler>>,
}

impl StarknetBlockIngestion {
    pub fn new(
        provider: StarknetProvider,
        ws_url: Option<String>,
        options: StarknetBlockIngestionOptions,
    ) -> Self {
        Self {
            provider,
            ws_url,
            options,
            live_assembler: Arc::new(Mutex::new(StarknetLiveAssembler::new())),
        }
    }
}

pub(crate) struct BlockIngestionResult {
    pub(crate) body: Vec<BodyFragment>,
    pub(crate) index: Vec<IndexFragment>,
    pub(crate) join: Vec<JoinFragment>,
}

impl BlockIngestion for StarknetBlockIngestion {
    fn supports_pending(&self) -> bool {
        self.options.ingest_pending
    }

    #[tracing::instrument("starknet_new_heads_stream", skip_all, err(Debug))]
    async fn new_heads_stream(&self) -> Result<BoxedNewHeadsStream, IngestionError> {
        let Some(url) = self.ws_url.as_ref() else {
            use futures::StreamExt;
            return Ok(futures::stream::pending().boxed());
        };

        info!("subscribing to new starknet heads");

        let stream = NewHeadsStream::connect(url)
            .await
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to connect to starknet ws")?
            .timeout(Duration::from_secs(60))
            .map(|message| match message {
                Ok(Ok(head)) => Ok(head.cursor()),
                Ok(Err(err)) => Err(err).change_context(IngestionError::RpcRequest),
                Err(err) => Err(err)
                    .change_context(StarknetProviderError::Timeout)
                    .change_context(IngestionError::RpcRequest),
            });

        {
            use futures::StreamExt;
            Ok(stream.boxed())
        }
    }

    #[tracing::instrument("starknet_pending_block_updates_stream", skip_all, err(Debug))]
    async fn pending_block_updates_stream(
        &self,
    ) -> Result<BoxedPendingBlockUpdateStream, IngestionError> {
        if !self.options.ingest_pending || !self.options.live_ingestion_enabled {
            use futures::StreamExt;
            return Ok(futures::stream::pending().boxed());
        }

        let Some(url) = self.ws_url.as_ref() else {
            use futures::StreamExt;
            return Ok(futures::stream::pending().boxed());
        };

        info!("subscribing to live starknet transaction receipts");

        let assembler = self.live_assembler.clone();
        let stream = StarknetLiveTransactionsStream::connect(url)
            .await
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to connect to starknet live ws")?
            .timeout(Duration::from_secs(60));

        {
            use futures::StreamExt;

            let stream = futures::StreamExt::then(stream, move |message| {
                let assembler = assembler.clone();
                async move {
                    match message {
                        Ok(Ok(message)) => {
                            let mut assembler = assembler.lock().await;
                            let insert = assembler.push_message(message);
                            if insert == LiveAssemblerInsert::Duplicate
                                || assembler.pending_block_numbers().is_empty()
                            {
                                None
                            } else {
                                Some(Ok(()))
                            }
                        }
                        Ok(Err(err)) => Some(Err(err).change_context(IngestionError::RpcRequest)),
                        Err(err) => Some(
                            Err(err)
                                .change_context(StarknetProviderError::Timeout)
                                .change_context(IngestionError::RpcRequest),
                        ),
                    }
                }
            });

            Ok(futures::StreamExt::filter_map(stream, |message| async move { message }).boxed())
        }
    }

    #[tracing::instrument("starknet_get_head_cursor", skip_all, err(Debug))]
    async fn get_head_cursor(&self) -> Result<Cursor, IngestionError> {
        let cursor = self
            .provider
            .get_block_with_tx_hashes(&BlockId::Head)
            .await
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to get head block")?
            .cursor()
            .ok_or(IngestionError::RpcRequest)
            .attach_printable("missing head block cursor")?;

        Ok(cursor)
    }

    #[tracing::instrument("starknet_get_finalized_cursor", skip_all, err(Debug))]
    async fn get_finalized_cursor(&self) -> Result<Cursor, IngestionError> {
        let cursor = self
            .provider
            .get_block_with_tx_hashes(&BlockId::Finalized)
            .await
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to get finalized block")?
            .cursor()
            .ok_or(IngestionError::RpcRequest)
            .attach_printable("missing finalized block cursor")?;

        Ok(cursor)
    }

    #[tracing::instrument("starknet_get_block_info_by_number", skip(self), err(Debug))]
    async fn get_block_info_by_number(
        &self,
        block_number: u64,
    ) -> Result<BlockInfo, IngestionError> {
        let block = match self
            .provider
            .get_block_with_tx_hashes(&BlockId::Number(block_number))
            .await
        {
            Ok(block) => block,
            Err(err) if err.is_not_found() => {
                return Err(err).change_context(IngestionError::BlockNotFound)
            }
            Err(err) => {
                return Err(err)
                    .change_context(IngestionError::RpcRequest)
                    .attach_printable("failed to get block by number")
                    .attach_printable_lazy(|| format!("block number: {}", block_number))
            }
        };

        let models::MaybePreConfirmedBlockWithTxHashes::Block(block) = block else {
            return Err(IngestionError::RpcRequest)
                .attach_printable("unexpected pending block")
                .attach_printable_lazy(|| format!("block number: {}", block_number));
        };

        let hash = block.block_hash.to_bytes_be().to_vec();
        let parent = block.parent_hash.to_bytes_be().to_vec();
        let number = block.block_number;

        Ok(BlockInfo {
            number,
            hash: Hash(hash),
            parent: Hash(parent),
        })
    }

    #[tracing::instrument("starknet_ingest_pending_block", skip(self), err(Debug))]
    async fn ingest_pending_block(
        &self,
        parent: &Cursor,
        generation: u64,
    ) -> Result<Option<(PendingBlockInfo, Block)>, IngestionError> {
        if self.options.live_ingestion_enabled {
            let number = parent.number + 1;
            let live_block = {
                let assembler = self.live_assembler.lock().await;
                assembler.build_pending_block(number)?
            };

            if let Some(live_block) = live_block {
                let block_info = PendingBlockInfo {
                    number: live_block.block_number,
                    generation,
                    parent: parent.hash.clone(),
                };

                return Ok(Some((block_info, live_block.block)));
            }
        }

        let block_id = BlockId::Pending;

        let block = match self.provider.get_block_with_receipts(&block_id).await {
            Ok(block) => block,
            Err(err) if err.is_not_found() => return Ok(None),
            Err(err) => {
                return Err(err)
                    .change_context(IngestionError::RpcRequest)
                    .attach_printable("failed to get pre-confirmed block")
            }
        };

        let models::MaybePreConfirmedBlockWithReceipts::PreConfirmedBlock(block) = block else {
            return Err(IngestionError::RpcRequest).attach_printable("unexpected accepted block");
        };

        if block.block_number != parent.number + 1 {
            return Ok(None);
        }

        let state_update = tokio::spawn({
            let provider = self.provider.clone();
            let block_id = block_id.clone();
            async move {
                provider
                    .get_state_update(&block_id)
                    .await
                    .change_context(IngestionError::RpcRequest)
            }
            .in_current_span()
        });

        let transaction_traces = tokio::spawn({
            let provider = self.provider.clone();
            let block_id = block_id.clone();
            let should_ingest = self.options.ingest_traces;
            async move {
                if should_ingest {
                    provider
                        .get_block_transaction_traces(&block_id)
                        .await
                        .change_context(IngestionError::RpcRequest)
                } else {
                    Ok(Vec::default())
                }
            }
            .in_current_span()
        });

        let models::MaybePreConfirmedStateUpdate::PreConfirmedUpdate(state_update) = state_update
            .await
            .change_context(IngestionError::RpcRequest)??
        else {
            return Err(IngestionError::RpcRequest)
                .attach_printable("unexpected accepted state update");
        };

        let transaction_traces = transaction_traces
            .await
            .change_context(IngestionError::RpcRequest)??;

        let number = block.block_number;

        let block_info = PendingBlockInfo {
            number,
            generation,
            parent: parent.hash.clone(),
        };

        let block = tracing::info_span!("index_block").in_scope(|| {
            let header_fragment = {
                let header = convert_pre_confirmed_block_header(&block);
                HeaderFragment {
                    data: header.encode_to_vec(),
                }
            };

            let body_ingestion_result =
                collect_block_body_and_index(&block.transactions, &transaction_traces)?;

            let state_update_ingestion_result =
                collect_state_update_body_and_index(&state_update.state_diff)?;

            let mut body_fragments = body_ingestion_result.body;
            let mut index_fragments = body_ingestion_result.index;
            let mut join_fragments = body_ingestion_result.join;

            body_fragments.extend(state_update_ingestion_result.body);
            index_fragments.extend(state_update_ingestion_result.index);
            join_fragments.extend(state_update_ingestion_result.join);

            let index_group = IndexGroupFragment {
                indexes: index_fragments,
            };

            let join_group = JoinGroupFragment {
                joins: join_fragments,
            };

            Ok::<_, Report<IngestionError>>(Block {
                header: header_fragment,
                index: index_group,
                body: body_fragments,
                join: join_group,
            })
        })?;

        Ok((block_info, block).into())
    }

    #[tracing::instrument("starknet_ingest_block_by_number", skip(self), err(Debug))]
    async fn ingest_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<(BlockInfo, Block), IngestionError> {
        let block = match self
            .provider
            .get_block_with_receipts(&BlockId::Number(block_number))
            .await
        {
            Ok(block) => block,
            Err(err) if err.is_not_found() => {
                return Err(err).change_context(IngestionError::BlockNotFound)
            }
            Err(err) => {
                return Err(err)
                    .change_context(IngestionError::RpcRequest)
                    .attach_printable("failed to get block by number")
                    .attach_printable_lazy(|| format!("block number: {}", block_number))
            }
        };

        let models::MaybePreConfirmedBlockWithReceipts::Block(block) = block else {
            return Err(IngestionError::RpcRequest)
                .attach_printable("unexpected pending block")
                .attach_printable_lazy(|| format!("block number: {}", block_number));
        };

        // Use the block hash to avoid issues with reorgs.
        let block_id = BlockId::Hash(block.block_hash);

        let state_update = tokio::spawn({
            let provider = self.provider.clone();
            let block_id = block_id.clone();
            async move {
                provider
                    .get_state_update(&block_id)
                    .await
                    .change_context(IngestionError::RpcRequest)
            }
            .in_current_span()
        });

        let transaction_traces = tokio::spawn({
            let provider = self.provider.clone();
            let block_id = block_id.clone();
            let should_ingest = self.options.ingest_traces;
            async move {
                if should_ingest {
                    provider
                        .get_block_transaction_traces(&block_id)
                        .await
                        .change_context(IngestionError::RpcRequest)
                } else {
                    Ok(Vec::default())
                }
            }
            .in_current_span()
        });

        let models::MaybePreConfirmedStateUpdate::Update(state_update) = state_update
            .await
            .change_context(IngestionError::RpcRequest)??
        else {
            return Err(IngestionError::RpcRequest)
                .attach_printable("unexpected pending state update")
                .attach_printable_lazy(|| format!("block number: {}", block_number));
        };

        let transaction_traces = transaction_traces
            .await
            .change_context(IngestionError::RpcRequest)??;

        let hash = block.block_hash.to_bytes_be().to_vec();
        let parent = block.parent_hash.to_bytes_be().to_vec();
        let number = block.block_number;

        let block_info = BlockInfo {
            number,
            hash: Hash(hash),
            parent: Hash(parent),
        };
        let block = tracing::info_span!("index_block").in_scope(|| {
            let header_fragment = {
                let header = convert_block_header(&block);
                HeaderFragment {
                    data: header.encode_to_vec(),
                }
            };

            let body_ingestion_result =
                collect_block_body_and_index(&block.transactions, &transaction_traces)?;

            let state_update_ingestion_result =
                collect_state_update_body_and_index(&state_update.state_diff)?;

            let mut body_fragments = body_ingestion_result.body;
            let mut index_fragments = body_ingestion_result.index;
            let mut join_fragments = body_ingestion_result.join;

            body_fragments.extend(state_update_ingestion_result.body);
            index_fragments.extend(state_update_ingestion_result.index);
            join_fragments.extend(state_update_ingestion_result.join);

            let index_group = IndexGroupFragment {
                indexes: index_fragments,
            };

            let join_group = JoinGroupFragment {
                joins: join_fragments,
            };

            Ok::<_, Report<IngestionError>>(Block {
                header: header_fragment,
                index: index_group,
                body: body_fragments,
                join: join_group,
            })
        })?;

        if self.options.live_ingestion_enabled {
            self.live_assembler.lock().await.prune_through_block(number);
        }

        Ok((block_info, block))
    }
}

impl Clone for StarknetBlockIngestion {
    fn clone(&self) -> Self {
        Self {
            provider: self.provider.clone(),
            ws_url: self.ws_url.clone(),
            options: self.options.clone(),
            live_assembler: self.live_assembler.clone(),
        }
    }
}

pub(crate) fn collect_block_body_and_index(
    transactions: &[models::TransactionWithReceipt],
    transaction_traces: &[models::TransactionTraceWithHash],
) -> Result<BlockIngestionResult, IngestionError> {
    let transaction_contents = transactions
        .iter()
        .map(|transaction| &transaction.transaction)
        .collect::<Vec<_>>();
    let receipts = transactions
        .iter()
        .map(|transaction| &transaction.receipt)
        .collect::<Vec<_>>();

    collect_block_records_body_and_index(&receipts, Some(&transaction_contents), transaction_traces)
}

pub(crate) fn collect_receipts_body_and_index(
    receipts: &[models::TransactionReceipt],
) -> Result<BlockIngestionResult, IngestionError> {
    let receipts = receipts.iter().collect::<Vec<_>>();

    collect_block_records_body_and_index(&receipts, None, &[])
}

fn collect_block_records_body_and_index(
    receipts: &[&models::TransactionReceipt],
    transactions: Option<&[&models::TransactionContent]>,
    transaction_traces: &[models::TransactionTraceWithHash],
) -> Result<BlockIngestionResult, IngestionError> {
    if let Some(transactions) = transactions {
        debug_assert_eq!(receipts.len(), transactions.len());
    }

    let mut block_transactions = Vec::new();
    let mut block_receipts = Vec::new();
    let mut block_events = Vec::new();
    let mut block_messages = Vec::new();
    let mut block_traces = Vec::new();

    let mut index_transaction_by_status = BitmapIndexBuilder::default();
    let mut index_transaction_by_type = BitmapIndexBuilder::default();
    let mut join_transaction_to_receipt = JoinToOneIndexBuilder::default();
    let mut join_transaction_to_events = JoinToManyIndexBuilder::default();
    let mut join_transaction_to_messages = JoinToManyIndexBuilder::default();
    let mut join_transaction_to_trace = JoinToOneIndexBuilder::default();

    let mut index_event_by_address = BitmapIndexBuilder::default();
    let mut index_event_by_key0 = BitmapIndexBuilder::default();
    let mut index_event_by_key1 = BitmapIndexBuilder::default();
    let mut index_event_by_key2 = BitmapIndexBuilder::default();
    let mut index_event_by_key3 = BitmapIndexBuilder::default();
    let mut index_event_by_key_length = BitmapIndexBuilder::default();
    let mut index_event_by_transaction_status = BitmapIndexBuilder::default();
    let mut join_event_to_transaction = JoinToOneIndexBuilder::default();
    let mut join_event_to_receipt = JoinToOneIndexBuilder::default();
    let mut join_event_to_siblings = JoinToManyIndexBuilder::default();
    let mut join_event_to_messages = JoinToManyIndexBuilder::default();
    let mut join_event_to_trace = JoinToOneIndexBuilder::default();

    let mut index_message_by_from_address = BitmapIndexBuilder::default();
    let mut index_message_by_to_address = BitmapIndexBuilder::default();
    let mut index_message_by_transaction_status = BitmapIndexBuilder::default();
    let mut join_message_to_transaction = JoinToOneIndexBuilder::default();
    let mut join_message_to_receipt = JoinToOneIndexBuilder::default();
    let mut join_message_to_events = JoinToManyIndexBuilder::default();
    let mut join_message_to_siblings = JoinToManyIndexBuilder::default();
    let mut join_message_to_trace = JoinToOneIndexBuilder::default();

    for transaction_trace in transaction_traces.iter() {
        let transaction_index = block_traces.len() as u32;
        let mut trace = transaction_trace.to_proto();

        trace.transaction_index = transaction_index;

        join_transaction_to_trace.insert(transaction_index, transaction_index);

        block_traces.push(trace);
    }

    for (transaction_index, receipt) in receipts.iter().enumerate() {
        let transaction_index = transaction_index as u32;
        let transaction_hash = receipt.transaction_hash().to_proto();

        let transaction_status = match receipt.execution_result() {
            models::ExecutionResult::Succeeded => starknet::TransactionStatus::Succeeded,
            models::ExecutionResult::Reverted { .. } => starknet::TransactionStatus::Reverted,
        };

        let events = match receipt {
            models::TransactionReceipt::Invoke(rx) => &rx.events,
            models::TransactionReceipt::L1Handler(rx) => &rx.events,
            models::TransactionReceipt::Declare(rx) => &rx.events,
            models::TransactionReceipt::Deploy(rx) => &rx.events,
            models::TransactionReceipt::DeployAccount(rx) => &rx.events,
        };

        let mut transaction_events_id = Vec::new();
        let mut transaction_messages_id = Vec::new();

        for (event_index_in_transaction, event) in events.iter().enumerate() {
            let mut event = event.to_proto();

            event.event_index = block_events.len() as u32;
            event.transaction_index = transaction_index;
            event.transaction_hash = transaction_hash.into();
            event.transaction_status = transaction_status as i32;
            event.event_index_in_transaction = event_index_in_transaction as u32;

            if transactions.is_some() {
                join_transaction_to_events.insert(transaction_index, event.event_index);
                join_event_to_transaction.insert(event.event_index, transaction_index);
            }
            join_event_to_receipt.insert(event.event_index, transaction_index);
            if !block_traces.is_empty() {
                join_event_to_trace.insert(event.event_index, transaction_index);
            }

            transaction_events_id.push(event.event_index);

            if let Some(address) = event.from_address {
                index_event_by_address
                    .insert(ScalarValue::B256(address.to_bytes()), event.event_index);
            }

            let mut keys = event.keys.iter();

            if let Some(key) = keys.next() {
                index_event_by_key0.insert(ScalarValue::B256(key.to_bytes()), event.event_index);
            }
            if let Some(key) = keys.next() {
                index_event_by_key1.insert(ScalarValue::B256(key.to_bytes()), event.event_index);
            }
            if let Some(key) = keys.next() {
                index_event_by_key2.insert(ScalarValue::B256(key.to_bytes()), event.event_index);
            }
            if let Some(key) = keys.next() {
                index_event_by_key3.insert(ScalarValue::B256(key.to_bytes()), event.event_index);
            }

            index_event_by_key_length.insert(
                ScalarValue::Uint32(event.keys.len() as u32),
                event.event_index,
            );

            index_event_by_transaction_status.insert(
                ScalarValue::Int32(transaction_status as i32),
                event.event_index,
            );

            block_events.push(event);
        }

        let messages = match receipt {
            models::TransactionReceipt::Invoke(rx) => &rx.messages_sent,
            models::TransactionReceipt::L1Handler(rx) => &rx.messages_sent,
            models::TransactionReceipt::Declare(rx) => &rx.messages_sent,
            models::TransactionReceipt::Deploy(rx) => &rx.messages_sent,
            models::TransactionReceipt::DeployAccount(rx) => &rx.messages_sent,
        };

        for (message_index_in_transaction, message) in messages.iter().enumerate() {
            let mut message = message.to_proto();

            message.message_index = block_messages.len() as u32;
            message.transaction_index = transaction_index;
            message.transaction_hash = transaction_hash.into();
            message.transaction_status = transaction_status as i32;
            message.message_index_in_transaction = message_index_in_transaction as u32;

            if transactions.is_some() {
                join_transaction_to_messages.insert(transaction_index, message.message_index);
                join_message_to_transaction.insert(message.message_index, transaction_index);
            }
            join_message_to_receipt.insert(message.message_index, transaction_index);
            if !block_traces.is_empty() {
                join_message_to_trace.insert(message.message_index, transaction_index);
            }

            transaction_messages_id.push(message.message_index);

            if let Some(address) = message.from_address {
                index_message_by_from_address
                    .insert(ScalarValue::B256(address.to_bytes()), message.message_index);
            }

            if let Some(address) = message.to_address {
                index_message_by_to_address
                    .insert(ScalarValue::B256(address.to_bytes()), message.message_index);
            }

            index_message_by_transaction_status.insert(
                ScalarValue::Int32(transaction_status as i32),
                message.message_index,
            );

            block_messages.push(message);
        }

        for event_id in transaction_events_id.iter() {
            for sibling_id in transaction_events_id.iter() {
                if event_id != sibling_id {
                    join_event_to_siblings.insert(*event_id, *sibling_id);
                    join_event_to_siblings.insert(*sibling_id, *event_id);
                }
            }

            for message_id in transaction_messages_id.iter() {
                join_event_to_messages.insert(*event_id, *message_id);
                join_message_to_events.insert(*message_id, *event_id);
            }
        }

        for message_id in transaction_messages_id.iter() {
            for sibling_id in transaction_messages_id.iter() {
                if message_id != sibling_id {
                    join_message_to_siblings.insert(*message_id, *sibling_id);
                    join_message_to_siblings.insert(*sibling_id, *message_id);
                }
            }
        }

        if let Some(transaction_content) =
            transactions.and_then(|transactions| transactions.get(transaction_index as usize))
        {
            let mut transaction = transaction_content.to_proto();
            set_transaction_meta(
                &mut transaction,
                transaction_hash,
                transaction_index,
                transaction_status,
            );

            use starknet::transaction::Transaction;
            let transaction_type = match transaction.transaction {
                Some(Transaction::InvokeV0(_)) => Some(TransactionType::InvokeV0),
                Some(Transaction::InvokeV1(_)) => Some(TransactionType::InvokeV1),
                Some(Transaction::InvokeV3(_)) => Some(TransactionType::InvokeV3),
                Some(Transaction::Deploy(_)) => Some(TransactionType::Deploy),
                Some(Transaction::DeclareV0(_)) => Some(TransactionType::DeclareV0),
                Some(Transaction::DeclareV1(_)) => Some(TransactionType::DeclareV1),
                Some(Transaction::DeclareV2(_)) => Some(TransactionType::DeclareV2),
                Some(Transaction::DeclareV3(_)) => Some(TransactionType::DeclareV3),
                Some(Transaction::L1Handler(_)) => Some(TransactionType::L1Handler),
                Some(Transaction::DeployAccountV1(_)) => Some(TransactionType::DeployAccountV1),
                Some(Transaction::DeployAccountV3(_)) => Some(TransactionType::DeployAccountV3),
                None => None,
            };

            if let Some(transaction_type) = transaction_type {
                index_transaction_by_type
                    .insert(transaction_type.to_scalar_value(), transaction_index);
            }

            index_transaction_by_status.insert(
                ScalarValue::Int32(transaction_status as i32),
                transaction_index,
            );

            join_transaction_to_receipt.insert(transaction_index, transaction_index);

            block_transactions.push(transaction);
        }

        let mut receipt = receipt.to_proto();
        set_receipt_transaction_index(&mut receipt, transaction_index);

        block_receipts.push(receipt);
    }

    block_events.sort_by_key(|event| event.event_index);
    block_transactions.sort_by_key(|tx| {
        tx.meta
            .as_ref()
            .map(|m| m.transaction_index)
            .unwrap_or_default()
    });
    block_receipts.sort_by_key(|rx| {
        rx.meta
            .as_ref()
            .map(|m| m.transaction_index)
            .unwrap_or_default()
    });
    block_messages.sort_by_key(|msg| msg.message_index);

    let transaction_index = {
        let index_transaction_by_status = Index {
            index_id: INDEX_TRANSACTION_BY_STATUS,
            index: index_transaction_by_status
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let index_transaction_by_type = Index {
            index_id: INDEX_TRANSACTION_BY_TYPE,
            index: index_transaction_by_type
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        IndexFragment {
            fragment_id: TRANSACTION_FRAGMENT_ID,
            range_start: 0,
            range_len: block_transactions.len() as u32,
            indexes: vec![index_transaction_by_status, index_transaction_by_type],
        }
    };

    let transaction_join = {
        let join_transaction_to_receipt = Join {
            to_fragment_id: RECEIPT_FRAGMENT_ID,
            index: join_transaction_to_receipt.build().into(),
        };

        let join_transaction_to_events = Join {
            to_fragment_id: EVENT_FRAGMENT_ID,
            index: join_transaction_to_events
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let join_transaction_to_messages = Join {
            to_fragment_id: MESSAGE_FRAGMENT_ID,
            index: join_transaction_to_messages
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let join_transaction_to_trace = Join {
            to_fragment_id: TRACE_FRAGMENT_ID,
            index: join_transaction_to_trace.build().into(),
        };

        JoinFragment {
            fragment_id: TRANSACTION_FRAGMENT_ID,
            joins: vec![
                join_transaction_to_receipt,
                join_transaction_to_events,
                join_transaction_to_messages,
                join_transaction_to_trace,
            ],
        }
    };

    let transaction_fragment = BodyFragment {
        fragment_id: TRANSACTION_FRAGMENT_ID,
        name: TRANSACTION_FRAGMENT_NAME.to_string(),
        data: block_transactions
            .iter()
            .map(Message::encode_to_vec)
            .collect(),
    };

    // Empty since no receipt filter.
    let receipt_index = IndexFragment {
        fragment_id: RECEIPT_FRAGMENT_ID,
        range_start: 0,
        range_len: block_receipts.len() as u32,
        indexes: Vec::default(),
    };

    let receipt_join = JoinFragment {
        fragment_id: RECEIPT_FRAGMENT_ID,
        joins: Vec::default(),
    };

    let receipt_fragment = BodyFragment {
        fragment_id: RECEIPT_FRAGMENT_ID,
        name: RECEIPT_FRAGMENT_NAME.to_string(),
        data: block_receipts.iter().map(Message::encode_to_vec).collect(),
    };

    let event_index = {
        let index_event_by_address = Index {
            index_id: INDEX_EVENT_BY_ADDRESS,
            index: index_event_by_address
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_event_by_key0 = Index {
            index_id: INDEX_EVENT_BY_KEY0,
            index: index_event_by_key0
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_event_by_key1 = Index {
            index_id: INDEX_EVENT_BY_KEY1,
            index: index_event_by_key1
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_event_by_key2 = Index {
            index_id: INDEX_EVENT_BY_KEY2,
            index: index_event_by_key2
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_event_by_key3 = Index {
            index_id: INDEX_EVENT_BY_KEY3,
            index: index_event_by_key3
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_event_by_key_length = Index {
            index_id: INDEX_EVENT_BY_KEY_LENGTH,
            index: index_event_by_key_length
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_event_by_transaction_status = Index {
            index_id: INDEX_EVENT_BY_TRANSACTION_STATUS,
            index: index_event_by_transaction_status
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        IndexFragment {
            fragment_id: EVENT_FRAGMENT_ID,
            range_start: 0,
            range_len: block_events.len() as u32,
            indexes: vec![
                index_event_by_address,
                index_event_by_key0,
                index_event_by_key1,
                index_event_by_key2,
                index_event_by_key3,
                index_event_by_key_length,
                index_event_by_transaction_status,
            ],
        }
    };

    let event_join = {
        let join_event_to_transaction = Join {
            to_fragment_id: TRANSACTION_FRAGMENT_ID,
            index: join_event_to_transaction.build().into(),
        };

        let join_event_to_receipt = Join {
            to_fragment_id: RECEIPT_FRAGMENT_ID,
            index: join_event_to_receipt.build().into(),
        };

        let join_event_to_siblings = Join {
            to_fragment_id: EVENT_FRAGMENT_ID,
            index: join_event_to_siblings
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let join_event_to_messages = Join {
            to_fragment_id: MESSAGE_FRAGMENT_ID,
            index: join_event_to_messages
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let join_event_to_trace = Join {
            to_fragment_id: TRACE_FRAGMENT_ID,
            index: join_event_to_trace.build().into(),
        };

        JoinFragment {
            fragment_id: EVENT_FRAGMENT_ID,
            joins: vec![
                join_event_to_transaction,
                join_event_to_receipt,
                join_event_to_siblings,
                join_event_to_messages,
                join_event_to_trace,
            ],
        }
    };

    let event_fragment = BodyFragment {
        fragment_id: EVENT_FRAGMENT_ID,
        name: EVENT_FRAGMENT_NAME.to_string(),
        data: block_events.iter().map(Message::encode_to_vec).collect(),
    };

    let message_index = {
        let index_message_by_from_address = Index {
            index_id: INDEX_MESSAGE_BY_FROM_ADDRESS,
            index: index_message_by_from_address
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_message_by_to_address = Index {
            index_id: INDEX_MESSAGE_BY_TO_ADDRESS,
            index: index_message_by_to_address
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_message_by_transaction_status = Index {
            index_id: INDEX_MESSAGE_BY_TRANSACTION_STATUS,
            index: index_message_by_transaction_status
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        IndexFragment {
            fragment_id: MESSAGE_FRAGMENT_ID,
            range_start: 0,
            range_len: block_messages.len() as u32,
            indexes: vec![
                index_message_by_from_address,
                index_message_by_to_address,
                index_message_by_transaction_status,
            ],
        }
    };

    let message_join = {
        let join_message_to_transaction = Join {
            to_fragment_id: TRANSACTION_FRAGMENT_ID,
            index: join_message_to_transaction.build().into(),
        };

        let join_message_to_receipt = Join {
            to_fragment_id: RECEIPT_FRAGMENT_ID,
            index: join_message_to_receipt.build().into(),
        };

        let join_message_to_events = Join {
            to_fragment_id: EVENT_FRAGMENT_ID,
            index: join_message_to_events
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let join_message_to_siblings = Join {
            to_fragment_id: MESSAGE_FRAGMENT_ID,
            index: join_message_to_siblings
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let join_message_to_trace = Join {
            to_fragment_id: TRACE_FRAGMENT_ID,
            index: join_message_to_trace.build().into(),
        };

        JoinFragment {
            fragment_id: MESSAGE_FRAGMENT_ID,
            joins: vec![
                join_message_to_transaction,
                join_message_to_receipt,
                join_message_to_events,
                join_message_to_siblings,
                join_message_to_trace,
            ],
        }
    };

    let message_fragment = BodyFragment {
        fragment_id: MESSAGE_FRAGMENT_ID,
        name: MESSAGE_FRAGMENT_NAME.to_string(),
        data: block_messages.iter().map(Message::encode_to_vec).collect(),
    };

    // Empty since no transaction filter.
    let trace_index = IndexFragment {
        fragment_id: TRACE_FRAGMENT_ID,
        range_start: 0,
        range_len: block_traces.len() as u32,
        indexes: Vec::default(),
    };

    let trace_join = JoinFragment {
        fragment_id: TRACE_FRAGMENT_ID,
        joins: Vec::default(),
    };

    let trace_fragment = BodyFragment {
        fragment_id: TRACE_FRAGMENT_ID,
        name: TRACE_FRAGMENT_NAME.to_string(),
        data: block_traces.iter().map(Message::encode_to_vec).collect(),
    };

    Ok(BlockIngestionResult {
        body: vec![
            transaction_fragment,
            receipt_fragment,
            event_fragment,
            message_fragment,
            trace_fragment,
        ],
        index: vec![
            transaction_index,
            receipt_index,
            event_index,
            message_index,
            trace_index,
        ],
        join: vec![
            transaction_join,
            receipt_join,
            event_join,
            message_join,
            trace_join,
        ],
    })
}

pub(crate) fn collect_state_update_body_and_index(
    state_diff: &models::StateDiff,
) -> Result<BlockIngestionResult, IngestionError> {
    let mut block_storage_diffs = Vec::new();
    let mut block_contract_changes = Vec::new();
    let mut block_nonce_updates = Vec::new();

    let mut index_storage_diff_by_contract_address = BitmapIndexBuilder::default();
    let mut index_contract_change_by_type = BitmapIndexBuilder::default();
    let mut index_nonce_update_by_contract_address = BitmapIndexBuilder::default();

    for storage_diff in state_diff.storage_diffs.iter() {
        let index = block_storage_diffs.len() as u32;
        let storage_diff = storage_diff.to_proto();

        if let Some(contract_address) = storage_diff.contract_address {
            index_storage_diff_by_contract_address
                .insert(ScalarValue::B256(contract_address.to_bytes()), index);
        }

        block_storage_diffs.push(storage_diff);
    }

    for deprecated_declared_class in state_diff.deprecated_declared_classes.iter() {
        let index = block_contract_changes.len() as u32;

        let class_hash = deprecated_declared_class.to_proto();

        let change = starknet::contract_change::Change::DeclaredClass(starknet::DeclaredClass {
            class_hash: class_hash.into(),
            compiled_class_hash: None,
        });

        let contract_change = starknet::ContractChange {
            filter_ids: Vec::default(),
            change: change.into(),
        };

        index_contract_change_by_type
            .insert(ContractChangeType::DeclaredClass.to_scalar_value(), index);

        block_contract_changes.push(contract_change);
    }

    for declared_class in state_diff.declared_classes.iter() {
        let index = block_contract_changes.len() as u32;
        let declared_class = declared_class.to_proto();
        let change = starknet::contract_change::Change::DeclaredClass(declared_class);
        let contract_change = starknet::ContractChange {
            filter_ids: Vec::default(),
            change: change.into(),
        };

        index_contract_change_by_type
            .insert(ContractChangeType::DeclaredClass.to_scalar_value(), index);

        block_contract_changes.push(contract_change);
    }

    for replaced_class in state_diff.replaced_classes.iter() {
        let index = block_contract_changes.len() as u32;
        let replaced_class = replaced_class.to_proto();
        let change = starknet::contract_change::Change::ReplacedClass(replaced_class);
        let contract_change = starknet::ContractChange {
            filter_ids: Vec::default(),
            change: change.into(),
        };

        index_contract_change_by_type.insert(ContractChangeType::Replaced.to_scalar_value(), index);

        block_contract_changes.push(contract_change);
    }

    for deployed_contract in state_diff.deployed_contracts.iter() {
        let index = block_contract_changes.len() as u32;
        let deployed_contract = deployed_contract.to_proto();
        let change = starknet::contract_change::Change::DeployedContract(deployed_contract);
        let contract_change = starknet::ContractChange {
            filter_ids: Vec::default(),
            change: change.into(),
        };

        index_contract_change_by_type.insert(ContractChangeType::Deployed.to_scalar_value(), index);

        block_contract_changes.push(contract_change);
    }

    for nonce_update in state_diff.nonces.iter() {
        let index = block_nonce_updates.len() as u32;
        let nonce_update = nonce_update.to_proto();

        if let Some(contract_address) = nonce_update.contract_address {
            index_nonce_update_by_contract_address
                .insert(ScalarValue::B256(contract_address.to_bytes()), index);
        }

        block_nonce_updates.push(nonce_update);
    }

    let storage_diff_fragment = BodyFragment {
        fragment_id: STORAGE_DIFF_FRAGMENT_ID,
        name: STORAGE_DIFF_FRAGMENT_NAME.to_string(),
        data: block_storage_diffs
            .iter()
            .map(Message::encode_to_vec)
            .collect(),
    };

    let storage_diff_index = {
        let index_storage_diff_by_contract_address = Index {
            index_id: INDEX_STORAGE_DIFF_BY_CONTRACT_ADDRESS,
            index: index_storage_diff_by_contract_address
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        IndexFragment {
            fragment_id: STORAGE_DIFF_FRAGMENT_ID,
            range_start: 0,
            range_len: block_storage_diffs.len() as u32,
            indexes: vec![index_storage_diff_by_contract_address],
        }
    };

    let storage_diff_join = JoinFragment {
        fragment_id: STORAGE_DIFF_FRAGMENT_ID,
        joins: Vec::default(),
    };

    let contract_change_fragment = BodyFragment {
        fragment_id: CONTRACT_CHANGE_FRAGMENT_ID,
        name: CONTRACT_CHANGE_FRAGMENT_NAME.to_string(),
        data: block_contract_changes
            .iter()
            .map(Message::encode_to_vec)
            .collect(),
    };

    let contract_change_index = {
        let index_contract_change_by_type = Index {
            index_id: INDEX_CONTRACT_CHANGE_BY_TYPE,
            index: index_contract_change_by_type
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        IndexFragment {
            fragment_id: CONTRACT_CHANGE_FRAGMENT_ID,
            range_start: 0,
            range_len: block_contract_changes.len() as u32,
            indexes: vec![index_contract_change_by_type],
        }
    };

    let contract_change_join = JoinFragment {
        fragment_id: CONTRACT_CHANGE_FRAGMENT_ID,
        joins: Vec::default(),
    };

    let nonce_update_fragment = BodyFragment {
        fragment_id: NONCE_UPDATE_FRAGMENT_ID,
        name: NONCE_UPDATE_FRAGMENT_NAME.to_string(),
        data: block_nonce_updates
            .iter()
            .map(Message::encode_to_vec)
            .collect(),
    };

    let nonce_update_index = {
        let index_nonce_update_by_contract_address = Index {
            index_id: INDEX_NONCE_UPDATE_BY_CONTRACT_ADDRESS,
            index: index_nonce_update_by_contract_address
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        IndexFragment {
            fragment_id: NONCE_UPDATE_FRAGMENT_ID,
            range_start: 0,
            range_len: block_nonce_updates.len() as u32,
            indexes: vec![index_nonce_update_by_contract_address],
        }
    };

    let nonce_update_join = JoinFragment {
        fragment_id: NONCE_UPDATE_FRAGMENT_ID,
        joins: Vec::default(),
    };

    Ok(BlockIngestionResult {
        body: vec![
            storage_diff_fragment,
            contract_change_fragment,
            nonce_update_fragment,
        ],
        index: vec![
            storage_diff_index,
            contract_change_index,
            nonce_update_index,
        ],
        join: vec![storage_diff_join, contract_change_join, nonce_update_join],
    })
}

fn set_transaction_meta(
    transaction: &mut starknet::Transaction,
    transaction_hash: starknet::FieldElement,
    index: u32,
    status: starknet::TransactionStatus,
) {
    let Some(meta) = transaction.meta.as_mut() else {
        return;
    };

    meta.transaction_hash = transaction_hash.into();
    meta.transaction_index = index;
    meta.transaction_status = status as i32;
}

fn set_receipt_transaction_index(receipt: &mut starknet::TransactionReceipt, index: u32) {
    let Some(meta) = receipt.meta.as_mut() else {
        return;
    };

    meta.transaction_index = index;
}
