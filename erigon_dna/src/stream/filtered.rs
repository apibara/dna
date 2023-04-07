use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll, Waker},
};

use apibara_core::{
    ethereum::v1alpha2::{self, Filter},
    node::v1alpha2::{stream_data_response, Data, DataFinality, StreamDataResponse},
};
use apibara_node::stream::{
    IngestionMessage, StreamConfiguration, StreamError,
};
use async_stream::try_stream;
use croaring::Bitmap;
use futures::Stream;
use prost::Message;
use reth_primitives::{Header, H160, H256, U256};
use tokio::sync::Mutex;
use tracing::debug;

use crate::{
    access::Erigon,
    erigon::{GlobalBlockId, Log, TransactionLog},
};

type InnerStream = Pin<Box<dyn Stream<Item = Result<StreamDataResponse, StreamError>> + Send>>;

pub struct FilteredDataStream {
    erigon: Arc<Mutex<Erigon>>,
    inner: Option<InnerStream>,
    waker: Option<Waker>,
}

struct InnerState {
    started: bool,
    stream_id: u64,
    batch_size: usize,
    // data_finality: DataFinality,
    filter: EthersFilter,
    previous_iter_cursor: Option<GlobalBlockId>,
    finalized_cursor: Option<GlobalBlockId>,
    accepted_cursor: Option<GlobalBlockId>,
    erigon: Arc<Mutex<Erigon>>,
}

struct EthersFilter {
    inner: EthersFilterInner,
}

enum EthersFilterInner {
    NotReady { filter: Filter },
    Ready(EthersFilterReady),
}

struct EthersFilterReady {
    pub block_bitmap: Option<Bitmap>,
    pub block_bitmap_end: u64,
    pub header: Option<v1alpha2::HeaderFilter>,
    pub logs: Vec<EthersLog>,
}

struct EthersLog {
    address: Option<H160>,
    topics: Vec<TopicChoice>,
}

struct TopicChoice {
    choices: Vec<H256>,
}

trait BlockData {
    type Proto: Message;

    fn into_proto(self) -> Option<Self::Proto>;
}

impl FilteredDataStream {
    pub fn new(erigon: Erigon) -> Self {
        let erigon = Arc::new(Mutex::new(erigon));
        FilteredDataStream {
            erigon,
            inner: None,
            waker: None,
        }
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

impl InnerState {
    pub async fn advance_to_next_batch(
        &mut self,
    ) -> Result<Option<StreamDataResponse>, StreamError> {
        debug!(
            previous_iter_cursor = ?self.previous_iter_cursor,
            finalized_cursor = ?self.finalized_cursor,
            accepted_cursor = ?self.accepted_cursor,
            "advance next batch"
        );
        let next_cursor = {
            // fetch finalized block if first time starting.
            let mut erigon = self.erigon.lock().await;
            if !self.started {
                let finalized = erigon
                    .finalized_block()
                    .await
                    .map_err(StreamError::internal)?;
                self.finalized_cursor = finalized;
                self.filter.prepare(&mut erigon).await?;
            }

            let next_block_number = self
                .filter
                .next_block_number(&mut erigon, &self.previous_iter_cursor)
                .await?;

            /*
            if let Some(prev_iter_cursor) = self.previous_iter_cursor {
                // a zero/empty hash is used to start a stream from a specific block number
                // ignoring the block hash.
                if !prev_iter_cursor.hash().is_zero() {
                    todo!()
                }
            }
            */

            if let Some(cursor) = erigon
                .canonical_block_id(next_block_number)
                .await
                .map_err(StreamError::internal)?
            {
                cursor
            } else {
                return Ok(None);
            }
        };

        if let Some(finalized_cursor) = self.finalized_cursor {
            if next_cursor.number() <= finalized_cursor.number() {
                return self
                    .send_finalized_batch(next_cursor, &finalized_cursor)
                    .await;
            }
        }

        Ok(None)
    }

    async fn send_finalized_batch(
        &mut self,
        first_cursor: GlobalBlockId,
        finalized_cursor: &GlobalBlockId,
    ) -> Result<Option<StreamDataResponse>, StreamError> {
        use stream_data_response::Message;

        debug!(
            previous_iter_cursor = ?self.previous_iter_cursor,
            finalized_cursor = ?finalized_cursor,
            accepted_cursor = ?self.accepted_cursor,
            first_cursor = ?first_cursor,
            "send finalized batch"
        );

        let mut erigon = self.erigon.lock().await;
        let batch_start_cursor = self.previous_iter_cursor.map(|c| c.to_proto());

        let mut batch = Vec::with_capacity(self.batch_size);
        let mut batch_end_cursor = None;
        let mut current_cursor = first_cursor;

        let mut iter = 0;
        while batch.len() < self.batch_size && iter < 1_000 {
            iter += 1;

            {
                if let Some(data) = self.data_for_block(&mut erigon, &current_cursor).await? {
                    batch.push(data.encode_to_vec());
                }
            }

            batch_end_cursor = Some(current_cursor);
            match erigon
                .canonical_block_id(current_cursor.number() + 1)
                .await
                .map_err(StreamError::internal)?
            {
                None => break,
                Some(cursor) => {
                    current_cursor = cursor;
                }
            }
        }

        if batch_end_cursor.is_some() {
            self.previous_iter_cursor = batch_end_cursor;
            let data = Data {
                cursor: batch_start_cursor,
                end_cursor: batch_end_cursor.map(|c| c.to_proto()),
                finality: DataFinality::DataStatusFinalized as i32,
                data: batch,
            };

            let response = StreamDataResponse {
                stream_id: self.stream_id,
                message: Some(Message::Data(data)),
            };

            Ok(Some(response))
        } else {
            Ok(None)
        }
    }

    async fn data_for_block(
        &self,
        erigon: &mut Erigon,
        block_cursor: &GlobalBlockId,
    ) -> Result<Option<v1alpha2::Block>, StreamError> {
        let header = erigon
            .header_by_number(block_cursor.number())
            .await
            .map_err(StreamError::internal)?;

        let logs = erigon
            .logs_by_block_number(block_cursor.number())
            .await
            .map_err(StreamError::internal)?;
        let mut log_result = Vec::default();
        while let Some((_log_id, tx_logs)) = logs.next_log().await.map_err(StreamError::internal)? {
            let filtered = self.filter.filtered_logs(tx_logs);
            log_result.extend(filtered.into_iter().map(|l| l.into_proto().unwrap()));
        }

        let header = header.and_then(|h| h.into_proto());

        let block = v1alpha2::Block {
            status: v1alpha2::BlockStatus::Finalized as i32,
            header,
            logs: log_result,
        };

        Ok(Some(block))
    }
}

/*
impl ConfigurableDataStream<GlobalBlockId, Filter> for FilteredDataStream {
    fn handle_ingestion_message(
        &mut self,
        _message: IngestionMessage<GlobalBlockId>,
    ) -> Result<(), StreamError> {
        todo!()
    }

    fn reconfigure_data_stream(
        &mut self,
        configuration: StreamConfiguration<GlobalBlockId, Filter>,
    ) -> Result<(), StreamError> {
        let filter = EthersFilter {
            inner: EthersFilterInner::NotReady {
                filter: configuration.filter,
            },
        };

        let mut inner_state = InnerState {
            started: false,
            stream_id: configuration.stream_id,
            batch_size: configuration.batch_size,
            // data_finality: configuration.finality,
            previous_iter_cursor: configuration.starting_cursor,
            finalized_cursor: None,
            accepted_cursor: None,
            filter,
            erigon: self.erigon.clone(),
        };

        let inner = Box::pin(try_stream! {
            loop {
            if let Some(response) = inner_state.advance_to_next_batch().await? {
                yield response;
            } else {
                return
            }
            }
        });

        self.inner = Some(inner);
        self.wake();

        Ok(())
    }
}
*/

impl Stream for FilteredDataStream {
    type Item = Result<apibara_core::node::v1alpha2::StreamDataResponse, StreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        self.waker = Some(cx.waker().clone());
        if let Some(inner) = &mut self.inner {
            Pin::new(inner).poll_next(cx)
        } else {
            Poll::Pending
        }
    }
}

impl BlockData for Header {
    type Proto = v1alpha2::BlockHeader;

    fn into_proto(self) -> Option<Self::Proto> {
        let withdrawals_root = match self.withdrawals_root {
            None => None,
            Some(root) => Some(root.into()),
        };
        let block = v1alpha2::BlockHeader {
            block_hash: Some(self.hash_slow().into()),
            parent_block_hash: Some(self.parent_hash.into()),
            ommers_hash: Some(self.ommers_hash.into()),
            beneficiary: Some(self.beneficiary.into()),
            state_root: Some(self.state_root.into()),
            transactions_root: Some(self.transactions_root.into()),
            receipts_root: Some(self.receipts_root.into()),
            withdrawals_root,
            logs_bloom: vec![], // TODO
            difficulty: self.difficulty.into_proto()?,
            block_number: self.number,
            gas_limit: self.gas_limit,
            gas_used: self.gas_used,
            timestamp: None,
            mix_hash: Some(self.mix_hash.into()),
            nonce: self.nonce,
            base_fee_per_gas: self.base_fee_per_gas,
            extra_data: self.extra_data.to_vec(),
        };
        Some(block)
    }
}

impl EthersFilter {
    pub async fn prepare(&mut self, _erigon: &mut Erigon) -> Result<(), StreamError> {
        let proto_filter = match &self.inner {
            EthersFilterInner::Ready { .. } => return Ok(()),
            EthersFilterInner::NotReady { filter } => filter,
        };

        let logs = proto_filter.logs.iter().map(|log| log.into()).collect();

        let ready = EthersFilterReady {
            block_bitmap: None,
            block_bitmap_end: 0,
            header: proto_filter.header.clone(),
            logs,
        };
        let new_filter = EthersFilterInner::Ready(ready);
        self.inner = new_filter;
        Ok(())
    }

    pub fn filtered_logs(&self, tx_logs: TransactionLog) -> Vec<Log> {
        let logs_filter = match &self.inner {
            EthersFilterInner::Ready(ready) => &ready.logs,
            EthersFilterInner::NotReady { .. } => panic!("call prepare"),
        };

        let mut res = Vec::new();
        for tx_log in tx_logs.logs {
            let mut has_match = false;
            for filter in logs_filter {
                if let Some(addr) = filter.address {
                    if addr != tx_log.address {
                        continue;
                    }
                }

                let mut topic_match = true;
                for (idx, topic) in filter.topics.iter().enumerate() {
                    let log_topic_value = &tx_log.topics[idx];
                    let mut any_choice_matched = false;
                    for choice in &topic.choices {
                        if choice == log_topic_value {
                            any_choice_matched = true;
                            break;
                        }
                    }
                    if !any_choice_matched {
                        // no choice matched
                        topic_match = false;
                    }
                }

                if topic_match {
                    has_match = true;
                }
            }
            if has_match {
                res.push(tx_log);
            }
        }
        res
    }

    pub async fn next_block_number(
        &mut self,
        erigon: &mut Erigon,
        starting_cursor: &Option<GlobalBlockId>,
    ) -> Result<u64, StreamError> {
        self.reload_bitmap(erigon, starting_cursor).await?;

        let ready = match &mut self.inner {
            EthersFilterInner::NotReady { .. } => panic!("call prepare"),
            EthersFilterInner::Ready(ready) => ready,
        };

        let mut next_block_number = starting_cursor.map(|c| c.number() + 1).unwrap_or(0);
        // TODO: use the bitmap iterator.
        if let Some(bitmap) = &ready.block_bitmap {
            for block_number in next_block_number..(next_block_number + 100_000) {
                if bitmap.contains(block_number as u32) {
                    return Ok(block_number);
                }
            }
            Ok(next_block_number + 100_000)
        } else {
            Ok(next_block_number)
        }
    }

    pub async fn reload_bitmap(
        &mut self,
        erigon: &mut Erigon,
        starting_cursor: &Option<GlobalBlockId>,
    ) -> Result<(), StreamError> {
        // TODO: load bitmap
        // in that case, don't read bitmap filter.
        let mut ready = match &mut self.inner {
            EthersFilterInner::NotReady { .. } => panic!("call prepare"),
            EthersFilterInner::Ready(ready) => ready,
        };

        let is_weak =
            ready.header.is_none() || ready.header.as_ref().map(|f| f.weak).unwrap_or(false);
        if !is_weak {
            debug!("skip bitamp reload. weak");
            return Ok(());
        }

        // No need to reload
        let starting_number = starting_cursor.map(|c| c.number()).unwrap_or(0);
        let end_number = starting_number + 100_000;
        if ready.block_bitmap_end > starting_number {
            debug!("skip bitamp reload. still valid");
            return Ok(());
        }

        debug!("reloading bitmap");
        let mut bitmap = Bitmap::create();
        bitmap.add_range((starting_number as u32)..(end_number as u32));
        let addresses = ready.logs.iter().flat_map(|f| f.address);
        let logs_bitmap = erigon
            .log_bitmap_for_addresses(addresses, starting_number, end_number)
            .await
            .map_err(StreamError::internal)?;
        bitmap &= logs_bitmap;
        debug!(bitmap = ?bitmap, "new bitmap loaded");
        // logs may have read more than needed
        let bitmap_end = u64::max(end_number, bitmap.maximum().unwrap_or(0) as u64);

        ready.block_bitmap = Some(bitmap);
        ready.block_bitmap_end = bitmap_end;

        Ok(())
    }
}

impl BlockData for H256 {
    type Proto = v1alpha2::H256;

    fn into_proto(self) -> Option<Self::Proto> {
        Some(self.into())
    }
}

impl BlockData for H160 {
    type Proto = v1alpha2::H160;

    fn into_proto(self) -> Option<Self::Proto> {
        Some(self.into())
    }
}

impl BlockData for U256 {
    type Proto = u64;

    fn into_proto(self) -> Option<Self::Proto> {
        match self.try_into() {
            Err(_) => None,
            Ok(num) => Some(num),
        }
    }
}

impl BlockData for Log {
    type Proto = v1alpha2::LogWithTransaction;

    fn into_proto(self) -> Option<Self::Proto> {
        let address = self.address.into();
        let topics = self
            .topics
            .into_iter()
            .map(|t| t.into_proto().unwrap())
            .collect();
        let log = v1alpha2::Log {
            address: Some(address),
            topics,
            data: self.data,
        };
        Some(v1alpha2::LogWithTransaction {
            log: Some(log),
            transaction: None,
            receipt: None,
        })
    }
}

impl From<&v1alpha2::LogFilter> for EthersLog {
    fn from(value: &v1alpha2::LogFilter) -> Self {
        let address = value.address.as_ref().map(|a| a.into());
        let topics = value.topics.iter().map(|t| t.into()).collect();
        EthersLog { address, topics }
    }
}

impl From<&v1alpha2::TopicChoice> for TopicChoice {
    fn from(value: &v1alpha2::TopicChoice) -> Self {
        let choices = value.choices.iter().map(|c| c.into()).collect();
        TopicChoice { choices }
    }
}
