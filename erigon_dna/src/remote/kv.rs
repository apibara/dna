use std::{fmt::Display, marker::PhantomData};

use apibara_node::db::{Table, TableKey};
use futures::TryStreamExt;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Channel, Streaming};
use tracing::{debug, trace};

use crate::{
    erigon::tables::TableValue,
    remote::proto::remote::{kv_client::KvClient, Cursor, Op, Pair},
};

pub struct RemoteTx {
    inner: Mutex<Streaming<Pair>>,
    tx: mpsc::Sender<Cursor>,
    tx_id: u64,
}

pub struct RemoteCursor<'a, T>
where
    T: Table,
    T::Value: TableValue,
{
    txn: &'a RemoteTx,
    cursor_id: u32,
    _phantom: PhantomData<T>,
}

impl RemoteTx {
    pub fn new(tx: mpsc::Sender<Cursor>, inner: Streaming<Pair>, tx_id: u64) -> Self {
        let inner = Mutex::new(inner);
        RemoteTx { tx, inner, tx_id }
    }

    pub async fn open_cursor<T>(&self) -> Result<RemoteCursor<'_, T>, tonic::Status>
    where
        T: Table,
        T::Value: TableValue,
    {
        let bucket_name = T::db_name().to_string();
        let pair = self
            .send_cursor(Cursor {
                op: Op::Open as i32,
                bucket_name,
                ..Cursor::default()
            })
            .await
            .expect("failed to send open cursor");
        let cursor = RemoteCursor {
            txn: self,
            cursor_id: pair.cursor_id,
            _phantom: PhantomData::default(),
        };
        Ok(cursor)
    }

    async fn send_cursor(&self, cursor: Cursor) -> Result<Pair, tonic::Status> {
        self.tx
            .send(cursor)
            .await
            .expect("failed to send open cursor");
        let pair = self.inner.lock().await.try_next().await?.expect("pair");
        Ok(pair)
    }
}

impl<'a, T> RemoteCursor<'a, T>
where
    T: Table,
    T::Value: TableValue,
    <T::Key as TableKey>::Encoded: Into<Vec<u8>>,
{
    pub async fn first(&self) -> Result<(T::Key, T::Value), tonic::Status> {
        self.send_op(Op::First, None).await
    }

    pub async fn last(&self) -> Result<(T::Key, T::Value), tonic::Status> {
        self.send_op(Op::Last, None).await
    }

    pub async fn next(&self) -> Result<(T::Key, T::Value), tonic::Status> {
        self.send_op(Op::Next, None).await
    }

    pub async fn prev(&self) -> Result<(T::Key, T::Value), tonic::Status> {
        self.send_op(Op::Prev, None).await
    }

    pub async fn seek_exact(&self, key: &T::Key) -> Result<(T::Key, T::Value), tonic::Status> {
        self.send_op(Op::SeekExact, Some(key)).await
    }

    async fn send_op(
        &self,
        op: Op,
        key: Option<&T::Key>,
    ) -> Result<(T::Key, T::Value), tonic::Status> {
        let key = key.map(|k| k.encode().into()).unwrap_or_default();
        trace!(cid = self.cursor_id, op = ?op, key = ?key, "send_op");
        let pair = self
            .txn
            .send_cursor(Cursor {
                op: op as i32,
                cursor: self.cursor_id,
                k: key,
                ..Cursor::default()
            })
            .await?;
        trace!(cid = self.cursor_id, op = ?op, pair = ?pair, "pair <- send_op");
        let key = T::Key::decode(&pair.k).expect("failed to decode key");
        let value = T::Value::decode(&pair.v).expect("failed to decode value");
        Ok((key, value))
    }
}

impl KvClient<Channel> {
    pub async fn begin_txn(&mut self) -> Result<RemoteTx, tonic::Status> {
        let (tx_tx, cursor_rx) = mpsc::channel(128);
        let cursor_stream = ReceiverStream::new(cursor_rx);
        let mut stream = self.tx(cursor_stream).await?.into_inner();
        // receive first message with transaction id
        let txn = stream.try_next().await?.expect("begin txn");
        debug!(txn = ?txn, "begin txn");

        Ok(RemoteTx::new(tx_tx, stream, txn.tx_id))
    }
}

impl Display for RemoteTx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RemoteTx(tx_id={})", self.tx_id)
    }
}
