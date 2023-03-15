use std::fmt::Display;

use futures::TryStreamExt;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Channel, Streaming};
use tracing::debug;

use self::kv_client::KvClient;

tonic::include_proto!("remote");

pub struct RemoteTx {
    inner: Mutex<Streaming<Pair>>,
    tx: mpsc::Sender<Cursor>,
    tx_id: u64,
}

pub struct RemoteCursor<'a> {
    txn: &'a RemoteTx,
    cursor_id: u32,
}

impl RemoteTx {
    pub fn new(tx: mpsc::Sender<Cursor>, inner: Streaming<Pair>, tx_id: u64) -> Self {
        let inner = Mutex::new(inner);
        RemoteTx { tx, inner, tx_id }
    }

    pub async fn open_table(
        &self,
        table_name: &'static str,
    ) -> Result<RemoteCursor<'_>, tonic::Status> {
        let pair = self
            .send_cursor(Cursor {
                op: Op::Open as i32,
                bucket_name: table_name.to_string(),
                ..Cursor::default()
            })
            .await
            .expect("failed to send open cursor");
        let cursor = RemoteCursor {
            txn: &self,
            cursor_id: pair.cursor_id,
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

impl<'a> RemoteCursor<'a> {
    pub async fn first(&self) -> Result<Vec<u8>, tonic::Status> {
        let val = self
            .txn
            .send_cursor(Cursor {
                op: Op::First as i32,
                cursor: self.cursor_id,
                ..Cursor::default()
            })
            .await?;
        debug!(val = ?val, "cursor.first");
        Ok(val.v)
    }

    pub async fn next(&self) -> Result<Vec<u8>, tonic::Status> {
        let val = self
            .txn
            .send_cursor(Cursor {
                op: Op::Next as i32,
                cursor: self.cursor_id,
                ..Cursor::default()
            })
            .await?;
        debug!(val = ?val, "cursor.next");
        Ok(val.v)
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
