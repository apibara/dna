use erigon_dna::proto::remote::{kv_client::KvClient, Cursor, Op};
use futures::TryStreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Hello, Erigon");
    let mut client = KvClient::connect("http://localhost:9090").await?;
    println!("Connected");
    let version = client.version(()).await?.into_inner();
    println!("Connected to KV version = {:?}", version);

    println!("Starting tx stream");
    let (tx_tx, cursor_rx) = mpsc::channel(128);
    let cursor_stream = ReceiverStream::new(cursor_rx);
    let mut stream = client.tx(cursor_stream).await?.into_inner();

    // Receive first message with txid.
    let tx = stream.try_next().await?.expect("Expected tx");
    println!("Got tx = {:?}", tx);

    // Open cursor.
    tx_tx
        .send(Cursor {
            op: Op::Open as i32,
            bucket_name: "CanonicalHeader".to_string(),
            ..Cursor::default()
        })
        .await?;

    let canon_cur = stream.try_next().await?.expect("Expected cursor").cursor_id;
    println!("Canonical chain cursor = {:?}", canon_cur);

    tx_tx
        .send(Cursor {
            op: Op::Open as i32,
            bucket_name: "Header".to_string(),
            ..Cursor::default()
        })
        .await?;

    let header_cur = stream.try_next().await?.expect("Expected cursor").cursor_id;
    println!("Headers cursor = {:?}", header_cur);

    // Seek to first element in canonical header table.
    tx_tx
        .send(Cursor {
            op: Op::First as i32,
            cursor: canon_cur,
            ..Cursor::default()
        })
        .await?;

    loop {
        let canon = stream.try_next().await?.expect("canonical");
        let block_number = u64::from_be_bytes(canon.k[..8].try_into().unwrap());
        let block_hash = hex::encode(&canon.v);
        println!();
        println!("{}    0x{}", block_number, block_hash);
        // seek to header
        let header_key = [canon.k, canon.v].concat();
        tx_tx
            .send(Cursor {
                op: Op::Seek as i32,
                cursor: header_cur,
                k: header_key,
                ..Cursor::default()
            })
            .await?;
        let header = stream.try_next().await?.expect("header");
        let header_rlp = hex::encode(&header.v);
        println!("  0x{}", header_rlp);
        tx_tx
            .send(Cursor {
                op: Op::Next as i32,
                cursor: canon_cur,
                ..Cursor::default()
            })
            .await?;
    }
}
