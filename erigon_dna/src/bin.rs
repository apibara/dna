use apibara_node::o11y;
use erigon_dna::erigon::remote::{ethbackend_client::EthbackendClient, Event, SubscribeRequest};
use futures::TryStreamExt;
use reth_primitives::Header;
use reth_rlp::Decodable;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    o11y::init_opentelemetry()?;

    let mut client = EthbackendClient::connect("http://localhost:9090").await?;

    let mut header_stream = client
        .subscribe(SubscribeRequest::default())
        .await?
        .into_inner();

    while let Some(message) = header_stream.try_next().await? {
        if message.r#type == Event::Header as i32 {
            let header = Header::decode(&mut message.data.as_ref())?;
            println!("HEADER = {:?}", header.hash_slow());
            println!("  {:?}", header);
            println!("");
        }
    }

    /*
    println!("Hello, Erigon");
    let mut client = KvClient::connect("http://localhost:9090").await?;
    println!("Connected");
    let version = client.version(()).await?.into_inner();
    println!("Connected to KV version = {:?}", version);

    println!("Starting tx stream");
    let txn = client.begin_txn().await.unwrap();

    let canon_cur = txn.open_table("CanonicalHeader").await.unwrap();
    let canon = canon_cur.first().await.unwrap();

    let header_cur = txn.open_table("Header").await.unwrap();
    let header = header_cur.first().await.unwrap();

    loop {
        let canon = canon_cur.next().await.unwrap();
        println!("data = {:?}", canon);
    }
    */
    /*

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
    */
    Ok(())
}
