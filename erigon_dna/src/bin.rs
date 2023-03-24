use apibara_node::o11y;
use erigon_dna::erigon::{tables, Forkchoice, KvClient};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    o11y::init_opentelemetry()?;

    println!("Hello, Erigon");
    let mut client = KvClient::connect("http://localhost:9090").await?;
    println!("Connected");
    let version = client.version(()).await?.into_inner();
    println!("Connected to KV version = {:?}", version);

    println!("Starting tx stream");
    let txn = client.begin_txn().await.unwrap();

    let fc_cur = txn
        .open_cursor::<tables::LastForkchoiceTable>()
        .await
        .unwrap();
    let header_num_cur = txn
        .open_cursor::<tables::HeaderNumberTable>()
        .await
        .unwrap();

    let (_, head) = fc_cur.seek_exact(&Forkchoice::HeadBlockHash).await.unwrap();
    let (_, head_num) = header_num_cur.seek_exact(&head.into()).await.unwrap();
    println!("head = {:?} / {}", head, head_num);

    let (_, safe) = fc_cur.seek_exact(&Forkchoice::SafeBlockHash).await.unwrap();
    let (_, safe_num) = header_num_cur.seek_exact(&safe.into()).await.unwrap();
    println!("safe = {:?} / {}", safe, safe_num);

    let (_, finalized) = fc_cur
        .seek_exact(&Forkchoice::FinalizedBlockHash)
        .await
        .unwrap();
    let (_, finalized_num) = header_num_cur.seek_exact(&finalized.into()).await.unwrap();
    println!("final = {:?} / {}", finalized, finalized_num);

    Ok(())
}
