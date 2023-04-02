use std::{io::Cursor, path::Path, time::Instant};

use apibara_node::o11y;
use clap::Parser;
use croaring::Bitmap;
use erigon_dna::{
    erigon::{
        tables,
        types::{Forkchoice, LogAddressIndex},
    },
    remote::{kv::RemoteTx, proto::remote::SnapshotsRequest, KvClient},
    snapshot::reader::SnapshotReader,
};
use ethers_core::types::{H160, H256};
use tracing::info;

#[derive(Parser)]
pub struct Cli {
    #[arg(long)]
    pub datadir: String,
}

/// Reads the bitmaps for the given addresses.
async fn get_addresses_bitmap(
    txn: &RemoteTx,
    addresses: impl Iterator<Item = H160>,
    from_block: u64,
    to_block: u64,
) -> Bitmap {
    let mut curr = txn
        .open_cursor::<tables::LogAddressIndexTable>()
        .await
        .unwrap();
    let mut chunks = Vec::new();
    for log_address in addresses {
        println!("get addr = {:?}", log_address);
        let log_addr_index = LogAddressIndex {
            log_address,
            block: from_block,
        };

        let mut value = curr.seek(&log_addr_index).await.unwrap();
        while let (k, v) = value {
            if k.log_address != log_address {
                break;
            }
            println!("k = {:?}", k);
            let bm = Bitmap::deserialize(&v);
            println!("v = {:?}", bm);
            chunks.push(bm);

            if k.block >= to_block {
                break;
            }

            value = curr.next().await.unwrap();
        }
    }
    let chunks: Vec<&Bitmap> = chunks.iter().collect();
    Bitmap::fast_or(&chunks)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    o11y::init_opentelemetry()?;

    let args = Cli::parse();

    let mut client = KvClient::connect("http://localhost:9090").await?;
    let version = client.version(()).await?.into_inner();
    info!(version = ?version, "connected to KV");

    let snapshots = client
        .snapshots(SnapshotsRequest::default())
        .await?
        .into_inner();
    info!(snapshots = ?snapshots, "snapshots");

    let txn = client.begin_txn().await?;

    /*
    let addr = vec![
        "0x1dc4c1cefef38a777b15aa20260a54e584b16c48".parse().unwrap(),
        "0xaf4159a80b6cc41ed517db1c453d1ef5c2e4db72".parse().unwrap(),
    ];

    let mut bitmap = Bitmap::create();
    bitmap.add_range(13520..1_000_000);
    println!("starting = {:?}", bitmap);
    let addr_bm = get_addresses_bitmap(&txn, addr.into_iter(), 0, 1_000_000).await;
    println!("addr = {:?}", addr_bm);
    bitmap &= addr_bm;
    println!("final = {:?}", bitmap);
    */
    let mut curr = txn.open_cursor::<tables::LogTable>().await?;

    let mut value = curr.first().await?;
    while let (k, v) = value {
        println!("k = {:?}", k);
        println!("v = {:?}", v);
        value = curr.next().await?;
    }
    /*
    {
        let mut curr = txn.open_cursor::<tables::LogAddressIndexTable>().await?;
        let mut value = curr.first().await?;

        let mut count = 0;
        while let (k, v) = value {
            let sub = Bitmap::deserialize(&v);
            println!("log = {:?}", sub);
            value = curr.next().await?;
            count += 1;
            if count > 10 {
                break;
            }
        }
    }
    */

    // let snapshots_dir = Path::new(&args.datadir).join("snapshots");
    // let chaindata_dir = Path::new(&args.datadir).join("chaindata");
    // println!("hash = {:?}", hash);
    /*
    let mut snapshot_reader = SnapshotReader::new(snapshots_dir);
    snapshot_reader.reopen_snapshots(snapshots.blocks_files.iter())?;

    let mut now = Instant::now();
    let interval = 100_000;
    for n in 0..8_632_000 {
        let _header = snapshot_reader.header(n)?;
        // info!(n = n, header = ?header, "found header");
        if n % interval == 0 {
            let ms = now.elapsed().as_millis();
            let rate = if ms > 0 {
                1000.0 * (interval as f64) / (now.elapsed().as_millis() as f64)
            } else {
                0.0
            };
            info!(n = n, elapsed = ?now.elapsed(), rate = %rate, "elapsed");
            now = Instant::now();
        }
    }
    */
    /*
    let snapshot_info = SnapshotInfo::new(1, 500_000, 1_000_000, datadir);
    let headers_snapshot_info = snapshot_info.headers();
    info!(info = ?headers_snapshot_info, "reading headers segment");

    let segment = Segment::<Header>::new(headers_snapshot_info)?;
    let mut reader = segment.new_reader(ByteSize::mib(1).as_u64() as usize);

    reader.reset(743_323)?;
    for block in reader.take(1) {
        info!(block = ?block, "block");
    }
    */
    /*
    let datadir = Path::new(&args.datadir);
    let mut snap_reader = RoSnapshots::new(datadir);
    snap_reader.reopen_files(&snapshots.blocks_files)?;

    let block = 8_000_100;
    let segment = snap_reader.view_headers(block).unwrap();
    // let segment = snap_reader.view_bodies(block).unwrap();
    // let segment = snap_reader.view_transactions(block).unwrap();

    let decompressor = segment.decompressor();
    info!(count = %decompressor.count(), "decompressor words count");

    let mut getter = decompressor.getter();

    let buf_size: usize = ByteSize::mib(5).as_u64() as usize;
    info!(buf_size = buf_size, other = 1024 * 1024, "alloc buffer");
    let mut buf = vec![0u8; buf_size];
    let mut counter = 0;
    while let Some(size) = getter.next_word(&mut buf) {
        // info!(i = counter, buf = hex::encode(&buf[1..size]), "got data");
        let header = Header::decode(&mut &buf[1..size]).expect("parse rlp");
        info!(i = counter, header = ?header, "got header");
        counter += 1;
    }
    */

    Ok(())
}
