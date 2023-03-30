use std::path::Path;

use apibara_node::o11y;
use bytesize::ByteSize;
use clap::Parser;
use erigon_dna::snapshot::{file::SnapshotInfo, segment::Segment};
use reth_primitives::Header;
use tracing::info;

#[derive(Parser)]
pub struct Cli {
    #[arg(long)]
    pub datadir: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    o11y::init_opentelemetry()?;

    let args = Cli::parse();

    /*
    let mut client = KvClient::connect("http://localhost:9090").await?;
    let version = client.version(()).await?.into_inner();
    info!(version = ?version, "connected to KV");

    let snapshots = client
        .snapshots(SnapshotsRequest::default())
        .await?
        .into_inner();
    info!(snapshots = ?snapshots, "snapshots");
    */

    let datadir = Path::new(&args.datadir);
    let snapshot_info = SnapshotInfo::new(1, 500_000, 1_000_000, datadir);
    let headers_snapshot_info = snapshot_info.headers();
    info!(info = ?headers_snapshot_info, "reading headers segment");

    let segment = Segment::<Header>::new(headers_snapshot_info)?;
    let mut reader = segment.new_reader(ByteSize::mib(1).as_u64() as usize);

    reader.reset(743_323)?;
    for block in reader.take(1) {
        info!(block = ?block, "block");
    }
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
