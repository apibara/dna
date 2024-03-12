use apibara_dna_common::storage::{
    CacheOptions, CachedStorage, LocalStorageBackend, StorageBackend,
};
use tempdir::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const BASE_SIZE: usize = 1024;

#[tokio::test]
#[ignore]
pub async fn test_cached_storage() {
    // Use two temporary directories to simulate a remote and local storage.
    let remote_dir = TempDir::new("remote").unwrap();
    let local_dir = TempDir::new("local").unwrap();

    let local = LocalStorageBackend::new(local_dir.path());
    let mut remote = LocalStorageBackend::new(remote_dir.path());

    populate_storage(&mut remote).await;

    let cache_options = vec![
        CacheOptions {
            prefix: "group".to_string(),
            max_file_count: 2,
            max_size_bytes: (BASE_SIZE * 5) as u64,
        },
        CacheOptions {
            prefix: "segment".to_string(),
            max_file_count: 10,
            max_size_bytes: (BASE_SIZE * 10) as u64,
        },
    ];

    let mut cached = CachedStorage::new(local, remote, &cache_options);

    assert!(cached.exists("segment/file-0", "test.bin").await.unwrap());

    assert_eq!(cached.cache_size("group"), Some(0));
    assert_ne!(cached.max_cache_size("group"), Some(0));

    assert_eq!(cached.cache_size("segment"), Some(0));
    assert_ne!(cached.max_cache_size("segment"), Some(0));

    assert_eq!(cached.cache_size(""), None);

    // Items 1 to 3 all fit in the cache.
    for i in 1..4 {
        let segment = format!("segment/file-{}", i);
        let mut reader = cached.get(&segment, "test.bin").await.unwrap();

        let mut buf = Vec::with_capacity(BASE_SIZE * 12);
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(BASE_SIZE * i, buf.len());
    }

    assert_eq!(count_files(local_dir.path(), "segment"), 3);
    assert_eq!(
        cached.cache_size("segment"),
        Some((1 + 2 + 3) * BASE_SIZE as u64)
    );

    // Reading item 4 should evict item 1.
    {
        let segment = "segment/file-4";
        let mut reader = cached.get(segment, "test.bin").await.unwrap();

        let mut buf = Vec::with_capacity(BASE_SIZE * 12);
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(BASE_SIZE * 4, buf.len());
    }

    assert_eq!(count_files(local_dir.path(), "segment"), 3);
    assert_eq!(
        cached.cache_size("segment"),
        Some((2 + 3 + 4) * BASE_SIZE as u64)
    );

    // Reading from different prefix should not affect the segment cache.
    {
        let group = "group/file-9";
        let mut reader = cached.get(group, "test.bin").await.unwrap();

        let mut buf = Vec::with_capacity(BASE_SIZE * 12);
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(BASE_SIZE * 9, buf.len());
    }

    assert_eq!(count_files(local_dir.path(), "segment"), 3);
    assert_eq!(
        cached.cache_size("segment"),
        Some((2 + 3 + 4) * BASE_SIZE as u64)
    );

    // Reading item 9 should evict all items.
    {
        let segment = "segment/file-9";
        let mut reader = cached.get(segment, "test.bin").await.unwrap();

        let mut buf = Vec::with_capacity(BASE_SIZE * 12);
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(BASE_SIZE * 9, buf.len());
    }

    assert_eq!(count_files(local_dir.path(), "segment"), 1);
    assert_eq!(cached.cache_size("segment"), Some(9 * BASE_SIZE as u64));

    assert_eq!(count_files(local_dir.path(), "group"), 1);

    remote_dir.close().unwrap();
    local_dir.close().unwrap();
}

/// Generate some test data in local storage.
async fn populate_storage(local: &mut LocalStorageBackend) {
    for p in ["group", "segment"] {
        for i in 0..10 {
            let prefix = format!("{}/file-{}", p, i);
            let mut writer = local.put(prefix, "test.bin").await.unwrap();

            let data = vec![i as u8; BASE_SIZE * i];
            writer.write_all(&data).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();
        }
    }
}

fn count_files(base: &std::path::Path, prefix: &str) -> usize {
    if !base.join(prefix).exists() {
        return 0;
    }

    let mut count = 0;
    for dir in base.join(prefix).read_dir().unwrap() {
        for _ in dir.unwrap().path().read_dir().unwrap() {
            count += 1;
        }
    }

    count
}
