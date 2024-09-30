fn main() {
    divan::main();
}

mod bitmap_index {
    use std::io::Write;

    use apibara_dna_common::{
        file_cache::Mmap,
        index::{BitmapIndex, BitmapIndexBuilder, ScalarValue},
    };
    use divan::{black_box, Bencher};
    use rand::{distributions::Uniform, Rng, RngCore, SeedableRng};

    #[divan::bench(args = [10, 100, 1000])]
    fn with_bool_key(bencher: Bencher, bitmap_size: usize) {
        let rng = rand::rngs::StdRng::seed_from_u64(420);

        let mut builder = BitmapIndexBuilder::default();

        for value in rng
            .clone()
            .sample_iter(Uniform::new(0, bitmap_size))
            .take(bitmap_size)
        {
            builder.insert(ScalarValue::Bool(true), value as u32);
        }

        for value in rng
            .sample_iter(Uniform::new(0, bitmap_size))
            .take(bitmap_size)
        {
            builder.insert(ScalarValue::Bool(true), value as u32);
        }

        let mmap = mmap_bitmap_index(builder);

        bencher.bench_local(move || {
            let index = rkyv::from_bytes::<BitmapIndex, rkyv::rancor::Error>(&mmap)
                .expect("failed to deserialize bitmap index");
            black_box(index.get(&ScalarValue::Bool(true)));
        })
    }

    #[divan::bench(args = [10, 100, 1000], consts = [100, 1000, 10000])]
    fn with_b160_key<const N: usize>(bencher: Bencher, bitmap_size: usize) {
        let mut rng = rand::rngs::StdRng::seed_from_u64(420);

        let mut builder = BitmapIndexBuilder::default();

        for value in rng
            .clone()
            .sample_iter(Uniform::new(0, bitmap_size))
            .take(N)
        {
            let mut bytes = [0; 20];
            rng.fill_bytes(&mut bytes);
            builder.insert(ScalarValue::B160(bytes), value as u32);
        }

        let mmap = mmap_bitmap_index(builder);

        bencher.bench_local(move || {
            let index = rkyv::from_bytes::<BitmapIndex, rkyv::rancor::Error>(&mmap)
                .expect("failed to deserialize bitmap index");
            black_box(index.get(&ScalarValue::B160([0; 20])));
        })
    }

    #[divan::bench(args = [10, 100, 1000], consts = [100, 1000, 10000])]
    fn with_b256_key<const N: usize>(bencher: Bencher, bitmap_size: usize) {
        let mut rng = rand::rngs::StdRng::seed_from_u64(420);

        let mut builder = BitmapIndexBuilder::default();

        for value in rng
            .clone()
            .sample_iter(Uniform::new(0, bitmap_size))
            .take(N)
        {
            let mut bytes = [0; 32];
            rng.fill_bytes(&mut bytes);
            builder.insert(ScalarValue::B256(bytes), value as u32);
        }

        let mmap = mmap_bitmap_index(builder);

        bencher.bench_local(move || {
            let index = rkyv::from_bytes::<BitmapIndex, rkyv::rancor::Error>(&mmap)
                .expect("failed to deserialize bitmap index");
            black_box(index.get(&ScalarValue::B256([0; 32])));
        })
    }

    fn mmap_bitmap_index(builder: BitmapIndexBuilder) -> Mmap {
        let index = builder.build().expect("failed to build bitmap index");
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&index)
            .expect("failed to serialize bitmap index");
        let mut file = tempfile::tempfile().expect("failed to create temporary file");
        file.write_all(&bytes)
            .expect("failed to write to temporary file");
        file.flush().expect("failed to flush temporary file");

        Mmap::mmap(&file).expect("failed to mmap temporary file")
    }
}
