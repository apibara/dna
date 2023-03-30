use std::{
    fs,
    io::{Cursor, Read, Seek, SeekFrom},
    path::PathBuf,
    time::SystemTime,
};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use memmap::Mmap;
use tracing::{info, trace};

use super::elias_fano::EliasFano;

#[derive(Debug)]
pub struct Index {
    /// Index file name.
    file_name: PathBuf,
    /// When the index was last modified.
    mod_time: SystemTime,
    /// The size of the index file, in bytes.
    size: u64,
    /// Mmapped content of the index.
    mmap: Mmap,
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError {}

impl Index {
    pub fn new(file_name: PathBuf) -> Result<Self, IndexError> {
        let stat = fs::metadata(&file_name).expect("failed to stat file");

        let file = fs::File::open(&file_name).expect("failed to open file");

        let mmap = unsafe { Mmap::map(&file).expect("failed to mmap file") };

        let mut cursor = Cursor::new(&mmap);

        let base_data_id = cursor
            .read_u64::<BigEndian>()
            .expect("failed to read base_data_id");
        let key_count = cursor
            .read_u64::<BigEndian>()
            .expect("failed to read key_count");
        let bytes_per_rec = cursor.read_u8().expect("failed to read bytes_per_rec");
        let rec_mask = (1u64 << (8 * bytes_per_rec)) - 1;
        let offset = 16 + 1 + key_count * (bytes_per_rec as u64);

        trace!(
            base_data_id = %base_data_id,
            key_count = %key_count,
            bytes_per_rec = %bytes_per_rec,
            rec_mask = %rec_mask,
            offset = %offset,
            "index file params"
        );

        cursor
            .seek(SeekFrom::Start(offset))
            .expect("failed to seek");

        let bucket_count = cursor
            .read_u64::<BigEndian>()
            .expect("failed to read bucket_count");
        let bucket_size = cursor
            .read_u16::<BigEndian>()
            .expect("failed to read bucket_size");
        let leaf_size = cursor
            .read_u16::<BigEndian>()
            .expect("failed to read leaf_size");
        let primary_aggr_bound =
            leaf_size * u16::max(2, (0.35 * (leaf_size as f64) + 0.5).ceil() as u16);
        let secondary_aggr_bound = if leaf_size < 7 {
            primary_aggr_bound * 2
        } else {
            primary_aggr_bound * (0.21 * (leaf_size as f64) + 9.0 / 10.0).ceil() as u16
        };
        let salt = cursor.read_u32::<BigEndian>().expect("failed to read salt");
        let start_seed_len = cursor.read_u8().expect("failed to read start_seed_len");
        let mut start_seed = vec![0u64; start_seed_len as usize];
        for i in 0..start_seed_len {
            start_seed[i as usize] = cursor
                .read_u64::<BigEndian>()
                .expect("failed to read start_seed");
        }

        trace!(
            bucket_count = %bucket_count,
            bucket_size = %bucket_size,
            leaf_size = %leaf_size,
            primary_aggr_bound = %primary_aggr_bound,
            secondary_aggr_bound = %secondary_aggr_bound,
            salt = %salt,
            start_seed_len = %start_seed_len,
            start_seed = ?start_seed,
            "read params"
        );

        let has_enums = 0 != cursor.read_u8().expect("failed to read enums");
        if has_enums {
            let ef = EliasFano::from_reader(&mut cursor).expect("failed to read elias fano");
        }

        // erigon reads as u16 but increments offset by 4.
        let golomb_param_size = cursor
            .read_u32::<BigEndian>()
            .expect("failed to read golomb_param_size");
        // let golomb_rice = vec![0u32; golomb_param_size as usize];
        trace!(golomb_param_size = %golomb_param_size, "read golomb_param_size");
        // TODO: read golomb_rice?
        let l = cursor.read_u64::<BigEndian>().expect("failed to read l");
        trace!(l = %l, "read l");
        let mut gr_data = vec![0u64; l as usize];
        // another case of reading without taking into account endiannes.
        cursor
            .read_u64_into::<LittleEndian>(&mut gr_data)
            .expect("failed to read gr_data");
        trace!(l = %l, gr_data = ?gr_data, "read gr_data data");
        // TODO: read double elias fano (ef).
        todo!()
    }
}
