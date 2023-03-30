use std::{
    fs,
    io::{Cursor, Seek, SeekFrom},
    path::PathBuf,
    time::SystemTime,
};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use memmap::Mmap;
use tracing::trace;

use super::elias_fano::{EliasFano, EliasFanoError};

#[derive(Debug)]
pub struct Index {
    file_name: PathBuf,
    mod_time: SystemTime,
    size: u64,
    base_data_id: u64,
    offset_ef: EliasFano,
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    EliasFano(#[from] EliasFanoError),
}

impl Index {
    /// Creates a new index from the given file.
    pub fn new(file_name: PathBuf) -> Result<Self, IndexError> {
        let stat = fs::metadata(&file_name)?;
        let mod_time = stat.modified()?;
        let file = fs::File::open(&file_name)?;

        let mmap = unsafe { Mmap::map(&file)? };
        let mut cursor = Cursor::new(&mmap);

        let base_data_id = cursor.read_u64::<BigEndian>()?;
        let key_count = cursor.read_u64::<BigEndian>()?;
        let bytes_per_rec = cursor.read_u8()?;
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

        cursor.seek(SeekFrom::Start(offset))?;

        let bucket_count = cursor.read_u64::<BigEndian>()?;
        let bucket_size = cursor.read_u16::<BigEndian>()?;
        let leaf_size = cursor.read_u16::<BigEndian>()?;
        let primary_aggr_bound =
            leaf_size * u16::max(2, (0.35 * (leaf_size as f64) + 0.5).ceil() as u16);
        let secondary_aggr_bound = if leaf_size < 7 {
            primary_aggr_bound * 2
        } else {
            primary_aggr_bound * (0.21 * (leaf_size as f64) + 9.0 / 10.0).ceil() as u16
        };
        let salt = cursor.read_u32::<BigEndian>()?;
        let start_seed_len = cursor.read_u8()?;
        let mut start_seed = vec![0u64; start_seed_len as usize];
        for i in 0..start_seed_len {
            start_seed[i as usize] = cursor.read_u64::<BigEndian>()?;
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

        let has_enums = 0 != cursor.read_u8()?;
        assert!(has_enums, "index file doesn't have offset fe");
        let offset_ef = EliasFano::from_reader(&mut cursor)?;

        // erigon reads as u16 but increments offset by 4.
        let golomb_param_size = cursor.read_u32::<BigEndian>()?;
        // let golomb_rice = vec![0u32; golomb_param_size as usize];
        trace!(golomb_param_size = %golomb_param_size, "read golomb_param_size");
        // TODO: read golomb_rice?
        let l = cursor.read_u64::<BigEndian>()?;
        trace!(l = %l, "read l");
        let mut gr_data = vec![0u64; l as usize];
        // another case of reading without taking into account endiannes.
        cursor.read_u64_into::<LittleEndian>(&mut gr_data)?;
        trace!(l = %l, gr_data = ?gr_data, "read gr_data data");
        // TODO: read double elias fano (ef).

        let index = Index {
            file_name,
            mod_time,
            size: stat.len(),
            base_data_id,
            offset_ef,
        };

        Ok(index)
    }

    /// Returns the index file name.
    pub fn file_name(&self) -> &PathBuf {
        &self.file_name
    }

    /// Returns the index file modification time.
    pub fn mod_time(&self) -> &SystemTime {
        &self.mod_time
    }

    /// Returns the index file size.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Returns the base data id.
    pub fn base_data_id(&self) -> u64 {
        self.base_data_id
    }

    /// Returns the offset of the `i`-th element in the index.
    pub fn ordinal_lookup(&self, i: u64) -> Option<u64> {
        self.offset_ef.get(i)
    }
}
