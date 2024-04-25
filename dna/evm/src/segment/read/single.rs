use apibara_dna_common::{
    core::Cursor,
    error::{DnaError, Result},
    storage::StorageBackend,
};
use error_stack::ResultExt;
use flatbuffers::VerifierOptions;

use crate::segment::store::SingleBlock;

pub struct SingleBlockReader<S: StorageBackend> {
    storage: S,
    buffer: Vec<u8>,
    verifier_options: VerifierOptions,
}

impl<S> SingleBlockReader<S>
where
    S: StorageBackend,
    <S as StorageBackend>::Reader: Unpin,
{
    pub fn new(storage: S, buffer_size: usize) -> Self {
        let buffer = vec![0; buffer_size];
        let verifier_options = VerifierOptions::default();
        Self {
            storage,
            buffer,
            verifier_options,
        }
    }

    pub fn with_verifier_options(mut self, verifies_options: VerifierOptions) -> Self {
        self.verifier_options = verifies_options;
        self
    }

    pub async fn read<'a>(&'a mut self, cursor: &Cursor) -> Result<SingleBlock<'a>> {
        let prefix = format!("blocks/{}-{}", cursor.number, cursor.hash_as_hex());

        let mut reader = self.storage.get(&prefix, "block").await?;

        let len = {
            let mut cursor = std::io::Cursor::new(&mut self.buffer[..]);
            tokio::io::copy(&mut reader, &mut cursor)
                .await
                .change_context(DnaError::Io)? as usize
        };

        let block =
            flatbuffers::root_with_opts::<SingleBlock>(&self.verifier_options, &self.buffer[..len])
                .change_context(DnaError::Fatal)
                .attach_printable_lazy(|| format!("failed to read single block {}", cursor))?;

        Ok(block)
    }
}
