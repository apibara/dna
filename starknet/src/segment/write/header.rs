use error_stack::ResultExt;
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use apibara_dna_common::error::{DnaError, Result};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{ingestion::models, segment::store};

pub trait BlockHeaderBuilderExt<'a: 'b, 'b> {
    fn create_header(
        builder: &'b mut FlatBufferBuilder<'a>,
        block: &models::BlockWithReceipts,
    ) -> WIPOffset<store::BlockHeader<'a>>;

    // fn copy_header<'c>(
    //     builder: &'b mut FlatBufferBuilder<'a>,
    //     header: &store::BlockHeader<'c>,
    // ) -> WIPOffset<store::BlockHeader<'a>>;
}

impl<'a: 'b, 'b> BlockHeaderBuilderExt<'a, 'b> for store::BlockHeaderBuilder<'a, 'b> {
    fn create_header(
        builder: &'b mut FlatBufferBuilder<'a>,
        block: &models::BlockWithReceipts,
    ) -> WIPOffset<store::BlockHeader<'a>> {
        let mut out = store::BlockHeaderBuilder::new(builder);

        out.finish()
    }
}
