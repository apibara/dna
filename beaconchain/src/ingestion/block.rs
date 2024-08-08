//! Block ingestion helpers.

use alloy_eips::eip2718::Decodable2718;
use error_stack::{Result, ResultExt};

use crate::{provider::models, store::fragment};

use super::error::IngestionError;

pub fn decode_transaction(
    transaction_index: usize,
    mut bytes: &[u8],
) -> Result<fragment::Transaction, IngestionError> {
    let tx = models::TxEnvelope::network_decode(&mut bytes)
        .change_context(IngestionError::Serialization)
        .attach_printable("failed to decode EIP 2718 transaction")?;
    let mut tx =
        fragment::Transaction::try_from(tx).change_context(IngestionError::Serialization)?;

    tx.transaction_index = transaction_index as u32;

    Ok(tx)
}

pub fn add_transaction_to_blobs(
    blobs: &mut [fragment::Blob],
    transactions: &[fragment::Transaction],
) -> Result<(), IngestionError> {
    let mut blobs_updated = 0;
    for transaction in transactions {
        for blob_hash in transaction.blob_versioned_hashes.iter() {
            let blob = blobs
                .iter_mut()
                .find(|blob| &blob.blob_hash == blob_hash)
                .ok_or(IngestionError::Storage)
                .attach_printable("expected blob to exist")
                .attach_printable_lazy(|| {
                    format!(
                        "blob_hash: {:?}, transaction_hash: {:?}",
                        blob_hash, transaction.transaction_hash
                    )
                })?;
            blob.transaction_index = transaction.transaction_index;
            blob.transaction_hash = transaction.transaction_hash;

            blobs_updated += 1;
        }
    }

    assert!(blobs_updated == blobs.len());
    Ok(())
}
