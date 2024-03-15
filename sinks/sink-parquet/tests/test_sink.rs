use std::{ffi::OsString, sync::Arc};

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::{Context, CursorAction, Sink, SinkError};
use apibara_sink_parquet::{ParquetSink, SinkParquetConfiguration};
use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use error_stack::Result;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};
use std::fs::File;
use tempdir::TempDir;

fn read_parquet(output_dir: &TempDir, file_name: &str) -> RecordBatch {
    let file = File::open(output_dir.path().join(file_name)).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut reader = builder.build().unwrap();

    reader.next().unwrap().unwrap()
}

async fn new_sink(batch_size: usize) -> (TempDir, ParquetSink) {
    let output_dir = TempDir::new("sink_parquet_test").unwrap();

    let config = SinkParquetConfiguration {
        output_dir: output_dir.path().to_path_buf(),
        datasets: None,
        batch_size,
    };

    (output_dir, ParquetSink::new(config).await)
}

fn new_batch(start_cursor: &Option<Cursor>, end_cursor: &Cursor) -> Value {
    let mut batch = Vec::new();

    let start_block_num = match start_cursor {
        Some(cursor) => cursor.order_key,
        None => 0,
    };

    let end_block_num = end_cursor.order_key;

    for i in start_block_num..end_block_num {
        batch.push(json!({
            "block_num": i,
            "block_str": format!("block_{}", i),
        }));
    }
    json!(batch)
}

fn new_not_array_of_objects() -> Value {
    json!([0, { "key": "value" }, 1])
}

fn new_record_batch(start_cursor: &Option<Cursor>, end_cursor: &Cursor) -> RecordBatch {
    let start_block_num = match start_cursor {
        Some(cursor) => cursor.order_key as i64,
        None => 0,
    };

    let end_block_num = end_cursor.order_key as i64;

    let block_num_array: Vec<i64> = (start_block_num..end_block_num).collect();
    let block_str_array: Vec<String> = block_num_array
        .iter()
        .map(|i| format!("block_{}", i))
        .collect();

    let block_num_array: ArrayRef = Arc::new(Int64Array::from(block_num_array));
    let block_str_array: ArrayRef = Arc::new(StringArray::from(block_str_array));

    RecordBatch::try_from_iter_with_nullable(vec![
        ("block_num", block_num_array, true),
        ("block_str", block_str_array, true),
    ])
    .unwrap()
}

fn new_cursor(order_key: u64) -> Cursor {
    Cursor {
        order_key,
        unique_key: order_key.to_be_bytes().to_vec(),
    }
}

fn get_file_names(output_dir: &TempDir, path: &str) -> Option<Vec<OsString>> {
    if !output_dir.as_ref().join(path).exists() {
        return None;
    }
    let files = std::fs::read_dir(output_dir.as_ref().join(path))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    Some(files)
}

#[tokio::test]
async fn test_handle_data() -> Result<(), SinkError> {
    let parquet_batch_size = 10;
    let (output_dir, mut sink) = new_sink(parquet_batch_size).await;

    let finality = DataFinality::DataStatusFinalized;

    let cursor = None;
    let end_cursor = new_cursor(5);
    let batch = new_batch(&cursor, &end_cursor);

    let ctx = Context {
        cursor,
        end_cursor,
        finality,
    };

    let action = sink.handle_data(&ctx, &batch).await?;

    assert_eq!(action, CursorAction::Skip);

    let file_names = get_file_names(&output_dir, "default");
    assert!(file_names.is_none());

    let cursor = Some(new_cursor(5));
    let end_cursor = new_cursor(10);
    let batch = new_batch(&cursor, &end_cursor);
    let ctx = Context {
        cursor,
        end_cursor: end_cursor.clone(),
        finality,
    };

    let action = sink.handle_data(&ctx, &batch).await?;

    assert_eq!(action, CursorAction::PersistAt(end_cursor.clone()));

    let file_names: Vec<OsString> = get_file_names(&output_dir, "default").unwrap();
    assert_eq!(file_names, vec!["0000000000_0000000010.parquet"]);

    let cursor = Some(new_cursor(10));
    let end_cursor = new_cursor(15);
    let batch = new_batch(&cursor, &end_cursor);
    let ctx = Context {
        cursor,
        end_cursor,
        finality,
    };

    let action = sink.handle_data(&ctx, &batch).await?;

    assert_eq!(action, CursorAction::Skip);

    let file_names: Vec<OsString> = get_file_names(&output_dir, "default").unwrap();
    assert_eq!(file_names, vec!["0000000000_0000000010.parquet"]);

    let action = sink.handle_data(&ctx, &new_not_array_of_objects()).await?;

    assert_eq!(action, CursorAction::Skip);

    let action = sink.handle_data(&ctx, &json!([])).await?;

    assert_eq!(action, CursorAction::Skip);

    let record_batch = read_parquet(&output_dir, "default/0000000000_0000000010.parquet");
    let expected_record_batch = new_record_batch(&None, &new_cursor(10));
    assert_eq!(record_batch, expected_record_batch);

    Ok(())
}
