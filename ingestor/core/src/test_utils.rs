use std::sync::Arc;

use arrow::array::{Int32Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema};

/// Generate a test batch with the specified number of rows.
/// The batch contains:
/// - `id`: row index (0, 1, 2, ...)
/// - `batch_size`: constant value equal to num_rows
pub fn generate_test_batch(num_rows: usize) -> RecordBatch {
    let ids: Vec<i32> = (0..num_rows as i32).collect();
    let batch_sizes: Vec<u64> = vec![num_rows as u64; num_rows];

    let id_array = Arc::new(Int32Array::from(ids));
    let batch_size_array = Arc::new(UInt64Array::from(batch_sizes));

    RecordBatch::try_new(schema(), vec![id_array, batch_size_array])
        .expect("Failed to create test batch")
}

pub fn fields() -> Fields {
    vec![
        Field::new("id", DataType::Int32, false),
        Field::new("batch_size", DataType::UInt64, false),
    ]
    .into()
}

/// Returns the schema for test data.
pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(fields()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_test_batch() {
        let batch = generate_test_batch(5);
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 2);

        let id_column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_column.values(), &[0, 1, 2, 3, 4]);

        let batch_size_column = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(batch_size_column.values(), &[5, 5, 5, 5, 5]);
    }

    #[test]
    fn test_schema() {
        let schema = schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).name(), "batch_size");
        assert_eq!(schema.field(1).data_type(), &DataType::UInt64);
    }
}
