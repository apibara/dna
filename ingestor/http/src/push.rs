use std::collections::HashSet;

use arrow::record_batch::RecordBatch;
use arrow_json::ReaderBuilder;
use axum::{Json as JsonExtractor, extract::State, http::StatusCode, response::Json};
use error_stack::ResultExt;
use futures::StreamExt;
use futures::stream::FuturesOrdered;
use wings_ingestor_core::Batch;
use wings_metadata_core::admin::{NamespaceName, TopicName};

use crate::HttpIngestorState;
use crate::error::{HttpIngestorError, HttpIngestorResult};
use crate::types::{BatchResponse, PushRequest, PushResponse};

/// Handler for the /v1/push endpoint.
///
/// This endpoint accepts POST requests with message data to be ingested into Wings.
/// It parses the namespace name, resolves topics from the cache, and converts JSON
/// data to Arrow RecordBatches using the topic schemas.
///
/// # Arguments
///
/// * `topic_cache` - The topic cache for resolving topic schemas
/// * `request` - The push request containing namespace and batches of data
///
/// # Returns
///
/// Returns a JSON response with an empty PushResponse on success, or an error status code.
pub async fn push_handler(
    State(state): State<HttpIngestorState>,
    JsonExtractor(request): JsonExtractor<PushRequest>,
) -> Result<Json<PushResponse>, StatusCode> {
    match process_push_request(&state, request).await {
        Ok(response) => Ok(Json(response)),
        Err(error) => {
            eprintln!("Push handler error: {}", error);
            Err(map_error_to_status_code(&error))
        }
    }
}

/// Process a push request by parsing namespace, resolving topics, and converting JSON to Arrow.
async fn process_push_request(
    state: &HttpIngestorState,
    request: PushRequest,
) -> HttpIngestorResult<PushResponse> {
    // Parse namespace name
    let namespace_name = NamespaceName::parse(&request.namespace).change_context(
        HttpIngestorError::NamespaceParseError {
            message: format!("invalid namespace format: {}", request.namespace),
        },
    )?;

    // Get namespace definition from cache
    let namespace_ref = state
        .namespace_cache
        .get(namespace_name.clone())
        .await
        .change_context(HttpIngestorError::MetadataError {
            message: format!("failed to resolve namespace: {}", namespace_name),
        })?;

    // Process each topic's batches
    let mut seen = HashSet::new();
    let mut writes = FuturesOrdered::new();

    for batch in request.batches {
        // Parse topic name
        let topic_name = TopicName::new(&batch.topic, namespace_name.clone()).change_context(
            HttpIngestorError::TopicParseError {
                message: format!("invalid topic name: {}", batch.topic),
            },
        )?;

        // Check that all batches have distinct (topic, partition).
        if !seen.insert((topic_name.clone(), batch.partition.clone())) {
            return Err(HttpIngestorError::InvalidRequest {
                message: format!(
                    "duplicate batch for topic {} partition {:?}",
                    topic_name, batch.partition
                ),
            }
            .into());
        }

        // Get topic definition from cache
        let topic_ref = state
            .topic_cache
            .get(topic_name.clone())
            .await
            .change_context(HttpIngestorError::MetadataError {
                message: format!("failed to resolve topic: {}", topic_name),
            })?;

        // Process each batch for this topic
        let schema = topic_ref.schema_without_partition_column();
        let record_batch = parse_json_to_arrow(&batch.data, schema).change_context(
            HttpIngestorError::JsonParseError {
                message: format!("failed to parse JSON data for topic: {}", topic_name),
            },
        )?;

        let batch = Batch {
            namespace: namespace_ref.clone(),
            topic: topic_ref,
            partition: batch.partition,
            records: record_batch,
        };

        writes.push_back(state.batch_ingestion.write(batch));
    }

    let mut batches = Vec::with_capacity(writes.len());
    while let Some(write_result) = writes.next().await {
        match write_result {
            Ok(write_info) => batches.push(BatchResponse::Success {
                start_offset: write_info.start_offset,
                end_offset: write_info.end_offset,
            }),
            Err(err) => batches.push(BatchResponse::Error {
                message: err.to_string(),
            }),
        }
    }

    Ok(PushResponse { batches })
}

/// Parse JSON data into an Arrow RecordBatch using the provided schema.
fn parse_json_to_arrow(
    json_data: &[serde_json::Value],
    schema: arrow::datatypes::SchemaRef,
) -> HttpIngestorResult<RecordBatch> {
    if json_data.is_empty() {
        return Err(HttpIngestorError::JsonParseError {
            message: "no data provided".to_string(),
        }
        .into());
    }

    // Convert JSON values to JSON strings for arrow-json
    let json_strings: Vec<String> = json_data.iter().map(|v| v.to_string()).collect();

    // Create a cursor from the JSON strings
    let json_bytes = json_strings.join("\n").into_bytes();
    let cursor = std::io::Cursor::new(json_bytes);

    // Use arrow-json to parse the JSON into a RecordBatch
    let mut reader = ReaderBuilder::new(schema).build(cursor).change_context(
        HttpIngestorError::JsonParseError {
            message: "failed to create JSON reader".to_string(),
        },
    )?;

    let record_batch = reader
        .next()
        .ok_or_else(|| HttpIngestorError::JsonParseError {
            message: "no data in JSON reader".to_string(),
        })?
        .change_context(HttpIngestorError::JsonParseError {
            message: "failed to read JSON data".to_string(),
        })?;

    Ok(record_batch)
}

/// Map HttpIngestorError to appropriate HTTP status code.
fn map_error_to_status_code(error: &error_stack::Report<HttpIngestorError>) -> StatusCode {
    match error.current_context() {
        HttpIngestorError::NamespaceParseError { .. } => StatusCode::BAD_REQUEST,
        HttpIngestorError::TopicParseError { .. } => StatusCode::BAD_REQUEST,
        HttpIngestorError::TopicNotFound { .. } => StatusCode::NOT_FOUND,
        HttpIngestorError::JsonParseError { .. } => StatusCode::BAD_REQUEST,
        HttpIngestorError::InvalidRequest { .. } => StatusCode::BAD_REQUEST,
        HttpIngestorError::MetadataError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        HttpIngestorError::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        HttpIngestorError::BindError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        HttpIngestorError::ServerError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
