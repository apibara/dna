use std::collections::HashSet;

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_json::ReaderBuilder;
use arrow_schema::ArrowError;
use axum::response::{IntoResponse, Response};
use axum::{Json as JsonExtractor, extract::State, http::StatusCode, response::Json};
use error_stack::{Report, bail};
use futures::StreamExt;
use futures::stream::FuturesOrdered;
use wings_ingestor_core::Batch;
use wings_metadata_core::admin::{NamespaceName, TopicName};

use crate::HttpIngestorState;
use crate::error::{HttpIngestorError, HttpIngestorResult};
use crate::types::{BatchResponse, ErrorResponse, PushRequest, PushResponse};

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
) -> impl IntoResponse {
    match process_push_request(&state, request).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => map_error_to_response(err),
    }
}

/// Process a push request by parsing namespace, resolving topics, and converting JSON to Arrow.
async fn process_push_request(
    state: &HttpIngestorState,
    request: PushRequest,
) -> HttpIngestorResult<PushResponse> {
    // Parse namespace name
    let namespace_name = NamespaceName::parse(&request.namespace).map_err(|err| {
        HttpIngestorError::BadRequest(format!(
            "invalid namespace format: {} {err}",
            request.namespace,
        ))
    })?;

    // Get namespace definition from cache
    let namespace_ref = state
        .namespace_cache
        .get(namespace_name.clone())
        .await
        .map_err(|err| {
            HttpIngestorError::Internal(format!(
                "failed to resolve namespace: {namespace_name} {err}"
            ))
        })?;

    // Process each topic's batches
    let mut seen = HashSet::new();
    let mut writes = FuturesOrdered::new();

    for batch in request.batches {
        // Parse topic name
        let topic_name = TopicName::new(&batch.topic, namespace_name.clone()).map_err(|err| {
            HttpIngestorError::BadRequest(format!("invalid topic name: {} {err}", batch.topic))
        })?;

        // Check that all batches have distinct (topic, partition).
        if !seen.insert((topic_name.clone(), batch.partition.clone())) {
            bail!(HttpIngestorError::BadRequest(format!(
                "duplicate batch for topic {} partition {:?}",
                topic_name, batch.partition
            )));
        }

        // Get topic definition from cache
        let topic_ref = state
            .topic_cache
            .get(topic_name.clone())
            .await
            .map_err(|err| {
                HttpIngestorError::Internal(format!("failed to resolve topic: {topic_name} {err}"))
            })?;

        // Process each batch for this topic
        if batch.data.is_empty() {
            bail!(HttpIngestorError::BadRequest(
                "no data provided".to_string()
            ));
        }

        let schema = topic_ref.schema_without_partition_column();
        let record_batch = parse_json_to_arrow(schema, &batch.data).map_err(|err| {
            HttpIngestorError::BadRequest(format!(
                "failed to parse JSON data for topic {topic_name}: {err}"
            ))
        })?;

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
    schema: SchemaRef,
    json_data: &[serde_json::Value],
) -> std::result::Result<RecordBatch, ArrowError> {
    // Convert JSON values to JSON strings for arrow-json
    let json_strings: Vec<String> = json_data.iter().map(|v| v.to_string()).collect();

    // Create a cursor from the JSON strings
    let json_bytes = json_strings.join("\n").into_bytes();
    let cursor = std::io::Cursor::new(json_bytes);

    // Use arrow-json to parse the JSON into a RecordBatch
    let reader = ReaderBuilder::new(schema.clone()).build(cursor)?;

    let mut batches = Vec::default();
    for batch in reader {
        let batch = batch?;
        batches.push(batch);
    }

    concat_batches(&schema, batches.iter())
}

fn map_error_to_response(error: Report<HttpIngestorError>) -> Response {
    let status_code = match error.current_context() {
        HttpIngestorError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        HttpIngestorError::BadRequest(_) => StatusCode::BAD_REQUEST,
        HttpIngestorError::NotFound(_) => StatusCode::NOT_FOUND,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };

    let response = Json(ErrorResponse {
        message: error.to_string(),
    });

    (status_code, response).into_response()
}
