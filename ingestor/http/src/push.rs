use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use arrow_json::ReaderBuilder;
use axum::{Json as JsonExtractor, extract::State, http::StatusCode, response::Json};
use error_stack::ResultExt;
use wings_metadata_core::admin::{NamespaceName, TopicName};

use crate::HttpIngestorState;
use crate::error::{HttpIngestorError, HttpIngestorResult};
use crate::types::{PushRequest, PushResponse};

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

    println!("Received push request for namespace: {}", namespace_name);
    println!("Number of batches: {}", request.batches.len());

    // Group batches by topic to avoid repeated cache lookups
    let mut topic_batches: HashMap<String, Vec<&crate::types::Batch>> = HashMap::new();
    for batch in &request.batches {
        topic_batches
            .entry(batch.topic.clone())
            .or_default()
            .push(batch);
    }

    // Process each topic's batches
    for (topic_id, batches) in topic_batches {
        // Parse topic name
        let topic_name = TopicName::new(topic_id.clone(), namespace_name.clone()).change_context(
            HttpIngestorError::TopicParseError {
                message: format!("invalid topic name: {}", topic_id),
            },
        )?;

        // Get topic definition from cache
        let topic_ref = state
            .topic_cache
            .get(topic_name.clone())
            .await
            .change_context(HttpIngestorError::MetadataError {
                message: format!("failed to resolve topic: {}", topic_name),
            })?;

        // Process each batch for this topic
        for batch in batches {
            let record_batch = parse_json_to_arrow(&batch.data, topic_ref.schema())
                .change_context(HttpIngestorError::JsonParseError {
                    message: format!("failed to parse JSON data for topic: {}", topic_name),
                })?;

            println!(
                "Parsed batch for topic {} with {} rows",
                topic_name,
                record_batch.num_rows()
            );

            // TODO: Push the RecordBatch to the inner ingestor
            // For now, we just log the successful parsing
        }
    }

    Ok(PushResponse::new())
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
