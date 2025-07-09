use axum::{Json as JsonExtractor, http::StatusCode, response::Json};

use crate::types::{PushRequest, PushResponse};

/// Handler for the /v1/push endpoint.
///
/// This endpoint accepts POST requests with message data to be ingested into Wings.
/// For now, it simply returns a successful response to indicate the endpoint is working.
///
/// # Arguments
///
/// * `request` - The push request containing namespace and batches of data
///
/// # Returns
///
/// Returns a JSON response with an empty PushResponse on success.
pub async fn push_handler(
    JsonExtractor(request): JsonExtractor<PushRequest>,
) -> Result<Json<PushResponse>, StatusCode> {
    println!("Received push request for namespace: {}", request.namespace);
    println!("Number of batches: {}", request.batches.len());

    Ok(Json(PushResponse::new()))
}
