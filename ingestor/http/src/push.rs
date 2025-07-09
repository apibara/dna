use axum::{http::StatusCode, response::Json};
use serde_json::{Value, json};

/// Handler for the /v1/push endpoint.
///
/// This endpoint accepts POST requests with message data to be ingested into Wings.
/// For now, it simply returns a successful response to indicate the endpoint is working.
///
/// # Returns
///
/// Returns a JSON response with `{"status": "ok"}` on success.
pub async fn push_handler() -> Result<Json<Value>, StatusCode> {
    Ok(Json(json!({
        "status": "ok"
    })))
}
