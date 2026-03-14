use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use state_search_core::CoreError;

/// Axum-compatible error wrapper — converts `CoreError` into HTTP responses.
pub struct ApiError(CoreError);

impl From<CoreError> for ApiError {
    fn from(e: CoreError) -> Self {
        Self(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self.0 {
            CoreError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            CoreError::InvalidInput(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg.clone()),
            CoreError::Database(e) => {
                tracing::error!(err = %e, "database error");
                (StatusCode::INTERNAL_SERVER_ERROR, "database error".to_string())
            }
            e => {
                tracing::error!(err = %e, "unexpected error");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string())
            }
        };

        (status, Json(json!({ "error": message }))).into_response()
    }
}

pub type ApiResult<T> = Result<T, ApiError>;
