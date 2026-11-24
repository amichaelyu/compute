//! API errors: every error body is `{"error": {"code", "message", "hint"}}`.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    pub hint: String,
}

pub type ApiResult<T> = Result<T, ApiError>;

impl ApiError {
    pub fn new(
        status: StatusCode,
        code: &'static str,
        message: impl Into<String>,
        hint: impl Into<String>,
    ) -> Self {
        ApiError { status, code, message: message.into(), hint: hint.into() }
    }
    pub fn bad_request(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message, hint)
    }
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message, "check the id with GET /v1/jobs")
    }
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            message,
            "sign a new challenge: POST /v1/auth/challenge, then POST /v1/auth/token",
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({"error": {"code": self.code, "message": self.message, "hint": self.hint}});
        (self.status, axum::Json(body)).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        tracing::error!("internal error: {e:#}");
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            format!("{e:#}"),
            "this is a daemon bug or an unreachable backend; check the daemon log",
        )
    }
}
