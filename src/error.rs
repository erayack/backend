use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};

pub use crate::types::api_error::{ApiErrorCode, ApiErrorResponse};

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("validation error: {message}")]
    Validation { message: String },

    #[error("unauthorized: {message}")]
    Unauthorized { message: String },

    #[error("rate limited: {message}")]
    RateLimited { message: String },

    #[error("not found: {message}")]
    NotFound { message: String },

    #[error("conflict: {message}")]
    Conflict { message: String },

    #[error("database error")]
    Db(#[from] sqlx::Error),

    #[error("internal error: {message}")]
    Internal { message: String },
}

impl ApiError {
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation {
            message: message.into(),
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::Unauthorized {
            message: message.into(),
        }
    }

    pub fn rate_limited(message: impl Into<String>) -> Self {
        Self::RateLimited {
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound {
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    fn into_response_parts(self) -> (StatusCode, ApiErrorCode, String) {
        match self {
            Self::Validation { message } => {
                (StatusCode::BAD_REQUEST, ApiErrorCode::Validation, message)
            }
            Self::Unauthorized { message } => (
                StatusCode::UNAUTHORIZED,
                ApiErrorCode::Unauthorized,
                message,
            ),
            Self::RateLimited { message } => (
                StatusCode::TOO_MANY_REQUESTS,
                ApiErrorCode::RateLimited,
                message,
            ),
            Self::NotFound { message } => (StatusCode::NOT_FOUND, ApiErrorCode::NotFound, message),
            Self::Conflict { message } => (StatusCode::CONFLICT, ApiErrorCode::Conflict, message),
            Self::Db(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiErrorCode::Database,
                "database error".to_string(),
            ),
            Self::Internal { message } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiErrorCode::Internal,
                message,
            ),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = self.into_response_parts();
        (status, Json(ApiErrorResponse { code, message })).into_response()
    }
}
