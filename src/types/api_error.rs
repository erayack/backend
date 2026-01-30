use serde::{Deserialize, Serialize};
use specta::Type;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "snake_case")]
pub enum ApiErrorCode {
    Validation,
    Unauthorized,
    RateLimited,
    NotFound,
    Conflict,
    Database,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct ApiErrorResponse {
    pub code: ApiErrorCode,
    pub message: String,
}
