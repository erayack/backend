use serde::{Deserialize, Serialize};
use specta::Type;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct WebhookAttemptLog {
    pub id: Uuid,
    pub event_id: Uuid,
    pub attempt_no: i64,
    pub started_at: String,
    pub finished_at: String,

    pub request_headers: BTreeMap<String, String>,
    pub request_body: String,

    pub response_status: Option<i64>,
    pub response_headers: Option<BTreeMap<String, String>>,
    pub response_body: Option<String>,

    pub error_kind: Option<WebhookAttemptErrorKind>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Type)]
#[serde(rename_all = "snake_case")]
pub enum WebhookAttemptErrorKind {
    Timeout,
    Network,
    InvalidResponse,
    Unexpected,
}
