use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use specta::Type;
use uuid::Uuid;

use super::{TargetCircuitState, WebhookAttemptErrorKind, WebhookEvent};

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct LeaseRequest {
    pub limit: i64,
    pub lease_ms: i64,
    pub worker_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct LeasedEvent {
    pub event: WebhookEvent,
    pub target_url: String,
    pub lease_expires_at: String,
    pub circuit: Option<TargetCircuitState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct LeaseResponse {
    pub events: Vec<LeasedEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct ReportRequest {
    pub worker_id: String,
    pub event_id: Uuid,
    pub outcome: ReportOutcome,
    pub retryable: bool,
    pub next_attempt_at: Option<String>,
    pub attempt: ReportAttempt,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct ReportAttempt {
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
pub enum ReportOutcome {
    Delivered,
    Retry,
    Dead,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct ReportResponse {
    pub circuit: Option<TargetCircuitState>,
    pub final_outcome: ReportOutcome,
}
