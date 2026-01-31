use serde::{Deserialize, Serialize};
use specta::Type;

use crate::types::{TargetCircuitState, WebhookAttemptLog, WebhookEvent, WebhookEventStatus};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct WebhookEventSummary {
    pub id: Uuid,
    pub endpoint_id: Uuid,
    pub replayed_from_event_id: Option<Uuid>,
    pub provider: String,
    pub status: WebhookEventStatus,
    pub attempts: i64,
    pub received_at: String,
    pub next_attempt_at: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct WebhookEventListItem {
    pub event: WebhookEventSummary,
    pub target_url: String,
    pub circuit: Option<TargetCircuitState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct ListEventsResponse {
    pub events: Vec<WebhookEventListItem>,
    pub next_before: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct GetEventResponse {
    pub event: WebhookEvent,
    pub target_url: String,
    pub circuit: Option<TargetCircuitState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct ListAttemptsResponse {
    pub attempts: Vec<WebhookAttemptLog>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type, Default)]
pub struct ReplayEventRequest {
    pub reset_circuit: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct ReplayEventResponse {
    pub event: WebhookEventSummary,
    pub circuit: Option<TargetCircuitState>,
}
