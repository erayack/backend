use serde::{Deserialize, Serialize};
use specta::Type;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct WebhookEvent {
    pub id: Uuid,
    pub endpoint_id: Uuid,
    pub replayed_from_event_id: Option<Uuid>,
    pub provider: String,
    pub headers: BTreeMap<String, String>,
    pub payload: String,

    pub status: WebhookEventStatus,
    pub attempts: i64,

    pub received_at: String,
    pub next_attempt_at: Option<String>,

    pub lease_expires_at: Option<String>,
    pub leased_by: Option<String>,

    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Type, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WebhookEventStatus {
    Pending,
    InFlight,
    Requeued,
    Delivered,
    Dead,
    Paused,
}
