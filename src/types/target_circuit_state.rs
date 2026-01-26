use serde::{Deserialize, Serialize};
use specta::Type;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
pub struct TargetCircuitState {
    pub endpoint_id: Uuid,
    pub state: TargetCircuitStatus,
    /// If state is open and this is None, the circuit is open indefinitely.
    pub open_until: Option<String>,
    pub consecutive_failures: i64,
    pub last_failure_at: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Type)]
#[serde(rename_all = "snake_case")]
pub enum TargetCircuitStatus {
    Closed,
    Open,
}
