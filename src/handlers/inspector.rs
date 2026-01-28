use axum::{
    Json,
    extract::{Path, Query, State},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::ApiError,
    inspector::{
        InspectorCursor, ListEventsParams, StoreError, get_event, list_attempts, list_events,
        replay_event,
    },
    state::AppState,
    types::{
        GetEventResponse, ListAttemptsResponse, ListEventsResponse, ReplayEventRequest,
        ReplayEventResponse, WebhookEventStatus,
    },
};

#[derive(Debug, Deserialize)]
pub struct ListEventsQuery {
    limit: Option<i64>,
    before: Option<String>,
    status: Option<String>,
    endpoint_id: Option<String>,
    provider: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CursorPayload {
    received_at: String,
    id: String,
}

pub async fn list_events_handler(
    State(state): State<AppState>,
    Query(query): Query<ListEventsQuery>,
) -> Result<Json<ListEventsResponse>, ApiError> {
    let limit = parse_limit(query.limit)?;
    let before = match query.before {
        Some(raw) => Some(decode_cursor(&raw)?),
        None => None,
    };
    let status = match query.status {
        Some(raw) => Some(parse_status(&raw)?),
        None => None,
    };
    let endpoint_id = match query.endpoint_id {
        Some(raw) => Some(parse_uuid("endpoint_id", &raw)?),
        None => None,
    };
    let provider = match query.provider {
        Some(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err(ApiError::BadRequest(
                    "provider must be non-empty".to_string(),
                ));
            }
            Some(trimmed.to_string())
        }
        None => None,
    };

    let params = ListEventsParams {
        limit,
        before,
        status,
        endpoint_id,
        provider,
    };

    let result = list_events(&state.pool, &params)
        .await
        .map_err(map_store_error)?;
    let next_before = match result.next_before {
        Some(cursor) => Some(encode_cursor(&cursor)?),
        None => None,
    };

    Ok(Json(ListEventsResponse {
        events: result.events,
        next_before,
    }))
}

pub async fn get_event_handler(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
) -> Result<Json<GetEventResponse>, ApiError> {
    let event_id = parse_uuid("event_id", &event_id)?;
    let result = get_event(&state.pool, event_id)
        .await
        .map_err(map_store_error)?;
    Ok(Json(result))
}

pub async fn list_attempts_handler(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
) -> Result<Json<ListAttemptsResponse>, ApiError> {
    let event_id = parse_uuid("event_id", &event_id)?;
    let result = list_attempts(&state.pool, event_id)
        .await
        .map_err(map_store_error)?;
    Ok(Json(result))
}

pub async fn replay_event_handler(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
    Json(req): Json<ReplayEventRequest>,
) -> Result<Json<ReplayEventResponse>, ApiError> {
    let event_id = parse_uuid("event_id", &event_id)?;
    let reset_circuit = req.reset_circuit.unwrap_or(false);
    let result = replay_event(&state.pool, event_id, reset_circuit)
        .await
        .map_err(map_store_error)?;
    Ok(Json(result))
}

fn parse_limit(limit: Option<i64>) -> Result<i64, ApiError> {
    let limit = limit.unwrap_or(50);
    if !(1..=200).contains(&limit) {
        return Err(ApiError::BadRequest(
            "limit must be between 1 and 200".to_string(),
        ));
    }
    Ok(limit)
}

fn parse_uuid(field: &str, value: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(value).map_err(|_| ApiError::BadRequest(format!("{field} must be a UUID")))
}

fn parse_status(value: &str) -> Result<WebhookEventStatus, ApiError> {
    match value {
        "pending" => Ok(WebhookEventStatus::Pending),
        "in_flight" => Ok(WebhookEventStatus::InFlight),
        "requeued" => Ok(WebhookEventStatus::Requeued),
        "delivered" => Ok(WebhookEventStatus::Delivered),
        "dead" => Ok(WebhookEventStatus::Dead),
        "paused" => Ok(WebhookEventStatus::Paused),
        _ => Err(ApiError::BadRequest("status is invalid".to_string())),
    }
}

fn decode_cursor(raw: &str) -> Result<InspectorCursor, ApiError> {
    let decoded = URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| ApiError::BadRequest("before must be a valid cursor".to_string()))?;
    let payload: CursorPayload = serde_json::from_slice(&decoded)
        .map_err(|_| ApiError::BadRequest("before must be a valid cursor".to_string()))?;
    DateTime::parse_from_rfc3339(&payload.received_at)
        .map_err(|_| ApiError::BadRequest("before must be a valid cursor".to_string()))?;
    let id = Uuid::parse_str(&payload.id)
        .map_err(|_| ApiError::BadRequest("before must be a valid cursor".to_string()))?;
    Ok(InspectorCursor {
        received_at: payload.received_at,
        id,
    })
}

fn encode_cursor(cursor: &InspectorCursor) -> Result<String, ApiError> {
    let payload = CursorPayload {
        received_at: cursor.received_at.clone(),
        id: cursor.id.to_string(),
    };
    let encoded = serde_json::to_vec(&payload)
        .map_err(|_| ApiError::Internal("failed to encode cursor".to_string()))?;
    Ok(URL_SAFE_NO_PAD.encode(encoded))
}

fn map_store_error(err: StoreError) -> ApiError {
    match err {
        StoreError::Conflict(message) => ApiError::Conflict(message),
        StoreError::Db(db) => ApiError::Db(db),
        StoreError::NotFound(message) => ApiError::NotFound(message),
        StoreError::Parse(message) => ApiError::Internal(message),
    }
}
