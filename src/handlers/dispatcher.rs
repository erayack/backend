use axum::{Json, extract::State};
use chrono::DateTime;

use crate::{
    dispatcher::{StoreError, lease_events, report_delivery},
    error::ApiError,
    state::AppState,
    types::{LeaseRequest, LeaseResponse, ReportRequest, ReportResponse},
};

pub async fn lease_handler(
    State(state): State<AppState>,
    Json(req): Json<LeaseRequest>,
) -> Result<Json<LeaseResponse>, ApiError> {
    validate_request(&req)?;

    let events = lease_events(&state.pool, &req)
        .await
        .map_err(map_store_error)?;

    Ok(Json(LeaseResponse { events }))
}

pub async fn report_handler(
    State(state): State<AppState>,
    Json(req): Json<ReportRequest>,
) -> Result<Json<ReportResponse>, ApiError> {
    validate_report_request(&req)?;

    let result = report_delivery(&state.pool, &state.dispatcher, &req)
        .await
        .map_err(map_store_error)?;

    Ok(Json(ReportResponse {
        circuit: result.circuit,
        final_outcome: result.final_outcome,
    }))
}

fn validate_request(req: &LeaseRequest) -> Result<(), ApiError> {
    if req.limit <= 0 {
        return Err(ApiError::BadRequest("limit must be > 0".to_string()));
    }
    if req.lease_ms <= 0 {
        return Err(ApiError::BadRequest("lease_ms must be > 0".to_string()));
    }
    if req.worker_id.trim().is_empty() {
        return Err(ApiError::BadRequest("worker_id is required".to_string()));
    }

    Ok(())
}

fn validate_report_request(req: &ReportRequest) -> Result<(), ApiError> {
    if req.worker_id.trim().is_empty() {
        return Err(ApiError::BadRequest("worker_id is required".to_string()));
    }
    let started_at_raw = req.attempt.started_at.trim();
    let finished_at_raw = req.attempt.finished_at.trim();
    if started_at_raw.is_empty() || finished_at_raw.is_empty() {
        return Err(ApiError::BadRequest(
            "attempt started_at and finished_at are required".to_string(),
        ));
    }
    let started_at = parse_rfc3339("attempt started_at", started_at_raw)?;
    let finished_at = parse_rfc3339("attempt finished_at", finished_at_raw)?;
    if finished_at < started_at {
        return Err(ApiError::BadRequest(
            "attempt finished_at must be >= started_at".to_string(),
        ));
    }
    if let Some(value) = req.next_attempt_at.as_deref() {
        parse_rfc3339("next_attempt_at", value)?;
    }
    Ok(())
}

fn parse_rfc3339(field: &str, value: &str) -> Result<DateTime<chrono::FixedOffset>, ApiError> {
    DateTime::parse_from_rfc3339(value)
        .map_err(|_| ApiError::BadRequest(format!("{field} must be RFC3339")))
}

fn map_store_error(err: StoreError) -> ApiError {
    match err {
        StoreError::Conflict(message) => ApiError::Conflict(message),
        StoreError::Db(db) => ApiError::Db(db),
        StoreError::NotFound(message) => ApiError::NotFound(message),
        StoreError::Parse(message) => ApiError::Internal(message),
    }
}
