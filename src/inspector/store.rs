use std::collections::BTreeMap;

use chrono::Utc;
use sqlx::{QueryBuilder, SqlitePool};
use uuid::Uuid;

use crate::types::{
    GetEventResponse, ListAttemptsResponse, ReplayEventResponse, TargetCircuitState,
    TargetCircuitStatus, WebhookAttemptErrorKind, WebhookAttemptLog, WebhookEvent,
    WebhookEventListItem, WebhookEventStatus, WebhookEventSummary,
};

#[derive(Debug)]
pub enum StoreError {
    Db(sqlx::Error),
    Conflict(String),
    NotFound(String),
    Parse(String),
}

impl From<sqlx::Error> for StoreError {
    fn from(err: sqlx::Error) -> Self {
        Self::Db(err)
    }
}

#[derive(Debug, Clone)]
pub struct InspectorCursor {
    pub received_at: String,
    pub id: Uuid,
}

#[derive(Debug, Clone)]
pub struct ListEventsParams {
    pub limit: i64,
    pub before: Option<InspectorCursor>,
    pub status: Option<WebhookEventStatus>,
    pub endpoint_id: Option<Uuid>,
    pub provider: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ListEventsResult {
    pub events: Vec<WebhookEventListItem>,
    pub next_before: Option<InspectorCursor>,
}

pub async fn list_events(
    pool: &SqlitePool,
    params: &ListEventsParams,
) -> Result<ListEventsResult, StoreError> {
    let mut query = QueryBuilder::new(
        "SELECT \
            e.id, \
            e.endpoint_id, \
            e.replayed_from_event_id, \
            e.provider, \
            e.status, \
            e.attempts, \
            e.received_at, \
            e.next_attempt_at, \
            e.last_error, \
            ep.target_url, \
            c.state AS circuit_state, \
            c.open_until AS circuit_open_until, \
            c.consecutive_failures AS circuit_consecutive_failures, \
            c.last_failure_at AS circuit_last_failure_at \
        FROM webhook_events e \
        JOIN endpoints ep ON ep.id = e.endpoint_id \
        LEFT JOIN target_circuit_states c ON c.endpoint_id = e.endpoint_id \
        WHERE 1 = 1",
    );

    if let Some(status) = params.status {
        query.push(" AND e.status = ");
        query.push_bind(status_to_str(status));
    }

    if let Some(endpoint_id) = params.endpoint_id {
        query.push(" AND e.endpoint_id = ");
        query.push_bind(endpoint_id.to_string());
    }

    if let Some(provider) = params.provider.as_deref() {
        query.push(" AND e.provider = ");
        query.push_bind(provider);
    }

    if let Some(cursor) = &params.before {
        query.push(" AND (e.received_at < ");
        query.push_bind(&cursor.received_at);
        query.push(" OR (e.received_at = ");
        query.push_bind(&cursor.received_at);
        query.push(" AND e.id < ");
        query.push_bind(cursor.id.to_string());
        query.push("))");
    }

    query.push(" ORDER BY e.received_at DESC, e.id DESC LIMIT ");
    query.push_bind(params.limit + 1);

    let rows: Vec<ListEventRow> = query.build_query_as().fetch_all(pool).await?;

    let has_more = rows.len() > params.limit as usize;
    let take_count = if has_more {
        params.limit as usize
    } else {
        rows.len()
    };

    let mut events = Vec::with_capacity(take_count);
    let mut last_cursor = None;

    for row in rows.into_iter().take(take_count) {
        let (item, cursor) = list_item_from_row(row)?;
        last_cursor = Some(cursor);
        events.push(item);
    }

    let next_before = if has_more { last_cursor } else { None };

    Ok(ListEventsResult {
        events,
        next_before,
    })
}

pub async fn get_event(pool: &SqlitePool, event_id: Uuid) -> Result<GetEventResponse, StoreError> {
    let row = sqlx::query_as::<_, GetEventRow>(
        r"
        SELECT
            e.id,
            e.endpoint_id,
            e.provider,
            e.headers,
            e.payload,
            e.status,
            e.attempts,
            e.received_at,
            e.next_attempt_at,
            e.replayed_from_event_id,
            e.lease_expires_at,
            e.leased_by,
            e.last_error,
            ep.target_url,
            c.state AS circuit_state,
            c.open_until AS circuit_open_until,
            c.consecutive_failures AS circuit_consecutive_failures,
            c.last_failure_at AS circuit_last_failure_at
        FROM webhook_events e
        JOIN endpoints ep ON ep.id = e.endpoint_id
        LEFT JOIN target_circuit_states c ON c.endpoint_id = e.endpoint_id
        WHERE e.id = ?
        ",
    )
    .bind(event_id.to_string())
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| StoreError::NotFound("event not found".to_string()))?;

    get_event_from_row(row)
}

pub async fn list_attempts(
    pool: &SqlitePool,
    event_id: Uuid,
) -> Result<ListAttemptsResponse, StoreError> {
    let rows = sqlx::query_as::<_, ListAttemptsRow>(
        r"
        SELECT \
            e.id AS event_id, \
            a.id AS attempt_id, \
            a.attempt_no AS attempt_no, \
            a.started_at AS started_at, \
            a.finished_at AS finished_at, \
            a.request_headers AS request_headers, \
            a.request_body AS request_body, \
            a.response_status AS response_status, \
            a.response_headers AS response_headers, \
            a.response_body AS response_body, \
            a.error_kind AS error_kind, \
            a.error_message AS error_message \
        FROM webhook_events e
        LEFT JOIN webhook_attempt_logs a ON a.event_id = e.id
        WHERE e.id = ?
        ORDER BY a.started_at ASC, a.attempt_no ASC
        ",
    )
    .bind(event_id.to_string())
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Err(StoreError::NotFound("event not found".to_string()));
    }

    let mut attempts = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(attempt) = attempt_from_optional_row(row)? {
            attempts.push(attempt);
        }
    }

    Ok(ListAttemptsResponse { attempts })
}

pub async fn replay_event(
    pool: &SqlitePool,
    event_id: Uuid,
    reset_circuit: bool,
) -> Result<ReplayEventResponse, StoreError> {
    let now = Utc::now();

    let mut tx = pool.begin().await?;

    let row = sqlx::query_as::<_, ReplaySourceRow>(
        r"
        SELECT \
            id, \
            endpoint_id, \
            provider, \
            headers, \
            payload, \
            status, \
            received_at, \
            lease_expires_at \
        FROM webhook_events
        WHERE id = ?
        ",
    )
    .bind(event_id.to_string())
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| StoreError::NotFound("event not found".to_string()))?;

    let status = parse_status(&row.status)?;
    if status == WebhookEventStatus::InFlight {
        let lease_expires_at = row
            .lease_expires_at
            .as_deref()
            .ok_or_else(|| StoreError::Conflict("lease_missing".to_string()))?;
        let expires = chrono::DateTime::parse_from_rfc3339(lease_expires_at)
            .map_err(|_| StoreError::Parse("invalid lease_expires_at".to_string()))?;
        if expires > now {
            return Err(StoreError::Conflict("lease_active".to_string()));
        }
    }

    let new_event_id = Uuid::new_v4();
    sqlx::query(
        r"
        INSERT INTO webhook_events (
            id,
            endpoint_id,
            replayed_from_event_id,
            provider,
            headers,
            payload,
            status,
            attempts,
            received_at,
            next_attempt_at,
            lease_expires_at,
            leased_by,
            last_error
        )
        VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, NULL, NULL, NULL, NULL)
        ",
    )
    .bind(new_event_id.to_string())
    .bind(&row.endpoint_id)
    .bind(event_id.to_string())
    .bind(&row.provider)
    .bind(&row.headers)
    .bind(&row.payload)
    .bind(&row.received_at)
    .execute(&mut *tx)
    .await?;

    if reset_circuit {
        sqlx::query(
            r"
            UPDATE target_circuit_states
            SET state = 'closed',
                open_until = NULL,
                consecutive_failures = 0,
                last_failure_at = NULL
            WHERE endpoint_id = ?
            ",
        )
        .bind(&row.endpoint_id)
        .execute(&mut *tx)
        .await?;
    }

    let endpoint_row = sqlx::query_as::<_, ReplayEndpointRow>(
        r"
        SELECT ep.target_url,
               c.state AS circuit_state,
               c.open_until AS circuit_open_until,
               c.consecutive_failures AS circuit_consecutive_failures,
               c.last_failure_at AS circuit_last_failure_at
        FROM endpoints ep
        LEFT JOIN target_circuit_states c ON c.endpoint_id = ep.id
        WHERE ep.id = ?
        ",
    )
    .bind(&row.endpoint_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| StoreError::NotFound("endpoint not found".to_string()))?;

    tx.commit().await?;

    let summary = WebhookEventSummary {
        id: new_event_id,
        endpoint_id: Uuid::parse_str(&row.endpoint_id)
            .map_err(|err| StoreError::Parse(format!("invalid endpoint id: {err}")))?,
        replayed_from_event_id: Some(event_id),
        provider: row.provider,
        status: WebhookEventStatus::Pending,
        attempts: 0,
        received_at: row.received_at,
        next_attempt_at: None,
        last_error: None,
    };

    let circuit = map_circuit(
        &row.endpoint_id,
        endpoint_row.circuit_state.as_deref(),
        endpoint_row.circuit_open_until.as_deref(),
        endpoint_row.circuit_consecutive_failures,
        endpoint_row.circuit_last_failure_at.as_deref(),
    )?;

    Ok(ReplayEventResponse {
        event: summary,
        circuit,
    })
}

#[derive(sqlx::FromRow)]
struct ListEventRow {
    id: String,
    endpoint_id: String,
    replayed_from_event_id: Option<String>,
    provider: String,
    status: String,
    attempts: i64,
    received_at: String,
    next_attempt_at: Option<String>,
    last_error: Option<String>,
    target_url: String,
    circuit_state: Option<String>,
    circuit_open_until: Option<String>,
    circuit_consecutive_failures: Option<i64>,
    circuit_last_failure_at: Option<String>,
}

#[derive(sqlx::FromRow)]
struct GetEventRow {
    id: String,
    endpoint_id: String,
    replayed_from_event_id: Option<String>,
    provider: String,
    headers: String,
    payload: String,
    status: String,
    attempts: i64,
    received_at: String,
    next_attempt_at: Option<String>,
    lease_expires_at: Option<String>,
    leased_by: Option<String>,
    last_error: Option<String>,
    target_url: String,
    circuit_state: Option<String>,
    circuit_open_until: Option<String>,
    circuit_consecutive_failures: Option<i64>,
    circuit_last_failure_at: Option<String>,
}

#[derive(sqlx::FromRow)]
struct ListAttemptsRow {
    event_id: String,
    attempt_id: Option<String>,
    attempt_no: Option<i64>,
    started_at: Option<String>,
    finished_at: Option<String>,
    request_headers: Option<String>,
    request_body: Option<String>,
    response_status: Option<i64>,
    response_headers: Option<String>,
    response_body: Option<String>,
    error_kind: Option<String>,
    error_message: Option<String>,
}

#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct ReplaySourceRow {
    id: String,
    endpoint_id: String,
    provider: String,
    headers: String,
    payload: String,
    status: String,
    received_at: String,
    lease_expires_at: Option<String>,
}

#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct ReplayEndpointRow {
    target_url: String,
    circuit_state: Option<String>,
    circuit_open_until: Option<String>,
    circuit_consecutive_failures: Option<i64>,
    circuit_last_failure_at: Option<String>,
}

fn list_item_from_row(
    row: ListEventRow,
) -> Result<(WebhookEventListItem, InspectorCursor), StoreError> {
    let status = parse_status(&row.status)?;
    let event_id = Uuid::parse_str(&row.id)
        .map_err(|err| StoreError::Parse(format!("invalid event id: {err}")))?;
    let endpoint_id = Uuid::parse_str(&row.endpoint_id)
        .map_err(|err| StoreError::Parse(format!("invalid endpoint id: {err}")))?;
    let replayed_from_event_id = match row.replayed_from_event_id {
        Some(value) if value.is_empty() => None,
        Some(value) => Some(
            Uuid::parse_str(&value)
                .map_err(|err| StoreError::Parse(format!("invalid replayed_from_event_id: {err}")))?,
        ),
        None => None,
    };

    let event = WebhookEventSummary {
        id: event_id,
        endpoint_id,
        replayed_from_event_id,
        provider: row.provider,
        status,
        attempts: row.attempts,
        received_at: row.received_at.clone(),
        next_attempt_at: row.next_attempt_at,
        last_error: row.last_error,
    };

    let circuit = map_circuit(
        &row.endpoint_id,
        row.circuit_state.as_deref(),
        row.circuit_open_until.as_deref(),
        row.circuit_consecutive_failures,
        row.circuit_last_failure_at.as_deref(),
    )?;

    Ok((
        WebhookEventListItem {
            event,
            target_url: row.target_url,
            circuit,
        },
        InspectorCursor {
            received_at: row.received_at,
            id: event_id,
        },
    ))
}

fn get_event_from_row(row: GetEventRow) -> Result<GetEventResponse, StoreError> {
    let status = parse_status(&row.status)?;
    let headers: BTreeMap<String, String> = serde_json::from_str(&row.headers)
        .map_err(|err| StoreError::Parse(format!("invalid headers JSON: {err}")))?;

    let event = WebhookEvent {
        id: Uuid::parse_str(&row.id)
            .map_err(|err| StoreError::Parse(format!("invalid event id: {err}")))?,
        endpoint_id: Uuid::parse_str(&row.endpoint_id)
            .map_err(|err| StoreError::Parse(format!("invalid endpoint id: {err}")))?,
        replayed_from_event_id: match row.replayed_from_event_id {
            Some(value) if value.is_empty() => None,
            Some(value) => Some(
                Uuid::parse_str(&value)
                    .map_err(|err| StoreError::Parse(format!("invalid replayed_from_event_id: {err}")))?,
            ),
            None => None,
        },
        provider: row.provider,
        headers,
        payload: row.payload,
        status,
        attempts: row.attempts,
        received_at: row.received_at,
        next_attempt_at: row.next_attempt_at,
        lease_expires_at: row.lease_expires_at,
        leased_by: row.leased_by,
        last_error: row.last_error,
    };

    let circuit = map_circuit(
        &row.endpoint_id,
        row.circuit_state.as_deref(),
        row.circuit_open_until.as_deref(),
        row.circuit_consecutive_failures,
        row.circuit_last_failure_at.as_deref(),
    )?;

    Ok(GetEventResponse {
        event,
        target_url: row.target_url,
        circuit,
    })
}

fn attempt_from_optional_row(
    row: ListAttemptsRow,
) -> Result<Option<WebhookAttemptLog>, StoreError> {
    let Some(attempt_id) = row.attempt_id else {
        return Ok(None);
    };

    let attempt_no = row
        .attempt_no
        .ok_or_else(|| StoreError::Parse("attempt row missing attempt_no".to_string()))?;
    let started_at = row
        .started_at
        .ok_or_else(|| StoreError::Parse("attempt row missing started_at".to_string()))?;
    let finished_at = row
        .finished_at
        .ok_or_else(|| StoreError::Parse("attempt row missing finished_at".to_string()))?;
    let request_headers = row
        .request_headers
        .ok_or_else(|| StoreError::Parse("attempt row missing request_headers".to_string()))?;
    let request_body = row
        .request_body
        .ok_or_else(|| StoreError::Parse("attempt row missing request_body".to_string()))?;

    let request_headers: BTreeMap<String, String> = serde_json::from_str(&request_headers)
        .map_err(|err| StoreError::Parse(format!("invalid request headers JSON: {err}")))?;
    let response_headers = match row.response_headers {
        Some(headers) => Some(
            serde_json::from_str::<BTreeMap<String, String>>(&headers).map_err(|err| {
                StoreError::Parse(format!("invalid response headers JSON: {err}"))
            })?,
        ),
        None => None,
    };

    let error_kind = match row.error_kind.as_deref() {
        Some(kind) => Some(parse_error_kind(kind)?),
        None => None,
    };

    Ok(Some(WebhookAttemptLog {
        id: Uuid::parse_str(&attempt_id)
            .map_err(|err| StoreError::Parse(format!("invalid attempt id: {err}")))?,
        event_id: Uuid::parse_str(&row.event_id)
            .map_err(|err| StoreError::Parse(format!("invalid event id: {err}")))?,
        attempt_no,
        started_at,
        finished_at,
        request_headers,
        request_body,
        response_status: row.response_status,
        response_headers,
        response_body: row.response_body,
        error_kind,
        error_message: row.error_message,
    }))
}

fn map_circuit(
    endpoint_id: &str,
    state: Option<&str>,
    open_until: Option<&str>,
    consecutive_failures: Option<i64>,
    last_failure_at: Option<&str>,
) -> Result<Option<TargetCircuitState>, StoreError> {
    let Some(state) = state else {
        return Ok(None);
    };
    let endpoint_id = Uuid::parse_str(endpoint_id)
        .map_err(|err| StoreError::Parse(format!("invalid endpoint id: {err}")))?;
    let circuit_status = parse_circuit_status(state)?;
    Ok(Some(TargetCircuitState {
        endpoint_id,
        state: circuit_status,
        open_until: open_until.map(str::to_string),
        consecutive_failures: consecutive_failures.unwrap_or(0),
        last_failure_at: last_failure_at.map(str::to_string),
    }))
}

fn parse_status(status: &str) -> Result<WebhookEventStatus, StoreError> {
    match status {
        "pending" => Ok(WebhookEventStatus::Pending),
        "in_flight" => Ok(WebhookEventStatus::InFlight),
        "requeued" => Ok(WebhookEventStatus::Requeued),
        "delivered" => Ok(WebhookEventStatus::Delivered),
        "dead" => Ok(WebhookEventStatus::Dead),
        "paused" => Ok(WebhookEventStatus::Paused),
        other => Err(StoreError::Parse(format!("unknown status: {other}"))),
    }
}

fn status_to_str(status: WebhookEventStatus) -> &'static str {
    match status {
        WebhookEventStatus::Pending => "pending",
        WebhookEventStatus::InFlight => "in_flight",
        WebhookEventStatus::Requeued => "requeued",
        WebhookEventStatus::Delivered => "delivered",
        WebhookEventStatus::Dead => "dead",
        WebhookEventStatus::Paused => "paused",
    }
}

fn parse_circuit_status(status: &str) -> Result<TargetCircuitStatus, StoreError> {
    match status {
        "closed" => Ok(TargetCircuitStatus::Closed),
        "open" => Ok(TargetCircuitStatus::Open),
        other => Err(StoreError::Parse(format!(
            "unknown circuit status: {other}"
        ))),
    }
}

fn parse_error_kind(kind: &str) -> Result<WebhookAttemptErrorKind, StoreError> {
    match kind {
        "timeout" => Ok(WebhookAttemptErrorKind::Timeout),
        "network" => Ok(WebhookAttemptErrorKind::Network),
        "invalid_response" => Ok(WebhookAttemptErrorKind::InvalidResponse),
        "unexpected" => Ok(WebhookAttemptErrorKind::Unexpected),
        other => Err(StoreError::Parse(format!("unknown error kind: {other}"))),
    }
}
