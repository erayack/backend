use std::collections::BTreeMap;

use chrono::{Duration, SecondsFormat, Utc};
use sqlx::{QueryBuilder, SqlitePool};
use uuid::Uuid;

use crate::dispatcher::DispatcherConfig;
use crate::types::{
    LeasedEvent, LeaseRequest, ReportOutcome, ReportRequest, TargetCircuitState,
    TargetCircuitStatus, WebhookAttemptErrorKind, WebhookEvent, WebhookEventStatus,
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

pub async fn lease_events(
    pool: &SqlitePool,
    req: &LeaseRequest,
) -> Result<Vec<LeasedEvent>, StoreError> {
    let now = Utc::now();
    let now_str = format_utc(now);
    let lease_expires_at = format_utc(now + Duration::milliseconds(req.lease_ms));

    let mut tx = pool.begin().await?;

    sqlx::query(
        r#"
        UPDATE webhook_events
        SET status = 'requeued',
            lease_expires_at = NULL,
            leased_by = NULL
        WHERE status = 'in_flight'
            AND lease_expires_at IS NOT NULL
            AND lease_expires_at <= ?
        "#,
    )
    .bind(&now_str)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE target_circuit_states
        SET state = 'closed',
            open_until = NULL
        WHERE state = 'open'
          AND open_until IS NOT NULL
          AND open_until <= ?
        "#,
    )
    .bind(&now_str)
    .execute(&mut *tx)
    .await?;

    let leased_ids: Vec<String> = sqlx::query_scalar(
        r#"
        WITH eligible AS (
            SELECT e.id
            FROM webhook_events e
            LEFT JOIN target_circuit_states c
                ON c.endpoint_id = e.endpoint_id
            WHERE (e.status = 'pending' OR e.status = 'requeued')
                AND (e.next_attempt_at IS NULL OR e.next_attempt_at <= ?)
                AND (e.lease_expires_at IS NULL OR e.lease_expires_at <= ?)
                AND (
                    c.state IS NULL
                    OR c.state = 'closed'
                    OR (c.state = 'open' AND c.open_until IS NOT NULL AND c.open_until <= ?)
                )
            ORDER BY e.received_at ASC
            LIMIT ?
        )
        UPDATE webhook_events
        SET lease_expires_at = ?,
            leased_by = ?,
            status = 'in_flight'
        WHERE id IN (SELECT id FROM eligible)
            AND (status = 'pending' OR status = 'requeued')
            AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
            AND (lease_expires_at IS NULL OR lease_expires_at <= ?)
        RETURNING id
        "#,
    )
    .bind(&now_str)
    .bind(&now_str)
    .bind(&now_str)
    .bind(req.limit)
    .bind(&lease_expires_at)
    .bind(&req.worker_id)
    .bind(&now_str)
    .bind(&now_str)
    .fetch_all(&mut *tx)
    .await?;

    if leased_ids.is_empty() {
        tx.commit().await?;
        return Ok(Vec::new());
    }

    let mut fetch = QueryBuilder::new(
        "SELECT \
            e.id, \
            e.endpoint_id, \
            e.provider, \
            e.headers, \
            e.payload, \
            e.status, \
            e.attempts, \
            e.received_at, \
            e.next_attempt_at, \
            e.lease_expires_at, \
            e.leased_by, \
            e.last_error, \
            ep.target_url, \
            c.state AS circuit_state, \
            c.open_until AS circuit_open_until, \
            c.consecutive_failures AS circuit_consecutive_failures, \
            c.last_failure_at AS circuit_last_failure_at \
        FROM webhook_events e \
        JOIN endpoints ep ON ep.id = e.endpoint_id \
        LEFT JOIN target_circuit_states c ON c.endpoint_id = e.endpoint_id \
        WHERE e.id IN (",
    );
    let mut fetch_list = fetch.separated(", ");
    for id in &leased_ids {
        fetch_list.push_bind(id);
    }
    fetch_list.push_unseparated(")");

    let rows: Vec<LeaseRow> = fetch.build_query_as().fetch_all(&mut *tx).await?;

    tx.commit().await?;

    rows.into_iter().map(LeaseRow::try_into).collect()
}

pub struct ReportResult {
    pub circuit: Option<TargetCircuitState>,
    pub final_outcome: ReportOutcome,
}

pub async fn report_delivery(
    pool: &SqlitePool,
    config: &DispatcherConfig,
    req: &ReportRequest,
) -> Result<ReportResult, StoreError> {
    let now = Utc::now();
    let now_str = format_utc(now);
    let event_id = req.event_id.to_string();

    let mut tx = pool.begin().await?;
    let mut circuit_state: Option<TargetCircuitState> = None;

    let row = sqlx::query_as::<_, ReportEventRow>(
        r#"
        SELECT endpoint_id, attempts, leased_by, lease_expires_at
        FROM webhook_events
        WHERE id = ?
        "#,
    )
    .bind(&event_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| StoreError::NotFound("event not found".to_string()))?;

    let leased_by = row
        .leased_by
        .as_deref()
        .ok_or_else(|| StoreError::Conflict("lease_missing".to_string()))?;
    if leased_by != req.worker_id {
        return Err(StoreError::Conflict("lease_not_owned".to_string()));
    }

    let lease_expires_at = row
        .lease_expires_at
        .as_deref()
        .ok_or_else(|| StoreError::Conflict("lease_missing".to_string()))?;

    if let Ok(expires) = chrono::DateTime::parse_from_rfc3339(lease_expires_at) {
        if expires <= now {
            return Err(StoreError::Conflict("lease_expired".to_string()));
        }
    }

    let request_headers = serde_json::to_string(&req.attempt.request_headers)
        .map_err(|err| StoreError::Parse(format!("invalid request headers JSON: {err}")))?;
    let response_headers = match &req.attempt.response_headers {
        Some(headers) => Some(
            serde_json::to_string(headers)
                .map_err(|err| StoreError::Parse(format!("invalid response headers JSON: {err}")))?,
        ),
        None => None,
    };
    let error_kind = req
        .attempt
        .error_kind
        .map(error_kind_to_str)
        .map(str::to_string);

    let attempt_id = Uuid::new_v4().to_string();
    let attempt_no = row.attempts + 1;
    let endpoint_id = Uuid::parse_str(&row.endpoint_id)
        .map_err(|err| StoreError::Parse(format!("invalid endpoint id: {err}")))?;

    let retryable = req.retryable;

    let exhausted = attempt_no >= config.max_attempts as i64;
    let final_outcome = if exhausted && matches!(req.outcome, ReportOutcome::Retry) {
        ReportOutcome::Dead
    } else {
        req.outcome
    };

    let last_error_for_exhausted = if exhausted && matches!(req.outcome, ReportOutcome::Retry) {
        Some(format!(
            "max_attempts_exceeded ({}): {}",
            config.max_attempts,
            req.attempt
                .error_message
                .as_deref()
                .unwrap_or("unknown")
        ))
    } else {
        None
    };

    match final_outcome {
        ReportOutcome::Delivered => {
            let result = sqlx::query(
                r#"
                UPDATE webhook_events
                SET status = 'delivered',
                    attempts = attempts + 1,
                    next_attempt_at = NULL,
                    lease_expires_at = NULL,
                    leased_by = NULL,
                    last_error = NULL
                WHERE id = ?
                  AND leased_by = ?
                "#,
            )
            .bind(&event_id)
            .bind(&req.worker_id)
            .execute(&mut *tx)
            .await?;
            if result.rows_affected() == 0 {
                return Err(StoreError::Conflict("lease_not_owned".to_string()));
            }

            let updated = sqlx::query(
                r#"
                UPDATE target_circuit_states
                SET state = 'closed',
                    open_until = NULL,
                    consecutive_failures = 0,
                    last_failure_at = NULL
                WHERE endpoint_id = ?
                "#,
            )
            .bind(&row.endpoint_id)
            .execute(&mut *tx)
            .await?;

            if updated.rows_affected() > 0 {
                circuit_state = Some(TargetCircuitState {
                    endpoint_id,
                    state: TargetCircuitStatus::Closed,
                    open_until: None,
                    consecutive_failures: 0,
                    last_failure_at: None,
                });
            }
        }
        ReportOutcome::Retry => {
            let next_attempt_at = match req.next_attempt_at.as_deref() {
                Some(value) => normalize_rfc3339_utc(value)?,
                None => compute_next_attempt_at(now, attempt_no),
            };
            let last_error = req
                .attempt
                .error_message
                .clone()
                .or_else(|| error_kind.clone());

            let result = sqlx::query(
                r#"
                UPDATE webhook_events
                SET status = 'pending',
                    attempts = attempts + 1,
                    next_attempt_at = ?,
                    lease_expires_at = NULL,
                    leased_by = NULL,
                    last_error = ?
                WHERE id = ?
                  AND leased_by = ?
                "#,
            )
            .bind(next_attempt_at)
            .bind(last_error.as_deref())
            .bind(&event_id)
            .bind(&req.worker_id)
            .execute(&mut *tx)
            .await?;
            if result.rows_affected() == 0 {
                return Err(StoreError::Conflict("lease_not_owned".to_string()));
            }

            circuit_state = update_circuit_on_failure(
                &mut tx,
                config,
                &row.endpoint_id,
                endpoint_id,
                now,
                &now_str,
                retryable,
            )
            .await?;
        }
        ReportOutcome::Dead => {
            let last_error = last_error_for_exhausted
                .clone()
                .or_else(|| req.attempt.error_message.clone())
                .or_else(|| error_kind.clone());

            let result = sqlx::query(
                r#"
                UPDATE webhook_events
                SET status = 'dead',
                    attempts = attempts + 1,
                    next_attempt_at = NULL,
                    lease_expires_at = NULL,
                    leased_by = NULL,
                    last_error = ?
                WHERE id = ?
                  AND leased_by = ?
                "#,
            )
            .bind(last_error.as_deref())
            .bind(&event_id)
            .bind(&req.worker_id)
            .execute(&mut *tx)
            .await?;
            if result.rows_affected() == 0 {
                return Err(StoreError::Conflict("lease_not_owned".to_string()));
            }

            circuit_state = update_circuit_on_failure(
                &mut tx,
                config,
                &row.endpoint_id,
                endpoint_id,
                now,
                &now_str,
                retryable,
            )
            .await?;
        }
    }

    sqlx::query(
        r#"
        INSERT INTO webhook_attempt_logs (
            id,
            event_id,
            attempt_no,
            started_at,
            finished_at,
            request_headers,
            request_body,
            response_status,
            response_headers,
            response_body,
            error_kind,
            error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&attempt_id)
    .bind(&event_id)
    .bind(attempt_no)
    .bind(&req.attempt.started_at)
    .bind(&req.attempt.finished_at)
    .bind(&request_headers)
    .bind(&req.attempt.request_body)
    .bind(req.attempt.response_status)
    .bind(response_headers.as_deref())
    .bind(req.attempt.response_body.as_deref())
    .bind(error_kind.as_deref())
    .bind(req.attempt.error_message.as_deref())
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(ReportResult {
        circuit: circuit_state,
        final_outcome,
    })
}

#[derive(sqlx::FromRow)]
struct LeaseRow {
    id: String,
    endpoint_id: String,
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

impl TryFrom<LeaseRow> for LeasedEvent {
    type Error = StoreError;

    fn try_from(row: LeaseRow) -> Result<Self, Self::Error> {
        let status = parse_status(&row.status)?;
        let headers: BTreeMap<String, String> = serde_json::from_str(&row.headers)
            .map_err(|err| StoreError::Parse(format!("invalid headers JSON: {err}")))?;
        let lease_expires_at = row
            .lease_expires_at
            .ok_or_else(|| StoreError::Parse("missing lease_expires_at".to_string()))?;

        let event = WebhookEvent {
            id: Uuid::parse_str(&row.id)
                .map_err(|err| StoreError::Parse(format!("invalid event id: {err}")))?,
            endpoint_id: Uuid::parse_str(&row.endpoint_id)
                .map_err(|err| StoreError::Parse(format!("invalid endpoint id: {err}")))?,
            provider: row.provider,
            headers,
            payload: row.payload,
            status,
            attempts: row.attempts,
            received_at: row.received_at,
            next_attempt_at: row.next_attempt_at,
            lease_expires_at: Some(lease_expires_at.clone()),
            leased_by: row.leased_by,
            last_error: row.last_error,
        };

        let circuit = match row.circuit_state.as_deref() {
            Some(state) => {
                let circuit_status = parse_circuit_status(state)?;
                let open_until = row.circuit_open_until.clone();
                let consecutive_failures = row.circuit_consecutive_failures.unwrap_or(0);
                let last_failure_at = row.circuit_last_failure_at.clone();
                Some(TargetCircuitState {
                    endpoint_id: Uuid::parse_str(&row.endpoint_id)
                        .map_err(|err| StoreError::Parse(format!("invalid endpoint id: {err}")))?,
                    state: circuit_status,
                    open_until,
                    consecutive_failures,
                    last_failure_at,
                })
            }
            None => None,
        };

        Ok(LeasedEvent {
            event,
            target_url: row.target_url,
            lease_expires_at,
            circuit,
        })
    }
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

fn parse_circuit_status(status: &str) -> Result<TargetCircuitStatus, StoreError> {
    match status {
        "closed" => Ok(TargetCircuitStatus::Closed),
        "open" => Ok(TargetCircuitStatus::Open),
        other => Err(StoreError::Parse(format!("unknown circuit status: {other}"))),
    }
}

#[derive(sqlx::FromRow)]
struct ReportEventRow {
    endpoint_id: String,
    attempts: i64,
    leased_by: Option<String>,
    lease_expires_at: Option<String>,
}

#[derive(sqlx::FromRow)]
struct CircuitRow {
    consecutive_failures: i64,
}

fn error_kind_to_str(kind: WebhookAttemptErrorKind) -> &'static str {
    match kind {
        WebhookAttemptErrorKind::Timeout => "timeout",
        WebhookAttemptErrorKind::Network => "network",
        WebhookAttemptErrorKind::InvalidResponse => "invalid_response",
        WebhookAttemptErrorKind::Unexpected => "unexpected",
    }
}

fn compute_cooldown_ms(config: &DispatcherConfig, consecutive_failures: i64) -> u64 {
    let threshold = config.circuit_failure_threshold as i64;
    if consecutive_failures < threshold {
        return 0;
    }

    let exponent = (consecutive_failures - threshold).max(0) as i32;
    let base = config.circuit_cooldown_base_ms as f64;
    let factor = config.circuit_cooldown_factor;
    let max_ms = config.circuit_cooldown_max_ms as f64;

    let cooldown = base * factor.powi(exponent);
    cooldown.min(max_ms).round() as u64
}

async fn update_circuit_on_failure(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    config: &DispatcherConfig,
    endpoint_id: &str,
    endpoint_uuid: Uuid,
    now: chrono::DateTime<Utc>,
    now_str: &str,
    retryable: bool,
) -> Result<Option<TargetCircuitState>, StoreError> {
    if !retryable {
        return Ok(None);
    }

    let row = sqlx::query_as::<_, CircuitRow>(
        r#"
        SELECT consecutive_failures
        FROM target_circuit_states
        WHERE endpoint_id = ?
        "#,
    )
    .bind(endpoint_id)
    .fetch_optional(&mut **tx)
    .await?;

    let current_failures = row.map(|row| row.consecutive_failures).unwrap_or(0);
    let consecutive_failures = current_failures + 1;
    let cooldown_ms = compute_cooldown_ms(config, consecutive_failures);
    let should_open = consecutive_failures >= config.circuit_failure_threshold as i64;
    let open_until = if should_open {
        Some(format_utc(now + Duration::milliseconds(cooldown_ms as i64)))
    } else {
        None
    };
    let state = if should_open {
        TargetCircuitStatus::Open
    } else {
        TargetCircuitStatus::Closed
    };

    sqlx::query(
        r#"
        INSERT INTO target_circuit_states (
            endpoint_id,
            state,
            open_until,
            consecutive_failures,
            last_failure_at
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(endpoint_id) DO UPDATE SET
            state = excluded.state,
            open_until = excluded.open_until,
            consecutive_failures = excluded.consecutive_failures,
            last_failure_at = excluded.last_failure_at
        "#,
    )
    .bind(endpoint_id)
    .bind(match state {
        TargetCircuitStatus::Closed => "closed",
        TargetCircuitStatus::Open => "open",
    })
    .bind(open_until.as_deref())
    .bind(consecutive_failures)
    .bind(now_str)
    .execute(&mut **tx)
    .await?;

    Ok(Some(TargetCircuitState {
        endpoint_id: endpoint_uuid,
        state,
        open_until,
        consecutive_failures,
        last_failure_at: Some(now_str.to_string()),
    }))
}

fn compute_next_attempt_at(now: chrono::DateTime<Utc>, attempt_no: i64) -> String {
    let attempt_no = attempt_no.max(1);
    let exponent = (attempt_no - 1).min(31) as u32;
    let delay_secs = (1u64 << exponent).min(3600);
    format_utc(now + Duration::seconds(delay_secs as i64))
}

fn normalize_rfc3339_utc(value: &str) -> Result<String, StoreError> {
    let parsed = chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|err| StoreError::Parse(format!("invalid next_attempt_at: {err}")))?;
    Ok(format_utc(parsed.with_timezone(&Utc)))
}

fn format_utc(dt: chrono::DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Secs, true)
}
