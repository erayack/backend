use std::collections::{BTreeMap, HashSet};

use chrono::{Duration, Utc};
use receiver::{
    dispatcher::{lease_events, report_delivery, DispatcherConfig},
    types::{LeaseRequest, ReportAttempt, ReportOutcome, ReportRequest, WebhookEventStatus},
};
use sqlx::{
    Connection, SqliteConnection, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use std::fs;
use tempfile::NamedTempFile;
use uuid::Uuid;

struct TestDb {
    pool: SqlitePool,
    _db_file: NamedTempFile,
}

async fn setup_db_shared(max_connections: u32) -> TestDb {
    let db_file = NamedTempFile::new().expect("create temp sqlite file");
    let options = SqliteConnectOptions::new()
        .filename(db_file.path())
        .create_if_missing(true)
        .busy_timeout(std::time::Duration::from_millis(500));

    let mut conn = SqliteConnection::connect_with(&options)
        .await
        .expect("connect sqlite for migrations");
    sqlx::query("PRAGMA foreign_keys = ON;")
        .execute(&mut conn)
        .await
        .expect("enable foreign keys for migrations");
    run_migrations_on_conn(&mut conn)
        .await
        .expect("run migrations");

    let pool = SqlitePoolOptions::new()
        .max_connections(max_connections)
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query("PRAGMA foreign_keys = ON;")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await
        .expect("connect sqlite file");

    TestDb {
        pool,
        _db_file: db_file,
    }
}

async fn run_migrations_on_conn(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    let mut entries: Vec<_> = fs::read_dir("migrations")
        .map_err(|err| sqlx::Error::Io(err))?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("sql"))
        .collect();

    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let contents = fs::read_to_string(entry.path()).map_err(|err| sqlx::Error::Io(err))?;
        for statement in contents.split(';') {
            let statement = statement.trim();
            if statement.is_empty() {
                continue;
            }
            sqlx::query(statement).execute(&mut *conn).await?;
        }
    }

    Ok(())
}

async fn seed_endpoint(pool: &SqlitePool) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO endpoints (id, target_url) VALUES (?, ?)")
        .bind(id.to_string())
        .bind("https://example.com/webhook")
        .execute(pool)
        .await
        .expect("insert endpoint");

    id
}

async fn seed_event(
    pool: &SqlitePool,
    endpoint_id: Uuid,
    status: &str,
    next_attempt_at: Option<&str>,
    lease_expires_at: Option<&str>,
    leased_by: Option<&str>,
) -> Uuid {
    seed_event_with_attempts(pool, endpoint_id, status, next_attempt_at, lease_expires_at, leased_by, 0).await
}

async fn seed_event_with_attempts(
    pool: &SqlitePool,
    endpoint_id: Uuid,
    status: &str,
    next_attempt_at: Option<&str>,
    lease_expires_at: Option<&str>,
    leased_by: Option<&str>,
    attempts: i64,
) -> Uuid {
    let id = Uuid::new_v4();
    let headers =
        serde_json::to_string(&BTreeMap::<String, String>::new()).expect("serialize headers");
    let received_at = Utc::now().to_rfc3339();

    sqlx::query(
        r#"
        INSERT INTO webhook_events (
            id,
            endpoint_id,
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(id.to_string())
    .bind(endpoint_id.to_string())
    .bind("stripe")
    .bind(headers)
    .bind("{}")
    .bind(status)
    .bind(attempts)
    .bind(received_at)
    .bind(next_attempt_at)
    .bind(lease_expires_at)
    .bind(leased_by)
    .bind(None::<String>)
    .execute(pool)
    .await
    .expect("insert event");

    id
}

async fn seed_circuit_state(
    pool: &SqlitePool,
    endpoint_id: Uuid,
    state: &str,
    open_until: Option<&str>,
) {
    sqlx::query(
        r#"
        INSERT INTO target_circuit_states (
            endpoint_id,
            state,
            open_until,
            consecutive_failures,
            last_failure_at
        )
        VALUES (?, ?, ?, 0, NULL)
        "#,
    )
    .bind(endpoint_id.to_string())
    .bind(state)
    .bind(open_until)
    .execute(pool)
    .await
    .expect("insert circuit state");
}

#[tokio::test]
async fn lease_eligibility_filter() {
    let test_db = setup_db_shared(1).await;
    let pool = test_db.pool;
    let endpoint_id = seed_endpoint(&pool).await;

    let now = Utc::now();
    let past = (now - Duration::hours(1)).to_rfc3339();
    let future = (now + Duration::hours(1)).to_rfc3339();

    let eligible_pending = seed_event(&pool, endpoint_id, "pending", None, None, None).await;
    let eligible_requeued =
        seed_event(&pool, endpoint_id, "requeued", Some(&past), None, None).await;
    let _ineligible_future =
        seed_event(&pool, endpoint_id, "pending", Some(&future), None, None).await;
    let _ineligible_in_flight = seed_event(
        &pool,
        endpoint_id,
        "in_flight",
        None,
        Some(&future),
        Some("other-worker"),
    )
    .await;

    let req = LeaseRequest {
        limit: 50,
        lease_ms: 30_000,
        worker_id: "worker-1".to_string(),
    };

    let events = lease_events(&pool, &req).await.expect("lease events");

    let returned_ids: HashSet<Uuid> = events.iter().map(|event| event.event.id).collect();
    let expected_ids: HashSet<Uuid> = [eligible_pending, eligible_requeued].into_iter().collect();

    assert_eq!(returned_ids, expected_ids);

    for leased in events {
        assert_eq!(leased.event.status, WebhookEventStatus::InFlight);
        assert_eq!(leased.event.leased_by.as_deref(), Some("worker-1"));
        assert!(leased.event.lease_expires_at.is_some());
    }
}

#[tokio::test]
async fn target_url_join() {
    let test_db = setup_db_shared(1).await;
    let pool = test_db.pool;

    let endpoint_id = Uuid::new_v4();
    let known_target_url = "https://custom-target.example.com/hooks";
    sqlx::query("INSERT INTO endpoints (id, target_url) VALUES (?, ?)")
        .bind(endpoint_id.to_string())
        .bind(known_target_url)
        .execute(&pool)
        .await
        .expect("insert endpoint");

    let _event_id = seed_event(&pool, endpoint_id, "pending", None, None, None).await;

    let req = LeaseRequest {
        limit: 10,
        lease_ms: 30_000,
        worker_id: "worker-1".to_string(),
    };

    let events = lease_events(&pool, &req).await.expect("lease events");

    assert_eq!(events.len(), 1, "should lease exactly one event");
    assert_eq!(
        events[0].target_url, known_target_url,
        "leased event should include correct target_url from endpoints table"
    );
}

#[tokio::test]
async fn expired_lease_recovery() {
    let test_db = setup_db_shared(1).await;
    let pool = test_db.pool;
    let endpoint_id = seed_endpoint(&pool).await;

    let now = Utc::now();
    let past = (now - Duration::hours(1)).to_rfc3339();

    let expired_id = seed_event(
        &pool,
        endpoint_id,
        "in_flight",
        None,
        Some(&past),
        Some("worker-old"),
    )
    .await;

    let req = LeaseRequest {
        limit: 10,
        lease_ms: 30_000,
        worker_id: "worker-new".to_string(),
    };

    let events = lease_events(&pool, &req).await.expect("lease events");

    assert_eq!(events.len(), 1);
    let leased = &events[0];
    assert_eq!(leased.event.id, expired_id);
    assert_eq!(leased.event.status, WebhookEventStatus::InFlight);
    assert_eq!(leased.event.leased_by.as_deref(), Some("worker-new"));

    let new_lease_expires_at = leased
        .event
        .lease_expires_at
        .as_deref()
        .expect("lease_expires_at set");
    assert_ne!(new_lease_expires_at, past);
    let parsed =
        chrono::DateTime::parse_from_rfc3339(new_lease_expires_at).expect("parse lease_expires_at");
    assert!(parsed.with_timezone(&Utc) > now);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_exclusivity_no_duplicate_leases() {
    let test_db = setup_db_shared(2).await;
    let pool = test_db.pool;
    let endpoint_id = seed_endpoint(&pool).await;

    let now = Utc::now().to_rfc3339();
    let total_events = 10;
    for _ in 0..total_events {
        seed_event(&pool, endpoint_id, "pending", Some(&now), None, None).await;
    }

    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));

    let req_a = LeaseRequest {
        limit: 6,
        lease_ms: 30_000,
        worker_id: "worker-a".to_string(),
    };
    let req_b = LeaseRequest {
        limit: 6,
        lease_ms: 30_000,
        worker_id: "worker-b".to_string(),
    };

    let barrier_a = barrier.clone();
    let barrier_b = barrier.clone();

    let (events_a, events_b) = tokio::join!(
        async {
            barrier_a.wait().await;
            lease_events(&pool, &req_a).await.expect("lease events a")
        },
        async {
            barrier_b.wait().await;
            lease_events(&pool, &req_b).await.expect("lease events b")
        }
    );

    let ids_a: HashSet<Uuid> = events_a.iter().map(|event| event.event.id).collect();
    let ids_b: HashSet<Uuid> = events_b.iter().map(|event| event.event.id).collect();

    let combined: HashSet<Uuid> = ids_a.union(&ids_b).copied().collect();
    let combined_len = combined.len();
    let total_returned = events_a.len() + events_b.len();

    assert_eq!(
        combined_len, total_returned,
        "duplicate leases detected across concurrent calls"
    );

    let expected_max = std::cmp::min(total_events as usize, req_a.limit as usize * 2);
    assert_eq!(combined_len, expected_max);
}

#[tokio::test]
async fn circuit_gating() {
    let test_db = setup_db_shared(1).await;
    let pool = test_db.pool;
    let endpoint_id = seed_endpoint(&pool).await;

    let now = Utc::now();
    let event_id = seed_event(&pool, endpoint_id, "pending", None, None, None).await;

    // Phase 1: Circuit is open, event should not be leased
    let open_until = (now + Duration::hours(1)).to_rfc3339();
    seed_circuit_state(&pool, endpoint_id, "open", Some(&open_until)).await;

    let req = LeaseRequest {
        limit: 50,
        lease_ms: 30_000,
        worker_id: "worker-1".to_string(),
    };

    let events = lease_events(&pool, &req).await.expect("lease events");

    assert!(
        events.is_empty(),
        "no events should be leased when circuit is open"
    );

    // Phase 2: Update circuit state to closed (open_until in past), event should now be leaseable
    let closed_time = (now - Duration::seconds(1)).to_rfc3339();
    sqlx::query(
        r#"
        UPDATE target_circuit_states
        SET open_until = ?
        WHERE endpoint_id = ?
        "#,
    )
    .bind(&closed_time)
    .bind(endpoint_id.to_string())
    .execute(&pool)
    .await
    .expect("update circuit state");

    let events = lease_events(&pool, &req)
        .await
        .expect("lease events second call");

    assert_eq!(
        events.len(),
        1,
        "event should be leased when circuit is closed"
    );

    let leased = &events[0];
    assert_eq!(leased.event.id, event_id);
    assert_eq!(leased.event.status, WebhookEventStatus::InFlight);
    assert_eq!(
        leased.event.leased_by.as_deref(),
        Some("worker-1"),
        "event should be leased by worker-1"
    );
    assert!(
        leased.event.lease_expires_at.is_some(),
        "lease_expires_at should be set"
    );
    assert_eq!(
        leased.target_url, "https://example.com/webhook",
        "target_url should be included in lease response"
    );
}

#[tokio::test]
async fn report_happy_path_delivered() {
    let test_db = setup_db_shared(1).await;
    let pool = test_db.pool;
    let endpoint_id = seed_endpoint(&pool).await;

    // Stage 2: Seed leased event
    let now = Utc::now();
    let lease_expires_at = (now + Duration::hours(1)).to_rfc3339();
    let started_at = (now - Duration::seconds(5)).to_rfc3339();
    let finished_at = now.to_rfc3339();

    let event_id = seed_event(
        &pool,
        endpoint_id,
        "in_flight",
        None,
        Some(&lease_expires_at),
        Some("test-worker"),
    )
    .await;

    // Stage 3: Build ReportRequest
    let report_req = ReportRequest {
        worker_id: "test-worker".to_string(),
        event_id,
        outcome: ReportOutcome::Delivered,
        retryable: true,
        next_attempt_at: None,
        attempt: ReportAttempt {
            started_at,
            finished_at,
            request_headers: BTreeMap::new(),
            request_body: r#"{"test":"payload"}"#.to_string(),
            response_status: Some(200),
            response_headers: Some(BTreeMap::from([(
                "content-type".to_string(),
                "application/json".to_string(),
            )])),
            response_body: Some(r#"{"status":"ok"}"#.to_string()),
            error_kind: None,
            error_message: None,
        },
    };

    // Stage 4: Invoke report_delivery
    let config = DispatcherConfig::default();
    let result = report_delivery(&pool, &config, &report_req).await;
    assert!(result.is_ok(), "report_delivery should succeed");

    // Stage 5: Assertion Group 1 - Attempt Log Inserted
    let attempt_row = sqlx::query_as::<_, (i64, String, Option<i64>, Option<String>)>(
        r#"
        SELECT attempt_no, request_body, response_status, response_body
        FROM webhook_attempt_logs
        WHERE event_id = ?
        "#,
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("attempt log should exist");

    assert_eq!(attempt_row.0, 1, "attempt_no should be 1 (first attempt)");
    assert_eq!(
        attempt_row.1, r#"{"test":"payload"}"#,
        "request_body should match sent payload"
    );
    assert_eq!(attempt_row.2, Some(200), "response_status should be 200");
    assert_eq!(
        attempt_row.3,
        Some(r#"{"status":"ok"}"#.to_string()),
        "response_body should match sent response"
    );

    // Stage 6: Assertion Group 2 - Event Status = Delivered
    let event_row = sqlx::query_as::<_, (String, i64, String)>(
        r#"
        SELECT status, attempts, endpoint_id
        FROM webhook_events
        WHERE id = ?
        "#,
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("event should exist");

    assert_eq!(
        event_row.0, "delivered",
        "event status should be delivered"
    );
    assert_eq!(event_row.1, 1, "attempts should be incremented to 1");
    assert_eq!(
        event_row.2, endpoint_id.to_string(),
        "endpoint_id should remain unchanged"
    );

    // Stage 7: Assertion Group 3 - Lease Cleared
    let lease_fields = sqlx::query_as::<_, (Option<String>, Option<String>)>(
        r#"
        SELECT lease_expires_at, leased_by
        FROM webhook_events
        WHERE id = ?
        "#,
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("event should exist");

    assert!(
        lease_fields.0.is_none(),
        "lease_expires_at should be cleared (NULL)"
    );
    assert!(lease_fields.1.is_none(), "leased_by should be cleared (NULL)");

    // Stage 8: Assertion Group 4 - last_error Cleared
    let last_error = sqlx::query_scalar::<_, Option<String>>(
        r#"
        SELECT last_error
        FROM webhook_events
        WHERE id = ?
        "#,
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("event should exist");

    assert!(
        last_error.is_none(),
        "last_error should be cleared (NULL) on successful delivery"
    );

    // Stage 9 (Optional): Verify circuit state closed if it exists
    let circuit_state = sqlx::query_as::<_, (String, Option<String>, i64)>(
        r#"
        SELECT state, open_until, consecutive_failures
        FROM target_circuit_states
        WHERE endpoint_id = ?
        "#,
    )
    .bind(endpoint_id.to_string())
    .fetch_optional(&pool)
    .await
    .expect("query circuit state");

    if let Some(state) = circuit_state {
        assert_eq!(
            state.0, "closed",
            "circuit should be closed on successful delivery"
        );
        assert!(state.1.is_none(), "open_until should be NULL");
        assert_eq!(
            state.2, 0,
            "consecutive_failures should reset to 0 on success"
        );
    }
}

#[tokio::test]
async fn report_retry_path() {
    let test_db = setup_db_shared(1).await;
    let pool = test_db.pool;
    let endpoint_id = seed_endpoint(&pool).await;

    let now = Utc::now();
    let lease_expires_at = (now + Duration::hours(1)).to_rfc3339();
    let started_at = (now - Duration::seconds(5)).to_rfc3339();
    let finished_at = now.to_rfc3339();
    let next_attempt_at = (now + Duration::minutes(5)).to_rfc3339();

    let event_id = seed_event(
        &pool,
        endpoint_id,
        "in_flight",
        None,
        Some(&lease_expires_at),
        Some("test-worker"),
    )
    .await;

    let report_req = ReportRequest {
        worker_id: "test-worker".to_string(),
        event_id,
        outcome: ReportOutcome::Retry,
        retryable: true,
        next_attempt_at: Some(next_attempt_at.clone()),
        attempt: ReportAttempt {
            started_at,
            finished_at,
            request_headers: BTreeMap::new(),
            request_body: r#"{"test":"payload"}"#.to_string(),
            response_status: Some(503),
            response_headers: None,
            response_body: Some("Service Unavailable".to_string()),
            error_kind: None,
            error_message: Some("Connection timed out".to_string()),
        },
    };

    let config = DispatcherConfig::default();
    let result = report_delivery(&pool, &config, &report_req).await;
    assert!(result.is_ok(), "report_delivery should succeed");

    // Assertion 1: Attempt log inserted
    let attempt_row = sqlx::query_as::<_, (i64, String, Option<String>)>(
        r#"
        SELECT attempt_no, request_body, error_message
        FROM webhook_attempt_logs
        WHERE event_id = ?
        "#,
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("attempt log should exist");

    assert_eq!(attempt_row.0, 1, "attempt_no should be 1");
    assert_eq!(attempt_row.1, r#"{"test":"payload"}"#);
    assert_eq!(
        attempt_row.2,
        Some("Connection timed out".to_string()),
        "error_message should be persisted"
    );

    // Assertion 2: Event status = pending
    let event_row = sqlx::query_as::<_, (String, i64)>(
        r#"
        SELECT status, attempts
        FROM webhook_events
        WHERE id = ?
        "#,
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("event should exist");

    assert_eq!(event_row.0, "pending", "event status should be pending");
    assert_eq!(event_row.1, 1, "attempts should be incremented to 1");

    // Assertion 3: next_attempt_at persisted
    let persisted_next_attempt = sqlx::query_scalar::<_, Option<String>>(
        r#"
        SELECT next_attempt_at
        FROM webhook_events
        WHERE id = ?
        "#,
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("event should exist");

    let expected_next_attempt = chrono::DateTime::parse_from_rfc3339(&next_attempt_at)
        .expect("next_attempt_at should be RFC3339")
        .with_timezone(&chrono::Utc)
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    assert_eq!(
        persisted_next_attempt,
        Some(expected_next_attempt),
        "next_attempt_at should be persisted"
    );

    // Assertion 4: Lease cleared
    let lease_fields = sqlx::query_as::<_, (Option<String>, Option<String>)>(
        r#"
        SELECT lease_expires_at, leased_by
        FROM webhook_events
        WHERE id = ?
        "#,
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("event should exist");

    assert!(lease_fields.0.is_none(), "lease_expires_at should be cleared");
    assert!(lease_fields.1.is_none(), "leased_by should be cleared");

    // Assertion 5: last_error set
    let last_error = sqlx::query_scalar::<_, Option<String>>(
        r#"
        SELECT last_error
        FROM webhook_events
        WHERE id = ?
        "#,
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("event should exist");

    assert_eq!(
        last_error,
        Some("Connection timed out".to_string()),
        "last_error should be set"
    );
}

#[tokio::test]
async fn report_lease_ownership_conflict_wrong_worker() {
    let test_db = setup_db_shared(1).await;
    let pool = test_db.pool;
    let endpoint_id = seed_endpoint(&pool).await;

    let now = Utc::now();
    let lease_expires_at = (now + Duration::hours(1)).to_rfc3339();

    let event_id = seed_event(
        &pool,
        endpoint_id,
        "in_flight",
        None,
        Some(&lease_expires_at),
        Some("original-worker"),
    )
    .await;

    let report_req = ReportRequest {
        worker_id: "wrong-worker".to_string(),
        event_id,
        outcome: ReportOutcome::Delivered,
        retryable: true,
        next_attempt_at: None,
        attempt: ReportAttempt {
            started_at: now.to_rfc3339(),
            finished_at: now.to_rfc3339(),
            request_headers: BTreeMap::new(),
            request_body: "{}".to_string(),
            response_status: Some(200),
            response_headers: None,
            response_body: None,
            error_kind: None,
            error_message: None,
        },
    };

    let config = DispatcherConfig::default();
    let result = report_delivery(&pool, &config, &report_req).await;
    assert!(result.is_err(), "report should fail with conflict");

    let attempt_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM webhook_attempt_logs WHERE event_id = ?",
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("count attempts");
    assert_eq!(attempt_count, 0, "no attempt log should be inserted");

    let event_row = sqlx::query_as::<_, (String, Option<String>, Option<String>)>(
        "SELECT status, leased_by, lease_expires_at FROM webhook_events WHERE id = ?",
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("event should exist");

    assert_eq!(event_row.0, "in_flight", "status should be unchanged");
    assert_eq!(
        event_row.1.as_deref(),
        Some("original-worker"),
        "leased_by should be unchanged"
    );
    assert!(event_row.2.is_some(), "lease_expires_at should be unchanged");
}

#[tokio::test]
async fn report_lease_ownership_conflict_expired_lease() {
    let test_db = setup_db_shared(1).await;
    let pool = test_db.pool;
    let endpoint_id = seed_endpoint(&pool).await;

    let now = Utc::now();
    let expired_lease = (now - Duration::hours(1)).to_rfc3339();

    let event_id = seed_event(
        &pool,
        endpoint_id,
        "in_flight",
        None,
        Some(&expired_lease),
        Some("test-worker"),
    )
    .await;

    let report_req = ReportRequest {
        worker_id: "test-worker".to_string(),
        event_id,
        outcome: ReportOutcome::Delivered,
        retryable: true,
        next_attempt_at: None,
        attempt: ReportAttempt {
            started_at: now.to_rfc3339(),
            finished_at: now.to_rfc3339(),
            request_headers: BTreeMap::new(),
            request_body: "{}".to_string(),
            response_status: Some(200),
            response_headers: None,
            response_body: None,
            error_kind: None,
            error_message: None,
        },
    };

    let config = DispatcherConfig::default();
    let result = report_delivery(&pool, &config, &report_req).await;
    assert!(result.is_err(), "report should fail with conflict for expired lease");

    let attempt_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM webhook_attempt_logs WHERE event_id = ?",
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("count attempts");
    assert_eq!(attempt_count, 0, "no attempt log should be inserted");

    let event_row = sqlx::query_as::<_, (String, Option<String>, Option<String>)>(
        "SELECT status, leased_by, lease_expires_at FROM webhook_events WHERE id = ?",
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("event should exist");

    assert_eq!(event_row.0, "in_flight", "status should be unchanged");
    assert_eq!(
        event_row.1.as_deref(),
        Some("test-worker"),
        "leased_by should be unchanged"
    );
    assert!(event_row.2.is_some(), "lease_expires_at should be unchanged");
}

#[tokio::test]
async fn report_max_attempts_forces_dead() {
    let test_db = setup_db_shared(1).await;
    let pool = test_db.pool;
    let endpoint_id = seed_endpoint(&pool).await;

    let now = Utc::now();
    let lease_expires_at = (now + Duration::hours(1)).to_rfc3339();

    let event_id = seed_event_with_attempts(
        &pool,
        endpoint_id,
        "in_flight",
        None,
        Some(&lease_expires_at),
        Some("test-worker"),
        4,
    )
    .await;

    let config = DispatcherConfig {
        max_attempts: 5,
        ..Default::default()
    };

    let report_req = ReportRequest {
        worker_id: "test-worker".to_string(),
        event_id,
        outcome: ReportOutcome::Retry,
        retryable: true,
        next_attempt_at: Some((now + Duration::minutes(5)).to_rfc3339()),
        attempt: ReportAttempt {
            started_at: now.to_rfc3339(),
            finished_at: now.to_rfc3339(),
            request_headers: BTreeMap::new(),
            request_body: "{}".to_string(),
            response_status: Some(503),
            response_headers: None,
            response_body: Some("Service Unavailable".to_string()),
            error_kind: None,
            error_message: Some("Connection timed out".to_string()),
        },
    };

    let result = report_delivery(&pool, &config, &report_req)
        .await
        .expect("report_delivery should succeed");

    assert_eq!(
        result.final_outcome,
        ReportOutcome::Dead,
        "final_outcome should be Dead when max_attempts exhausted"
    );

    let event_row = sqlx::query_as::<_, (String, i64, Option<String>)>(
        "SELECT status, attempts, last_error FROM webhook_events WHERE id = ?",
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("event should exist");

    assert_eq!(event_row.0, "dead", "status should be dead");
    assert_eq!(event_row.1, 5, "attempts should be 5");
    assert!(
        event_row.2.as_ref().unwrap().contains("max_attempts_exceeded"),
        "last_error should contain max_attempts_exceeded"
    );
}

#[tokio::test]
async fn report_under_max_attempts_preserves_retry() {
    let test_db = setup_db_shared(1).await;
    let pool = test_db.pool;
    let endpoint_id = seed_endpoint(&pool).await;

    let now = Utc::now();
    let lease_expires_at = (now + Duration::hours(1)).to_rfc3339();
    let next_attempt_at = (now + Duration::minutes(5)).to_rfc3339();

    let event_id = seed_event_with_attempts(
        &pool,
        endpoint_id,
        "in_flight",
        None,
        Some(&lease_expires_at),
        Some("test-worker"),
        2,
    )
    .await;

    let config = DispatcherConfig {
        max_attempts: 5,
        ..Default::default()
    };

    let report_req = ReportRequest {
        worker_id: "test-worker".to_string(),
        event_id,
        outcome: ReportOutcome::Retry,
        retryable: true,
        next_attempt_at: Some(next_attempt_at.clone()),
        attempt: ReportAttempt {
            started_at: now.to_rfc3339(),
            finished_at: now.to_rfc3339(),
            request_headers: BTreeMap::new(),
            request_body: "{}".to_string(),
            response_status: Some(503),
            response_headers: None,
            response_body: None,
            error_kind: None,
            error_message: Some("Server error".to_string()),
        },
    };

    let result = report_delivery(&pool, &config, &report_req)
        .await
        .expect("report_delivery should succeed");

    assert_eq!(
        result.final_outcome,
        ReportOutcome::Retry,
        "final_outcome should be Retry when under max_attempts"
    );

    let event_row = sqlx::query_as::<_, (String, i64)>(
        "SELECT status, attempts FROM webhook_events WHERE id = ?",
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("event should exist");

    assert_eq!(event_row.0, "pending", "status should be pending for retry");
    assert_eq!(event_row.1, 3, "attempts should be 3");
}

#[tokio::test]
async fn report_max_attempts_overrides_delivered() {
    let test_db = setup_db_shared(1).await;
    let pool = test_db.pool;
    let endpoint_id = seed_endpoint(&pool).await;

    let now = Utc::now();
    let lease_expires_at = (now + Duration::hours(1)).to_rfc3339();

    let event_id = seed_event_with_attempts(
        &pool,
        endpoint_id,
        "in_flight",
        None,
        Some(&lease_expires_at),
        Some("test-worker"),
        4,
    )
    .await;

    let config = DispatcherConfig {
        max_attempts: 5,
        ..Default::default()
    };

    let report_req = ReportRequest {
        worker_id: "test-worker".to_string(),
        event_id,
        outcome: ReportOutcome::Delivered,
        retryable: false,
        next_attempt_at: None,
        attempt: ReportAttempt {
            started_at: now.to_rfc3339(),
            finished_at: now.to_rfc3339(),
            request_headers: BTreeMap::new(),
            request_body: "{}".to_string(),
            response_status: Some(200),
            response_headers: None,
            response_body: Some(r#"{"ok":true}"#.to_string()),
            error_kind: None,
            error_message: None,
        },
    };

    let result = report_delivery(&pool, &config, &report_req)
        .await
        .expect("report_delivery should succeed");

    assert_eq!(
        result.final_outcome,
        ReportOutcome::Dead,
        "final_outcome should be Dead when max_attempts exhausted"
    );

    let event_row = sqlx::query_as::<_, (String, i64, Option<String>)>(
        "SELECT status, attempts, last_error FROM webhook_events WHERE id = ?",
    )
    .bind(event_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("event should exist");

    assert_eq!(event_row.0, "dead", "status should be dead");
    assert_eq!(event_row.1, 5, "attempts should be 5");
    assert!(
        event_row.2.as_ref().unwrap().contains("max_attempts_exceeded"),
        "last_error should contain max_attempts_exceeded"
    );
}

#[tokio::test]
async fn report_final_outcome_returned_in_result() {
    let test_db = setup_db_shared(1).await;
    let pool = test_db.pool;
    let endpoint_id = seed_endpoint(&pool).await;

    let now = Utc::now();
    let lease_expires_at = (now + Duration::hours(1)).to_rfc3339();

    let event_id = seed_event(&pool, endpoint_id, "in_flight", None, Some(&lease_expires_at), Some("worker")).await;

    let config = DispatcherConfig::default();
    let report_req = ReportRequest {
        worker_id: "worker".to_string(),
        event_id,
        outcome: ReportOutcome::Delivered,
        retryable: false,
        next_attempt_at: None,
        attempt: ReportAttempt {
            started_at: now.to_rfc3339(),
            finished_at: now.to_rfc3339(),
            request_headers: BTreeMap::new(),
            request_body: "{}".to_string(),
            response_status: Some(200),
            response_headers: None,
            response_body: None,
            error_kind: None,
            error_message: None,
        },
    };

    let result = report_delivery(&pool, &config, &report_req)
        .await
        .expect("report should succeed");

    assert_eq!(result.final_outcome, ReportOutcome::Delivered, "final_outcome should match reported outcome");
}
