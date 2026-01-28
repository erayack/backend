use std::collections::BTreeMap;

use chrono::{Duration, Utc};
use receiver::{
    inspector::{ListEventsParams, StoreError, get_event, list_events},
    types::WebhookEventStatus,
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

async fn setup_db() -> TestDb {
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
        .expect("enable foreign keys");
    run_migrations(&mut conn).await.expect("run migrations");

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
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
        .expect("connect sqlite");

    TestDb {
        pool,
        _db_file: db_file,
    }
}

async fn run_migrations(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    let mut entries: Vec<_> = fs::read_dir("migrations")
        .map_err(sqlx::Error::Io)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|ext| ext.to_str()) == Some("sql"))
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let contents = fs::read_to_string(entry.path()).map_err(sqlx::Error::Io)?;
        for stmt in contents.split(';') {
            let stmt = stmt.trim();
            if !stmt.is_empty() {
                sqlx::query(stmt).execute(&mut *conn).await?;
            }
        }
    }
    Ok(())
}

async fn seed_endpoint(pool: &SqlitePool, target_url: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO endpoints (id, target_url) VALUES (?, ?)")
        .bind(id.to_string())
        .bind(target_url)
        .execute(pool)
        .await
        .expect("insert endpoint");
    id
}

async fn seed_event(
    pool: &SqlitePool,
    endpoint_id: Uuid,
    provider: &str,
    status: &str,
    received_at: &str,
) -> Uuid {
    let id = Uuid::new_v4();
    let headers = serde_json::to_string(&BTreeMap::<String, String>::new()).unwrap();
    sqlx::query(
        r#"
        INSERT INTO webhook_events (
            id, endpoint_id, provider, headers, payload,
            status, attempts, received_at, next_attempt_at,
            lease_expires_at, leased_by, last_error
        ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, NULL, NULL, NULL, NULL)
        "#,
    )
    .bind(id.to_string())
    .bind(endpoint_id.to_string())
    .bind(provider)
    .bind(&headers)
    .bind(r#"{"secret":"data"}"#)
    .bind(status)
    .bind(received_at)
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
        INSERT INTO target_circuit_states (endpoint_id, state, open_until, consecutive_failures, last_failure_at)
        VALUES (?, ?, ?, 3, NULL)
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
async fn list_events_returns_empty_when_no_events() {
    let db = setup_db().await;
    let params = ListEventsParams {
        limit: 50,
        before: None,
        status: None,
        endpoint_id: None,
        provider: None,
    };

    let result = list_events(&db.pool, &params).await.expect("list_events");

    assert!(result.events.is_empty());
    assert!(result.next_before.is_none());
}

#[tokio::test]
async fn list_events_returns_summary_without_payload() {
    let db = setup_db().await;
    let endpoint_id = seed_endpoint(&db.pool, "https://example.com/hook").await;
    let now = Utc::now().to_rfc3339();
    seed_event(&db.pool, endpoint_id, "stripe", "pending", &now).await;

    let params = ListEventsParams {
        limit: 50,
        before: None,
        status: None,
        endpoint_id: None,
        provider: None,
    };

    let result = list_events(&db.pool, &params).await.expect("list_events");

    assert_eq!(result.events.len(), 1);
    let item = &result.events[0];
    assert_eq!(item.event.provider, "stripe");
    assert_eq!(item.event.status, WebhookEventStatus::Pending);
}

#[tokio::test]
async fn list_events_joins_target_url() {
    let db = setup_db().await;
    let target = "https://custom.example.com/webhooks";
    let endpoint_id = seed_endpoint(&db.pool, target).await;
    let now = Utc::now().to_rfc3339();
    seed_event(&db.pool, endpoint_id, "stripe", "pending", &now).await;

    let params = ListEventsParams {
        limit: 50,
        before: None,
        status: None,
        endpoint_id: None,
        provider: None,
    };

    let result = list_events(&db.pool, &params).await.expect("list_events");

    assert_eq!(result.events[0].target_url, target);
}

#[tokio::test]
async fn list_events_joins_circuit_state() {
    let db = setup_db().await;
    let endpoint_id = seed_endpoint(&db.pool, "https://example.com/hook").await;
    let now = Utc::now().to_rfc3339();
    let open_until = (Utc::now() + Duration::hours(1)).to_rfc3339();
    seed_event(&db.pool, endpoint_id, "stripe", "pending", &now).await;
    seed_circuit_state(&db.pool, endpoint_id, "open", Some(&open_until)).await;

    let params = ListEventsParams {
        limit: 50,
        before: None,
        status: None,
        endpoint_id: None,
        provider: None,
    };

    let result = list_events(&db.pool, &params).await.expect("list_events");

    let circuit = result.events[0].circuit.as_ref().expect("circuit present");
    assert_eq!(circuit.endpoint_id, endpoint_id);
    assert_eq!(circuit.state, receiver::types::TargetCircuitStatus::Open);
    assert!(circuit.open_until.is_some());
}

#[tokio::test]
async fn list_events_circuit_none_when_missing() {
    let db = setup_db().await;
    let endpoint_id = seed_endpoint(&db.pool, "https://example.com/hook").await;
    let now = Utc::now().to_rfc3339();
    seed_event(&db.pool, endpoint_id, "stripe", "pending", &now).await;

    let params = ListEventsParams {
        limit: 50,
        before: None,
        status: None,
        endpoint_id: None,
        provider: None,
    };

    let result = list_events(&db.pool, &params).await.expect("list_events");

    assert!(result.events[0].circuit.is_none());
}

#[tokio::test]
async fn list_events_filters_by_status() {
    let db = setup_db().await;
    let endpoint_id = seed_endpoint(&db.pool, "https://example.com/hook").await;
    let now = Utc::now();
    seed_event(
        &db.pool,
        endpoint_id,
        "stripe",
        "pending",
        &now.to_rfc3339(),
    )
    .await;
    seed_event(
        &db.pool,
        endpoint_id,
        "stripe",
        "delivered",
        &(now - Duration::seconds(1)).to_rfc3339(),
    )
    .await;
    seed_event(
        &db.pool,
        endpoint_id,
        "stripe",
        "dead",
        &(now - Duration::seconds(2)).to_rfc3339(),
    )
    .await;

    let params = ListEventsParams {
        limit: 50,
        before: None,
        status: Some(WebhookEventStatus::Delivered),
        endpoint_id: None,
        provider: None,
    };

    let result = list_events(&db.pool, &params).await.expect("list_events");

    assert_eq!(result.events.len(), 1);
    assert_eq!(result.events[0].event.status, WebhookEventStatus::Delivered);
}

#[tokio::test]
async fn list_events_filters_by_endpoint_id() {
    let db = setup_db().await;
    let endpoint_a = seed_endpoint(&db.pool, "https://a.example.com").await;
    let endpoint_b = seed_endpoint(&db.pool, "https://b.example.com").await;
    let now = Utc::now().to_rfc3339();
    seed_event(&db.pool, endpoint_a, "stripe", "pending", &now).await;
    seed_event(&db.pool, endpoint_b, "stripe", "pending", &now).await;

    let params = ListEventsParams {
        limit: 50,
        before: None,
        status: None,
        endpoint_id: Some(endpoint_a),
        provider: None,
    };

    let result = list_events(&db.pool, &params).await.expect("list_events");

    assert_eq!(result.events.len(), 1);
    assert_eq!(result.events[0].event.endpoint_id, endpoint_a);
}

#[tokio::test]
async fn list_events_filters_by_provider() {
    let db = setup_db().await;
    let endpoint_id = seed_endpoint(&db.pool, "https://example.com/hook").await;
    let now = Utc::now();
    seed_event(
        &db.pool,
        endpoint_id,
        "stripe",
        "pending",
        &now.to_rfc3339(),
    )
    .await;
    seed_event(
        &db.pool,
        endpoint_id,
        "github",
        "pending",
        &(now - Duration::seconds(1)).to_rfc3339(),
    )
    .await;

    let params = ListEventsParams {
        limit: 50,
        before: None,
        status: None,
        endpoint_id: None,
        provider: Some("github".to_string()),
    };

    let result = list_events(&db.pool, &params).await.expect("list_events");

    assert_eq!(result.events.len(), 1);
    assert_eq!(result.events[0].event.provider, "github");
}

#[tokio::test]
async fn list_events_respects_limit() {
    let db = setup_db().await;
    let endpoint_id = seed_endpoint(&db.pool, "https://example.com/hook").await;
    let now = Utc::now();
    for i in 0..5 {
        let ts = (now - Duration::seconds(i)).to_rfc3339();
        seed_event(&db.pool, endpoint_id, "stripe", "pending", &ts).await;
    }

    let params = ListEventsParams {
        limit: 3,
        before: None,
        status: None,
        endpoint_id: None,
        provider: None,
    };

    let result = list_events(&db.pool, &params).await.expect("list_events");

    assert_eq!(result.events.len(), 3);
    assert!(result.next_before.is_some());
}

#[tokio::test]
async fn list_events_cursor_pagination() {
    let db = setup_db().await;
    let endpoint_id = seed_endpoint(&db.pool, "https://example.com/hook").await;
    let now = Utc::now();
    let mut ids = Vec::new();
    for i in 0..5 {
        let ts = (now - Duration::seconds(i)).to_rfc3339();
        let id = seed_event(&db.pool, endpoint_id, "stripe", "pending", &ts).await;
        ids.push(id);
    }

    let first_page = list_events(
        &db.pool,
        &ListEventsParams {
            limit: 2,
            before: None,
            status: None,
            endpoint_id: None,
            provider: None,
        },
    )
    .await
    .expect("first page");

    assert_eq!(first_page.events.len(), 2);
    assert_eq!(first_page.events[0].event.id, ids[0]);
    assert_eq!(first_page.events[1].event.id, ids[1]);

    let cursor = first_page.next_before.expect("cursor present");

    let second_page = list_events(
        &db.pool,
        &ListEventsParams {
            limit: 2,
            before: Some(cursor),
            status: None,
            endpoint_id: None,
            provider: None,
        },
    )
    .await
    .expect("second page");

    assert_eq!(second_page.events.len(), 2);
    assert_eq!(second_page.events[0].event.id, ids[2]);
    assert_eq!(second_page.events[1].event.id, ids[3]);
}

#[tokio::test]
async fn list_events_ordering_desc() {
    let db = setup_db().await;
    let endpoint_id = seed_endpoint(&db.pool, "https://example.com/hook").await;
    let now = Utc::now();
    let oldest = seed_event(
        &db.pool,
        endpoint_id,
        "stripe",
        "pending",
        &(now - Duration::seconds(2)).to_rfc3339(),
    )
    .await;
    let middle = seed_event(
        &db.pool,
        endpoint_id,
        "stripe",
        "pending",
        &(now - Duration::seconds(1)).to_rfc3339(),
    )
    .await;
    let newest = seed_event(
        &db.pool,
        endpoint_id,
        "stripe",
        "pending",
        &now.to_rfc3339(),
    )
    .await;

    let params = ListEventsParams {
        limit: 50,
        before: None,
        status: None,
        endpoint_id: None,
        provider: None,
    };

    let result = list_events(&db.pool, &params).await.expect("list_events");

    assert_eq!(result.events[0].event.id, newest);
    assert_eq!(result.events[1].event.id, middle);
    assert_eq!(result.events[2].event.id, oldest);
}

#[tokio::test]
async fn list_events_cursor_stability() {
    let db = setup_db().await;
    let endpoint_id = seed_endpoint(&db.pool, "https://example.com/hook").await;
    let now = Utc::now();
    for i in 0..4 {
        let ts = (now - Duration::seconds(i)).to_rfc3339();
        seed_event(&db.pool, endpoint_id, "stripe", "pending", &ts).await;
    }

    let first_run = list_events(
        &db.pool,
        &ListEventsParams {
            limit: 2,
            before: None,
            status: None,
            endpoint_id: None,
            provider: None,
        },
    )
    .await
    .expect("first run");

    let cursor = first_run.next_before.clone().expect("cursor");

    let second_run = list_events(
        &db.pool,
        &ListEventsParams {
            limit: 2,
            before: Some(cursor.clone()),
            status: None,
            endpoint_id: None,
            provider: None,
        },
    )
    .await
    .expect("second run");

    let third_run = list_events(
        &db.pool,
        &ListEventsParams {
            limit: 2,
            before: Some(cursor),
            status: None,
            endpoint_id: None,
            provider: None,
        },
    )
    .await
    .expect("third run");

    assert_eq!(second_run.events.len(), third_run.events.len());
    for (a, b) in second_run.events.iter().zip(third_run.events.iter()) {
        assert_eq!(a.event.id, b.event.id);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// get_event tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn get_event_returns_full_payload() {
    let db = setup_db().await;
    let endpoint_id = seed_endpoint(&db.pool, "https://example.com/hook").await;
    let now = Utc::now().to_rfc3339();
    let event_id = seed_event(&db.pool, endpoint_id, "stripe", "pending", &now).await;

    let result = get_event(&db.pool, event_id).await.expect("get_event");

    assert_eq!(result.event.id, event_id);
    assert_eq!(result.event.provider, "stripe");
    assert_eq!(result.event.payload, r#"{"secret":"data"}"#);
    assert!(result.event.headers.is_empty());
}

#[tokio::test]
async fn get_event_not_found() {
    let db = setup_db().await;
    let non_existent_id = Uuid::new_v4();

    let result = get_event(&db.pool, non_existent_id).await;

    assert!(matches!(result, Err(StoreError::NotFound(_))));
}

#[tokio::test]
async fn get_event_includes_circuit_state() {
    let db = setup_db().await;
    let endpoint_id = seed_endpoint(&db.pool, "https://example.com/hook").await;
    let now = Utc::now().to_rfc3339();
    let open_until = (Utc::now() + Duration::hours(1)).to_rfc3339();
    let event_id = seed_event(&db.pool, endpoint_id, "stripe", "pending", &now).await;
    seed_circuit_state(&db.pool, endpoint_id, "open", Some(&open_until)).await;

    let result = get_event(&db.pool, event_id).await.expect("get_event");

    let circuit = result.circuit.expect("circuit present");
    assert_eq!(circuit.endpoint_id, endpoint_id);
    assert_eq!(circuit.state, receiver::types::TargetCircuitStatus::Open);
    assert!(circuit.open_until.is_some());
}

#[tokio::test]
async fn get_event_circuit_none_when_missing() {
    let db = setup_db().await;
    let endpoint_id = seed_endpoint(&db.pool, "https://example.com/hook").await;
    let now = Utc::now().to_rfc3339();
    let event_id = seed_event(&db.pool, endpoint_id, "stripe", "pending", &now).await;

    let result = get_event(&db.pool, event_id).await.expect("get_event");

    assert!(result.circuit.is_none());
}
