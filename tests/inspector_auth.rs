#![allow(clippy::expect_used, clippy::unwrap_used)]

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header::AUTHORIZATION},
    middleware,
    routing::{get, post},
};
use http_body_util::BodyExt;
use receiver::{auth::inspector_auth, dispatcher::DispatcherConfig, state::AppState};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::fs;
use tempfile::NamedTempFile;
use tower::ServiceExt;

struct TestDb {
    pool: sqlx::SqlitePool,
    _db_file: NamedTempFile,
}

async fn setup_db() -> TestDb {
    let db_file = NamedTempFile::new().expect("create temp sqlite file");
    let options = SqliteConnectOptions::new()
        .filename(db_file.path())
        .create_if_missing(true)
        .busy_timeout(std::time::Duration::from_millis(500));

    let mut conn = sqlx::SqliteConnection::connect_with(&options)
        .await
        .expect("connect sqlite");
    sqlx::query("PRAGMA foreign_keys = ON;")
        .execute(&mut conn)
        .await
        .expect("enable foreign keys");

    let mut entries: Vec<_> = fs::read_dir("migrations")
        .expect("read migrations dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|ext| ext.to_str()) == Some("sql"))
        .collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let contents = fs::read_to_string(entry.path()).expect("read migration");
        for stmt in contents.split(';') {
            let stmt = stmt.trim();
            if !stmt.is_empty() {
                sqlx::query(stmt)
                    .execute(&mut conn)
                    .await
                    .expect("run migration");
            }
        }
    }

    use sqlx::Connection;
    conn.close().await.expect("close migration conn");

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("connect pool");

    TestDb {
        pool,
        _db_file: db_file,
    }
}

async fn dummy_handler() -> &'static str {
    "ok"
}

fn build_app(state: AppState) -> Router {
    let protected_router = Router::new().route("/protected", get(dummy_handler)).layer(
        middleware::from_fn_with_state(state.clone(), inspector_auth),
    );

    let unprotected_router = Router::new().route("/internal/dispatcher/lease", post(dummy_handler));

    Router::new()
        .merge(unprotected_router)
        .nest("/api/inspector", protected_router)
        .with_state(state)
}

async fn response_body(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// No token configured (feature disabled) - requests pass through
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn auth_disabled_allows_request_without_header() {
    let db = setup_db().await;
    let state = AppState {
        pool: db.pool,
        dispatcher: DispatcherConfig::default(),
        inspector_api_token: None,
    };
    let app = build_app(state);

    let request = Request::builder()
        .uri("/api/inspector/protected")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response_body(response).await, "ok");
}

#[tokio::test]
async fn auth_disabled_allows_request_with_any_header() {
    let db = setup_db().await;
    let state = AppState {
        pool: db.pool,
        dispatcher: DispatcherConfig::default(),
        inspector_api_token: None,
    };
    let app = build_app(state);

    let request = Request::builder()
        .uri("/api/inspector/protected")
        .header(AUTHORIZATION, "Bearer random-token")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ─────────────────────────────────────────────────────────────────────────────
// Token configured, valid Bearer header - request allowed
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn valid_bearer_token_allows_request() {
    let db = setup_db().await;
    let token = "secret-api-token";
    let state = AppState {
        pool: db.pool,
        dispatcher: DispatcherConfig::default(),
        inspector_api_token: Some(token.to_string()),
    };
    let app = build_app(state);

    let request = Request::builder()
        .uri("/api/inspector/protected")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response_body(response).await, "ok");
}

#[tokio::test]
async fn valid_bearer_token_with_trailing_whitespace_allows_request() {
    let db = setup_db().await;
    let token = "secret-api-token";
    let state = AppState {
        pool: db.pool,
        dispatcher: DispatcherConfig::default(),
        inspector_api_token: Some(token.to_string()),
    };
    let app = build_app(state);

    let request = Request::builder()
        .uri("/api/inspector/protected")
        .header(AUTHORIZATION, format!("Bearer {token}  "))
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ─────────────────────────────────────────────────────────────────────────────
// Token configured, missing Authorization header - 401
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn missing_auth_header_returns_401() {
    let db = setup_db().await;
    let state = AppState {
        pool: db.pool,
        dispatcher: DispatcherConfig::default(),
        inspector_api_token: Some("secret".to_string()),
    };
    let app = build_app(state);

    let request = Request::builder()
        .uri("/api/inspector/protected")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = response_body(response).await;
    assert!(body.contains("missing"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Token configured, invalid/wrong token - 401
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn wrong_token_returns_401() {
    let db = setup_db().await;
    let state = AppState {
        pool: db.pool,
        dispatcher: DispatcherConfig::default(),
        inspector_api_token: Some("correct-token".to_string()),
    };
    let app = build_app(state);

    let request = Request::builder()
        .uri("/api/inspector/protected")
        .header(AUTHORIZATION, "Bearer wrong-token")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = response_body(response).await;
    assert!(body.contains("invalid"));
}

#[tokio::test]
async fn empty_bearer_token_returns_401() {
    let db = setup_db().await;
    let state = AppState {
        pool: db.pool,
        dispatcher: DispatcherConfig::default(),
        inspector_api_token: Some("secret".to_string()),
    };
    let app = build_app(state);

    let request = Request::builder()
        .uri("/api/inspector/protected")
        .header(AUTHORIZATION, "Bearer ")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

// ─────────────────────────────────────────────────────────────────────────────
// Token configured, malformed header (not "Bearer ...") - 401
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn basic_auth_header_returns_401() {
    let db = setup_db().await;
    let state = AppState {
        pool: db.pool,
        dispatcher: DispatcherConfig::default(),
        inspector_api_token: Some("secret".to_string()),
    };
    let app = build_app(state);

    let request = Request::builder()
        .uri("/api/inspector/protected")
        .header(AUTHORIZATION, "Basic dXNlcjpwYXNz")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn token_without_bearer_prefix_returns_401() {
    let db = setup_db().await;
    let state = AppState {
        pool: db.pool,
        dispatcher: DispatcherConfig::default(),
        inspector_api_token: Some("secret".to_string()),
    };
    let app = build_app(state);

    let request = Request::builder()
        .uri("/api/inspector/protected")
        .header(AUTHORIZATION, "secret")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn lowercase_bearer_allows_request() {
    let db = setup_db().await;
    let state = AppState {
        pool: db.pool,
        dispatcher: DispatcherConfig::default(),
        inspector_api_token: Some("secret".to_string()),
    };
    let app = build_app(state);

    let request = Request::builder()
        .uri("/api/inspector/protected")
        .header(AUTHORIZATION, "bearer secret")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn mixed_case_bearer_allows_request() {
    let db = setup_db().await;
    let state = AppState {
        pool: db.pool,
        dispatcher: DispatcherConfig::default(),
        inspector_api_token: Some("secret".to_string()),
    };
    let app = build_app(state);

    let request = Request::builder()
        .uri("/api/inspector/protected")
        .header(AUTHORIZATION, "BeArEr secret")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn leading_whitespace_in_header_allows_request() {
    let db = setup_db().await;
    let state = AppState {
        pool: db.pool,
        dispatcher: DispatcherConfig::default(),
        inspector_api_token: Some("secret".to_string()),
    };
    let app = build_app(state);

    let request = Request::builder()
        .uri("/api/inspector/protected")
        .header(AUTHORIZATION, "  Bearer secret")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal dispatcher routes remain unaffected by auth
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn dispatcher_routes_unaffected_by_inspector_auth() {
    let db = setup_db().await;
    let state = AppState {
        pool: db.pool,
        dispatcher: DispatcherConfig::default(),
        inspector_api_token: Some("secret".to_string()),
    };
    let app = build_app(state);

    let request = Request::builder()
        .uri("/internal/dispatcher/lease")
        .method("POST")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ─────────────────────────────────────────────────────────────────────────────
// Timing attack resistance (basic sanity check)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn different_length_tokens_both_rejected() {
    let db = setup_db().await;
    let state = AppState {
        pool: db.pool.clone(),
        dispatcher: DispatcherConfig::default(),
        inspector_api_token: Some("a-very-long-secret-token-here".to_string()),
    };

    let app1 = build_app(state.clone());
    let request1 = Request::builder()
        .uri("/api/inspector/protected")
        .header(AUTHORIZATION, "Bearer x")
        .body(Body::empty())
        .unwrap();
    let response1 = app1.oneshot(request1).await.unwrap();
    assert_eq!(response1.status(), StatusCode::UNAUTHORIZED);

    let app2 = build_app(state);
    let request2 = Request::builder()
        .uri("/api/inspector/protected")
        .header(AUTHORIZATION, "Bearer a-very-long-secret-token-her")
        .body(Body::empty())
        .unwrap();
    let response2 = app2.oneshot(request2).await.unwrap();
    assert_eq!(response2.status(), StatusCode::UNAUTHORIZED);
}
