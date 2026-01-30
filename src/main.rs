use axum::{
    Router, middleware,
    routing::{get, post},
};
use receiver::{
    auth::inspector_auth,
    dispatcher::DispatcherConfig,
    handlers::{
        dispatcher::{lease_handler, report_handler},
        inspector::{
            get_event_handler, list_attempts_handler, list_events_handler, replay_event_handler,
        },
    },
    state::AppState,
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::net::SocketAddr;
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:receiver.db".to_string());
    let bind_addr = std::env::var("RECEIVER_INTERNAL_BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:3001".to_string());
    let inspector_api_token = std::env::var("INSPECTOR_API_TOKEN")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let connect_options = SqliteConnectOptions::from_str(&database_url)?.create_if_missing(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(connect_options)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    let dispatcher = DispatcherConfig::from_env();
    let state = AppState {
        pool,
        dispatcher,
        inspector_api_token,
    };

    let inspector_router = Router::new()
        .route("/events", get(list_events_handler))
        .route("/events/:event_id", get(get_event_handler))
        .route("/events/:event_id/attempts", get(list_attempts_handler))
        .route("/events/:event_id/replay", post(replay_event_handler))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            inspector_auth,
        ));

    let app = Router::new()
        .route("/internal/dispatcher/lease", post(lease_handler))
        .route("/internal/dispatcher/report", post(report_handler))
        .nest("/api/inspector", inspector_router)
        .with_state(state);

    let addr: SocketAddr = bind_addr.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
