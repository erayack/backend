use axum::{routing::post, Router};
use receiver::{
    dispatcher::DispatcherConfig,
    handlers::dispatcher::{lease_handler, report_handler},
    state::AppState,
};
use sqlx::sqlite::SqlitePoolOptions;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "sqlite:receiver.db".to_string());
    let bind_addr = std::env::var("RECEIVER_INTERNAL_BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:3001".to_string());

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    sqlx::query("PRAGMA foreign_keys = ON;")
        .execute(&pool)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    let dispatcher = DispatcherConfig::from_env();
    let state = AppState { pool, dispatcher };

    let app = Router::new()
        .route("/internal/dispatcher/lease", post(lease_handler))
        .route("/internal/dispatcher/report", post(report_handler))
        .with_state(state);

    let addr: SocketAddr = bind_addr.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
