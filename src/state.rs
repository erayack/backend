use sqlx::SqlitePool;

use crate::dispatcher::DispatcherConfig;

#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
    pub dispatcher: DispatcherConfig,
    pub inspector_api_token: Option<String>,
}
