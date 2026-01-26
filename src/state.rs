use sqlx::SqlitePool;

use crate::dispatcher::DispatcherConfig;

#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
    pub dispatcher: DispatcherConfig,
}
