# AGENTS.md

## Commands
- Build: `cargo build`
- Test all: `cargo test`
- Test single: `cargo test <test_name>` (e.g., `cargo test export_bindings`)
- Check: `cargo check`
- Sync TS bindings: `just sync-bindings`

## Architecture
Rust/Axum webhook receiver service with SQLite (sqlx). Generates TypeScript bindings via specta.
- `src/handlers/` - Axum route handlers
- `src/types/` - Domain types (WebhookEvent, AttemptLog, CircuitState) with `#[derive(specta::Type)]`
- `src/dispatcher/` - Webhook dispatch logic
- `src/error.rs` - `ApiError` enum implementing `IntoResponse`
- `migrations/` - SQLx migrations (numbered `NNNN_*.sql`)
- `tests/export_bindings.rs` - Generates TS types to `modern-product-repo`

## Code Style
- Use `ApiError` variants for error handling, convert with `?` via `From<sqlx::Error>`
- Types use `#[derive(Debug, Clone, Serialize, Deserialize, specta::Type)]`
- UUIDs via `uuid::Uuid`, timestamps via `chrono::DateTime<Utc>`
- Imports: std first, then external crates, then local modules
