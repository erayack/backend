# Repository Guidelines

## Project Structure
- `src/`: Rust crate source.
  - `main.rs`: Axum HTTP server + route wiring.
  - `handlers/`: HTTP handlers (request/response shaping).
  - `dispatcher/`: leasing/reporting logic for webhook delivery.
  - `inspector/`: event/attempt inspection queries.
  - `types/`: shared request/response DTOs (also exported via Specta).
- `migrations/`: SQLite schema migrations (`000X_description.sql`, applied in order).
- `tests/`: integration tests (Tokio + SQLite temp DB).
- `docs/`: product/phase notes and design docs.

## Build, Test, and Development Commands
- `cargo run`: run the server locally.
  - Env: `DATABASE_URL` (default `sqlite:receiver.db`), `RECEIVER_INTERNAL_BIND_ADDR` (default `127.0.0.1:3001`).
- `cargo nextest run`: run unit + integration tests.
- `cargo fmt`: format with rustfmt (run before committing).
- `cargo clippy`: lint (Clippy pedantic is enabled; keep the crate warning-free).
- `just sync-bindings`: export TypeScript bindings (expects sibling checkout at `../modern-product-repo/...`).

## Coding Style & Naming
- Rust 2024 edition; rely on `cargo fmt` for formatting.
- Naming: `snake_case` for modules/functions, `CamelCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- Avoid `unwrap()`/`expect()`/`panic!` in production code; return errors and use `?` instead (Clippy denies these in the main crate).
- Prefer parameterized SQL via `sqlx::query(...).bind(...)`; never build SQL with untrusted string concatenation.

## Testing Guidelines
- Prefer integration tests in `tests/` for DB-backed behavior; use `#[tokio::test]`.
- When adding schema changes, add a new `migrations/000X_*.sql` file and update/extend tests to cover the new behavior.

## Commit & Pull Request Guidelines
- Commit messages follow Conventional Commits seen in history: `feat: …`, `fix: …`, `refactor: …`, `docs: …`, `test: …`, `chore: …`.
- PRs should include: summary, test plan (commands run), migration notes (if any), and API changes (routes/DTOs) when applicable.

