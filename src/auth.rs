use axum::{
    body::Body,
    extract::State,
    http::{Request, header::AUTHORIZATION},
    middleware::Next,
    response::Response,
};
use subtle::ConstantTimeEq;

use crate::{error::ApiError, state::AppState};

pub async fn inspector_auth(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let Some(expected_token) = &state.inspector_api_token else {
        return Ok(next.run(req).await);
    };

    let provided_token = match req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|value| {
            let trimmed = value.trim_start();
            if trimmed.len() >= 7 && trimmed[..7].eq_ignore_ascii_case("bearer ") {
                Some(trimmed[7..].trim())
            } else {
                None
            }
        }) {
        Some(token) => token,
        _ => {
            return Err(ApiError::unauthorized(
                "missing or invalid Authorization header",
            ));
        }
    };

    if !constant_time_eq(expected_token.as_bytes(), provided_token.as_bytes()) {
        return Err(ApiError::unauthorized("invalid token"));
    }

    Ok(next.run(req).await)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}
