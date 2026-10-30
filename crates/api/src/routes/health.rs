use axum::{Router, extract::State, http::StatusCode, routing::get};
use motogear_persistence::schema_is_ready;

use crate::state::AppState;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
}

async fn live() -> (StatusCode, &'static str) {
    (StatusCode::OK, "live")
}

async fn ready(State(state): State<AppState>) -> (StatusCode, &'static str) {
    match schema_is_ready(&state.pool).await {
        Ok(true) => (StatusCode::OK, "ready"),
        Ok(false) => {
            eprintln!("database readiness check failed: migrations are incomplete");
            (StatusCode::SERVICE_UNAVAILABLE, "not ready")
        }
        Err(error) => {
            eprintln!("database readiness check failed: {error}");
            (StatusCode::SERVICE_UNAVAILABLE, "not ready")
        }
    }
}
