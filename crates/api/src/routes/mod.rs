mod catalogue;
mod health;

use axum::Router;
use motogear_persistence::PgPool;
use tower_http::services::{ServeDir, ServeFile};

use crate::state::AppState;

pub(crate) fn build_router(pool: PgPool) -> Router {
    Router::<AppState>::new()
        .merge(health::router())
        .merge(catalogue::router())
        .route_service("/", ServeFile::new("web/index.html"))
        .nest_service("/static", ServeDir::new("web"))
        .with_state(AppState::new(pool))
}
