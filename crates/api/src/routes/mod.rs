mod catalogue;
mod health;
mod notifications;
mod orders;

pub use orders::MAX_ORDER_REQUEST_BYTES;

use axum::Router;
use motogear_persistence::PgPool;
use tower_http::services::{ServeDir, ServeFile};

use crate::state::AppState;

pub(crate) fn build_router(pool: PgPool) -> Router {
    Router::<AppState>::new()
        .merge(health::router())
        .merge(catalogue::router())
        .merge(notifications::router())
        .merge(orders::router())
        .route_service("/", ServeFile::new("web/index.html"))
        .nest_service("/static", ServeDir::new("web"))
        .with_state(AppState::new(pool))
}
