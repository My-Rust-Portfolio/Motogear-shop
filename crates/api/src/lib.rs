pub mod config;
pub mod dto;
pub mod error;
pub mod routes;
pub mod state;

use axum::Router;
use motogear_persistence::PgPool;

/// Builds the HTTP application around an already connected database pool.
pub fn create_router(pool: PgPool) -> Router {
    routes::build_router(pool)
}
