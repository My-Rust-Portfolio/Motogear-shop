use std::{env, error::Error};

use axum::{Router, extract::State, http::StatusCode, routing::get};
use motogear_persistence::{
    PgPool, config::postgres_options_from_env, connect_with, schema_is_ready,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Loading `.env` is optional so Compose can provide variables directly.
    let _ = dotenvy::dotenv();

    let pool = connect_with(postgres_options_from_env()?).await?;
    let bind_address = env::var("API_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());
    let listener = TcpListener::bind(&bind_address).await?;

    println!("MotoGear API listening on http://{bind_address}");
    axum::serve(listener, routes(pool)).await?;
    Ok(())
}

fn routes(pool: PgPool) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .with_state(pool)
}

async fn live() -> (StatusCode, &'static str) {
    (StatusCode::OK, "live")
}

async fn ready(State(pool): State<PgPool>) -> (StatusCode, &'static str) {
    match schema_is_ready(&pool).await {
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
