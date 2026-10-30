pub mod catalogue;
pub mod config;
pub mod orders;
pub mod seed;

use sqlx::{
    migrate::{MigrateError, Migrator},
    postgres::{PgConnectOptions, PgPoolOptions},
};

pub use sqlx::PgPool;

// The path is relative to this crate's Cargo.toml. Embedding the migrations
// keeps runtime startup independent of the process's working directory.
static MIGRATOR: Migrator = sqlx::migrate!("../../migrations");

/// Opens a PostgreSQL connection pool from a `postgres://` or `postgresql://` URL.
pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await
}

/// Opens a PostgreSQL pool from structured connection settings.
pub async fn connect_with(options: PgConnectOptions) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(10)
        .connect_with(options)
        .await
}

/// Applies any migrations that have not yet been recorded by SQLx.
pub async fn run_migrations(pool: &PgPool) -> Result<(), MigrateError> {
    MIGRATOR.run(pool).await
}

/// Returns whether all migrations embedded in this build have been applied.
pub async fn schema_is_ready(pool: &PgPool) -> Result<bool, sqlx::Error> {
    let applied_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _sqlx_migrations WHERE success = TRUE")
            .fetch_one(pool)
            .await?;

    Ok(applied_count == MIGRATOR.iter().count() as i64)
}
