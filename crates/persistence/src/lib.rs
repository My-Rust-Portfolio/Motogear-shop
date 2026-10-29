use sqlx::{
    PgPool,
    migrate::{MigrateError, Migrator},
    postgres::PgPoolOptions,
};

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

/// Applies any migrations that have not yet been recorded by SQLx.
pub async fn run_migrations(pool: &PgPool) -> Result<(), MigrateError> {
    MIGRATOR.run(pool).await
}
