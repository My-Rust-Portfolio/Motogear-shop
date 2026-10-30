use sqlx::{PgPool, migrate::Migrator};

use crate::PersistenceError;

// The path is relative to this crate's Cargo.toml. Embedding the migrations
// keeps runtime startup independent of the process's working directory.
static MIGRATOR: Migrator = sqlx::migrate!("../../migrations");

/// Applies any migrations that have not yet been recorded by SQLx.
pub async fn run_migrations(pool: &PgPool) -> Result<(), PersistenceError> {
    MIGRATOR.run(pool).await.map_err(PersistenceError::from)
}

/// Returns whether all migrations embedded in this build have been applied.
pub async fn schema_is_ready(pool: &PgPool) -> Result<bool, PersistenceError> {
    let applied_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _sqlx_migrations WHERE success = TRUE")
            .fetch_one(pool)
            .await?;

    Ok(applied_count == MIGRATOR.iter().count() as i64)
}
