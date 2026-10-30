use sqlx::PgPool;
use uuid::Uuid;

use crate::PersistenceError;

#[derive(Debug, sqlx::FromRow)]
pub struct CustomerRecord {
    pub id: Uuid,
    pub name: String,
    pub email: String,
}

/// Lists demo identities with deterministic paging.
pub async fn list_customers(
    pool: &PgPool,
    limit: u32,
    offset: u32,
) -> Result<Vec<CustomerRecord>, PersistenceError> {
    sqlx::query_as(
        "SELECT id, name, email \
         FROM customers \
         ORDER BY name ASC, id ASC \
         LIMIT $1 OFFSET $2",
    )
    .bind(i64::from(limit))
    .bind(i64::from(offset))
    .fetch_all(pool)
    .await
    .map_err(PersistenceError::from)
}
