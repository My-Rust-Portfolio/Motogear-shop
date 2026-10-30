use sqlx::PgPool;
use uuid::Uuid;

use crate::PersistenceError;

#[derive(Debug, sqlx::FromRow)]
pub struct ProductRecord {
    pub id: Uuid,
    /// Stock Keeping Unit.
    pub sku: String,
    pub name: String,
    pub description: String,
    pub price_cents: i64,
    pub stock_available: i32,
}

/// Lists active products with deterministic, bounded-page ordering.
pub async fn list_active_products(
    pool: &PgPool,
    limit: u32,
    offset: u32,
) -> Result<Vec<ProductRecord>, PersistenceError> {
    sqlx::query_as(
        "SELECT id, sku, name, description, price_cents, stock_available \
         FROM products \
         WHERE active = TRUE \
         ORDER BY sku ASC, id ASC \
         LIMIT $1 OFFSET $2",
    )
    .bind(i64::from(limit))
    .bind(i64::from(offset))
    .fetch_all(pool)
    .await
    .map_err(PersistenceError::from)
}
