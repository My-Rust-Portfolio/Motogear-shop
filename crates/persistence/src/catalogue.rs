use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, sqlx::FromRow)]
pub struct CustomerRecord {
    pub id: Uuid,
    pub name: String,
    pub email: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ProductRecord {
    pub id: Uuid,
    // Stock Keeping Unit
    pub sku: String,
    pub name: String,
    pub description: String,
    pub price_cents: i64,
    pub stock_available: i32,
}

/// Lists demo identities with deterministic paging.
pub async fn list_customers(
    pool: &PgPool,
    limit: u32,
    offset: u32,
) -> Result<Vec<CustomerRecord>, sqlx::Error> {
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
}

/// Lists active products with deterministic, bounded-page ordering.
pub async fn list_active_products(
    pool: &PgPool,
    limit: u32,
    offset: u32,
) -> Result<Vec<ProductRecord>, sqlx::Error> {
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
}
