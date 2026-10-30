use sqlx::{FromRow, PgConnection, PgPool};
use uuid::Uuid;

use super::pricing::PricedItem;

#[derive(Debug, FromRow)]
pub(super) struct ExistingOrder {
    pub id: Uuid,
    pub status: String,
    pub request_fingerprint: String,
}

#[derive(Debug, FromRow)]
pub(super) struct ProductPrice {
    pub id: Uuid,
    pub price_cents: i64,
}

pub(super) async fn find_existing_order(
    pool: &PgPool,
    customer_id: Uuid,
    idempotency_key: Uuid,
) -> Result<Option<ExistingOrder>, sqlx::Error> {
    sqlx::query_as(
        "SELECT id, status, request_fingerprint \
         FROM orders \
         WHERE customer_id = $1 AND idempotency_key = $2",
    )
    .bind(customer_id)
    .bind(idempotency_key)
    .fetch_optional(pool)
    .await
}

pub(super) async fn find_existing_order_in_transaction(
    connection: &mut PgConnection,
    customer_id: Uuid,
    idempotency_key: Uuid,
) -> Result<Option<ExistingOrder>, sqlx::Error> {
    sqlx::query_as(
        "SELECT id, status, request_fingerprint \
         FROM orders \
         WHERE customer_id = $1 AND idempotency_key = $2",
    )
    .bind(customer_id)
    .bind(idempotency_key)
    .fetch_optional(connection)
    .await
}

pub(super) async fn lock_customer(
    connection: &mut PgConnection,
    customer_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let exists =
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM customers WHERE id = $1 FOR KEY SHARE")
            .bind(customer_id)
            .fetch_optional(connection)
            .await?
            .is_some();

    Ok(exists)
}

pub(super) async fn lock_active_product_prices(
    connection: &mut PgConnection,
    product_ids: &[Uuid],
) -> Result<Vec<ProductPrice>, sqlx::Error> {
    sqlx::query_as(
        "SELECT id, price_cents \
         FROM products \
         WHERE active = TRUE AND id = ANY($1) \
         ORDER BY id ASC \
         FOR SHARE",
    )
    .bind(product_ids)
    .fetch_all(connection)
    .await
}

pub(super) async fn insert_pending_order(
    connection: &mut PgConnection,
    order_id: Uuid,
    customer_id: Uuid,
    total_cents: i64,
    idempotency_key: Uuid,
    fingerprint: &str,
) -> Result<Option<(Uuid, String)>, sqlx::Error> {
    sqlx::query_as(
        "INSERT INTO orders \
         (id, customer_id, status, total_cents, idempotency_key, request_fingerprint) \
         VALUES ($1, $2, 'pending', $3, $4, $5) \
         ON CONFLICT (customer_id, idempotency_key) DO NOTHING \
         RETURNING id, status",
    )
    .bind(order_id)
    .bind(customer_id)
    .bind(total_cents)
    .bind(idempotency_key)
    .bind(fingerprint)
    .fetch_optional(connection)
    .await
}

pub(super) async fn insert_order_items(
    connection: &mut PgConnection,
    order_id: Uuid,
    items: &[PricedItem],
) -> Result<(), sqlx::Error> {
    for item in items {
        sqlx::query(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(order_id)
        .bind(item.product_id)
        .bind(item.quantity)
        .bind(item.unit_price_cents)
        .execute(&mut *connection)
        .await?;
    }

    Ok(())
}
