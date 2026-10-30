use std::collections::HashMap;

use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::PersistenceError;

#[derive(Debug)]
pub struct OrderRecord {
    pub id: Uuid,
    pub customer_id: Uuid,
    pub status: String,
    pub total_cents: i64,
    pub currency: String,
    pub rejection_reason: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub tracking_reference: Option<String>,
    pub shipped_at: Option<String>,
    pub items: Vec<OrderItemRecord>,
}

#[derive(Debug)]
pub struct OrderItemRecord {
    pub product_id: Uuid,
    pub sku: String,
    pub name: String,
    pub quantity: i32,
    pub unit_price_cents: i64,
}

#[derive(Debug, FromRow)]
struct OrderHeaderRow {
    id: Uuid,
    customer_id: Uuid,
    status: String,
    total_cents: i64,
    currency: String,
    rejection_reason: Option<String>,
    created_at: String,
    updated_at: String,
    tracking_reference: Option<String>,
    shipped_at: Option<String>,
}

#[derive(Debug, FromRow)]
struct OrderItemRow {
    order_id: Uuid,
    product_id: Uuid,
    sku: String,
    name: String,
    quantity: i32,
    unit_price_cents: i64,
}

/// Returns `None` if the order does not exist.
pub async fn get_order_details(
    pool: &PgPool,
    order_id: Uuid,
) -> Result<Option<OrderRecord>, PersistenceError> {
    let mut transaction = pool.begin().await?;
    set_read_snapshot(&mut transaction).await?;

    let header = fetch_order_header(&mut transaction, order_id).await?;
    let Some(header) = header else {
        transaction.commit().await?;
        return Ok(None);
    };
    let items = fetch_order_items(&mut transaction, &[order_id]).await?;
    transaction.commit().await?;

    Ok(Some(assemble_order(header, items)))
}

/// Returns `None` if the customer does not exist, or their newest page of orders.
pub async fn list_customer_orders(
    pool: &PgPool,
    customer_id: Uuid,
    limit: u32,
    offset: u32,
) -> Result<Option<Vec<OrderRecord>>, PersistenceError> {
    let mut transaction = pool.begin().await?;
    set_read_snapshot(&mut transaction).await?;

    let customer = sqlx::query_scalar::<_, Uuid>("SELECT id FROM customers WHERE id = $1")
        .bind(customer_id)
        .fetch_optional(&mut *transaction)
        .await?;
    if customer.is_none() {
        transaction.commit().await?;
        return Ok(None);
    }

    let headers = sqlx::query_as::<_, OrderHeaderRow>(
        "SELECT o.id, o.customer_id, o.status, o.total_cents, o.currency, \
                o.rejection_reason, \
                to_char(o.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS created_at, \
                to_char(o.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS updated_at, \
                s.tracking_reference, \
                to_char(s.shipped_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS shipped_at \
         FROM orders o \
         LEFT JOIN shipments s ON s.order_id = o.id \
         WHERE o.customer_id = $1 \
         ORDER BY o.created_at DESC, o.id DESC \
         LIMIT $2 OFFSET $3",
    )
    .bind(customer_id)
    .bind(i64::from(limit))
    .bind(i64::from(offset))
    .fetch_all(&mut *transaction)
    .await?;

    let order_ids = headers.iter().map(|header| header.id).collect::<Vec<_>>();
    let items = fetch_order_items(&mut transaction, &order_ids).await?;
    transaction.commit().await?;

    Ok(Some(assemble_orders(headers, items)))
}

async fn set_read_snapshot(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), PersistenceError> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn fetch_order_header(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    order_id: Uuid,
) -> Result<Option<OrderHeaderRow>, PersistenceError> {
    sqlx::query_as::<_, OrderHeaderRow>(
        "SELECT o.id, o.customer_id, o.status, o.total_cents, o.currency, \
                o.rejection_reason, \
                to_char(o.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS created_at, \
                to_char(o.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS updated_at, \
                s.tracking_reference, \
                to_char(s.shipped_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS shipped_at \
         FROM orders o \
         LEFT JOIN shipments s ON s.order_id = o.id \
         WHERE o.id = $1",
    )
    .bind(order_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(PersistenceError::from)
}

async fn fetch_order_items(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    order_ids: &[Uuid],
) -> Result<Vec<OrderItemRow>, PersistenceError> {
    if order_ids.is_empty() {
        return Ok(Vec::new());
    }

    sqlx::query_as::<_, OrderItemRow>(
        "SELECT oi.order_id, oi.product_id, p.sku, p.name, oi.quantity, oi.unit_price_cents \
         FROM order_items oi \
         JOIN products p ON p.id = oi.product_id \
         WHERE oi.order_id = ANY($1) \
         ORDER BY oi.order_id ASC, oi.product_id ASC",
    )
    .bind(order_ids)
    .fetch_all(&mut **transaction)
    .await
    .map_err(PersistenceError::from)
}

fn assemble_orders(headers: Vec<OrderHeaderRow>, items: Vec<OrderItemRow>) -> Vec<OrderRecord> {
    let mut items_by_order = HashMap::<Uuid, Vec<OrderItemRow>>::new();
    for item in items {
        items_by_order.entry(item.order_id).or_default().push(item);
    }

    headers
        .into_iter()
        .map(|header| {
            let order_items = items_by_order.remove(&header.id).unwrap_or_default();
            assemble_order(header, order_items)
        })
        .collect()
}

fn assemble_order(header: OrderHeaderRow, items: Vec<OrderItemRow>) -> OrderRecord {
    OrderRecord {
        id: header.id,
        customer_id: header.customer_id,
        status: header.status,
        total_cents: header.total_cents,
        currency: header.currency.trim().to_owned(),
        rejection_reason: header.rejection_reason,
        created_at: header.created_at,
        updated_at: header.updated_at,
        tracking_reference: header.tracking_reference,
        shipped_at: header.shipped_at,
        items: items
            .into_iter()
            .map(|item| OrderItemRecord {
                product_id: item.product_id,
                sku: item.sku,
                name: item.name,
                quantity: item.quantity,
                unit_price_cents: item.unit_price_cents,
            })
            .collect(),
    }
}
