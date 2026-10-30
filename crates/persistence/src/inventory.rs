use std::{collections::HashMap, error::Error, fmt};

use motogear_domain::orders::{OrderRequest, OrderRequestItem, OrderStatus, ValidatedOrderRequest};
use serde::Deserialize;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::events::outbox::enqueue_order_event;

/// Stable consumer identity shared by every inventory-worker replica.
pub const INVENTORY_CONSUMER_NAME: &str = "motogear-inventory-v1";

#[derive(Clone, Debug, Deserialize)]
pub struct OrderCreatedEvent {
    pub event_id: Uuid,
    pub event_type: String,
    pub schema_version: i32,
    pub order_id: Uuid,
    pub correlation_id: Uuid,
    pub data: OrderCreatedData,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OrderCreatedData {
    pub customer_id: Uuid,
    pub items: Vec<OrderCreatedItem>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub struct OrderCreatedItem {
    pub product_id: Uuid,
    pub quantity: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InventoryOutcome {
    Accepted,
    Rejected { reason: String },
    Duplicate,
    IgnoredNoLongerPending,
}

#[derive(Debug)]
pub enum InventoryProcessError {
    InvalidEvent(String),
    Database(sqlx::Error),
}

impl fmt::Display for InventoryProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEvent(message) => {
                write!(formatter, "invalid OrderCreated event: {message}")
            }
            Self::Database(error) => write!(formatter, "{error}"),
        }
    }
}

impl Error for InventoryProcessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::InvalidEvent(_) => None,
        }
    }
}

impl From<sqlx::Error> for InventoryProcessError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

#[derive(Debug, FromRow)]
struct LockedOrder {
    customer_id: Uuid,
    status: String,
}

#[derive(Debug, FromRow)]
struct OrderItemRow {
    product_id: Uuid,
    quantity: i32,
}

#[derive(Debug, FromRow)]
struct ProductStock {
    id: Uuid,
    stock_available: i32,
}

/// Applies one `OrderCreated` event atomically and records its deduplication
/// marker in the same transaction as stock, order status, and follow-up event.
pub async fn process_order_created(
    pool: &PgPool,
    event: &OrderCreatedEvent,
) -> Result<InventoryOutcome, InventoryProcessError> {
    if event.event_type != "OrderCreated" || event.schema_version != 1 {
        return Err(InventoryProcessError::InvalidEvent(format!(
            "expected OrderCreated schema version 1, received {} version {}",
            event.event_type, event.schema_version
        )));
    }

    let request = OrderRequest {
        customer_id: event.data.customer_id,
        items: event
            .data
            .items
            .iter()
            .map(|item| OrderRequestItem {
                product_id: item.product_id,
                quantity: item.quantity,
            })
            .collect(),
    }
    .validate()
    .map_err(|error| InventoryProcessError::InvalidEvent(error.to_string()))?;

    let mut transaction = pool.begin().await?;
    let new_marker = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO processed_events (consumer_name, event_id) \
         VALUES ($1, $2) \
         ON CONFLICT (consumer_name, event_id) DO NOTHING \
         RETURNING event_id",
    )
    .bind(INVENTORY_CONSUMER_NAME)
    .bind(event.event_id)
    .fetch_optional(&mut *transaction)
    .await?;

    if new_marker.is_none() {
        transaction.commit().await?;
        return Ok(InventoryOutcome::Duplicate);
    }

    let order = sqlx::query_as::<_, LockedOrder>(
        "SELECT customer_id, status FROM orders WHERE id = $1 FOR NO KEY UPDATE",
    )
    .bind(event.order_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or_else(|| {
        InventoryProcessError::InvalidEvent(format!("order {} does not exist", event.order_id))
    })?;

    if order.customer_id != event.data.customer_id {
        return Err(InventoryProcessError::InvalidEvent(format!(
            "customer {} does not match order {}",
            event.data.customer_id, event.order_id
        )));
    }

    if order.status != OrderStatus::Pending.as_str() {
        transaction.commit().await?;
        return Ok(InventoryOutcome::IgnoredNoLongerPending);
    }

    let order_items = sqlx::query_as::<_, OrderItemRow>(
        "SELECT product_id, quantity FROM order_items \
         WHERE order_id = $1 ORDER BY product_id ASC",
    )
    .bind(event.order_id)
    .fetch_all(&mut *transaction)
    .await?;
    ensure_event_items_match(&request, &order_items)?;

    let product_ids = order_items
        .iter()
        .map(|item| item.product_id)
        .collect::<Vec<_>>();
    // All replicas lock the same rows in product-ID order. Do not use
    // SKIP LOCKED: contention is not a stock shortage.
    let products = sqlx::query_as::<_, ProductStock>(
        "SELECT id, stock_available FROM products \
         WHERE id = ANY($1) ORDER BY id ASC FOR NO KEY UPDATE",
    )
    .bind(&product_ids)
    .fetch_all(&mut *transaction)
    .await?;
    let stock_by_product = products
        .into_iter()
        .map(|product| (product.id, product.stock_available))
        .collect::<HashMap<_, _>>();

    let shortages = order_items
        .iter()
        .filter_map(|item| {
            let available = stock_by_product
                .get(&item.product_id)
                .copied()
                .unwrap_or_default();
            (available < item.quantity).then(|| {
                format!(
                    "{} (requested {}, available {})",
                    item.product_id, item.quantity, available
                )
            })
        })
        .collect::<Vec<_>>();

    let (status, rejection_reason, outcome, event_type, data) = if shortages.is_empty() {
        reserve_stock(&mut transaction, event.order_id, &order_items).await?;
        (
            OrderStatus::Accepted.as_str(),
            None,
            InventoryOutcome::Accepted,
            "OrderAccepted",
            serde_json::json!({
                "customer_id": order.customer_id.to_string(),
                "items": order_items.iter().map(|item| serde_json::json!({
                    "product_id": item.product_id.to_string(),
                    "quantity": item.quantity,
                })).collect::<Vec<_>>(),
            }),
        )
    } else {
        let reason = format!("Insufficient stock for {}", shortages.join("; "));
        (
            OrderStatus::Rejected.as_str(),
            Some(reason.clone()),
            InventoryOutcome::Rejected {
                reason: reason.clone(),
            },
            "OrderRejected",
            serde_json::json!({
                "customer_id": order.customer_id.to_string(),
                "reason": reason,
            }),
        )
    };

    let update = sqlx::query(
        "UPDATE orders \
         SET status = $2, rejection_reason = $3, updated_at = transaction_timestamp() \
         WHERE id = $1 AND status = $4",
    )
    .bind(event.order_id)
    .bind(status)
    .bind(rejection_reason)
    .bind(OrderStatus::Pending.as_str())
    .execute(&mut *transaction)
    .await?;
    if update.rows_affected() != 1 {
        return Err(sqlx::Error::RowNotFound.into());
    }

    enqueue_order_event(
        &mut transaction,
        Uuid::new_v4(),
        event.order_id,
        event_type,
        event.correlation_id,
        Some(event.event_id),
        data,
    )
    .await?;

    transaction.commit().await?;
    Ok(outcome)
}

fn ensure_event_items_match(
    request: &ValidatedOrderRequest,
    order_items: &[OrderItemRow],
) -> Result<(), InventoryProcessError> {
    let items_match = request.items().len() == order_items.len()
        && request
            .items()
            .iter()
            .zip(order_items)
            .all(|(event_item, database_item)| {
                event_item.product_id() == database_item.product_id
                    && event_item.quantity().get() == database_item.quantity
            });

    if items_match {
        Ok(())
    } else {
        Err(InventoryProcessError::InvalidEvent(
            "event items do not match the persisted order items".to_owned(),
        ))
    }
}

async fn reserve_stock(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    order_id: Uuid,
    items: &[OrderItemRow],
) -> Result<(), sqlx::Error> {
    for item in items {
        let update = sqlx::query(
            "UPDATE products \
             SET stock_available = stock_available - $2, \
                 updated_at = transaction_timestamp() \
             WHERE id = $1 AND stock_available >= $2",
        )
        .bind(item.product_id)
        .bind(item.quantity)
        .execute(&mut **transaction)
        .await?;
        if update.rows_affected() != 1 {
            return Err(sqlx::Error::RowNotFound);
        }

        sqlx::query(
            "INSERT INTO stock_reservations (order_id, product_id, quantity) \
             VALUES ($1, $2, $3)",
        )
        .bind(order_id)
        .bind(item.product_id)
        .bind(item.quantity)
        .execute(&mut **transaction)
        .await?;
    }

    Ok(())
}
