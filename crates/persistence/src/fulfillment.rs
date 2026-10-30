use std::{error::Error, fmt};

use motogear_domain::orders::{OrderRequest, OrderRequestItem, OrderStatus, ValidatedOrderRequest};
use serde::Deserialize;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::events::outbox::enqueue_order_event;

/// Stable consumer identity shared by fulfillment-worker replicas.
pub const FULFILLMENT_CONSUMER_NAME: &str = "motogear-fulfillment-v1";

#[derive(Clone, Debug, Deserialize)]
pub struct OrderAcceptedEvent {
    pub event_id: Uuid,
    pub event_type: String,
    pub schema_version: i32,
    pub order_id: Uuid,
    pub correlation_id: Uuid,
    pub data: OrderAcceptedData,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OrderAcceptedData {
    pub customer_id: Uuid,
    pub items: Vec<OrderAcceptedItem>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub struct OrderAcceptedItem {
    pub product_id: Uuid,
    pub quantity: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FulfillmentOutcome {
    Shipped { tracking_reference: String },
    Duplicate,
    IgnoredAlreadyShipped,
}

#[derive(Debug)]
pub enum FulfillmentProcessError {
    InvalidEvent(String),
    Database(sqlx::Error),
}

impl fmt::Display for FulfillmentProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEvent(message) => {
                write!(formatter, "invalid OrderAccepted event: {message}")
            }
            Self::Database(error) => write!(formatter, "{error}"),
        }
    }
}

impl Error for FulfillmentProcessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::InvalidEvent(_) => None,
        }
    }
}

impl From<sqlx::Error> for FulfillmentProcessError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

#[derive(Debug, FromRow)]
struct LockedOrder {
    customer_id: Uuid,
    status: String,
    total_cents: i64,
    currency: String,
}

#[derive(Debug, FromRow, Eq, PartialEq)]
struct OrderLine {
    product_id: Uuid,
    quantity: i32,
}

/// Checks the durable deduplication marker so the worker can avoid applying
/// its optional simulation delay to an event that was already completed.
///
/// This is only a delay optimization. `process_order_accepted` repeats the
/// deduplication check inside the transaction that creates the shipment.
pub async fn fulfillment_event_is_processed(
    pool: &PgPool,
    event_id: Uuid,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (\
             SELECT 1 FROM processed_events \
             WHERE consumer_name = $1 AND event_id = $2\
         )",
    )
    .bind(FULFILLMENT_CONSUMER_NAME)
    .bind(event_id)
    .fetch_one(pool)
    .await
}

/// Creates one simulated shipment and emits `OrderShipped` atomically.
///
/// No inventory is changed here: stock was allocated when the order became
/// accepted. A repeated Kafka delivery is a safe no-op through the processed
/// event marker, while the order lock and unique shipment constraint protect
/// against distinct accepted events racing for the same order.
pub async fn process_order_accepted(
    pool: &PgPool,
    event: &OrderAcceptedEvent,
) -> Result<FulfillmentOutcome, FulfillmentProcessError> {
    if event.event_type != "OrderAccepted" || event.schema_version != 1 {
        return Err(FulfillmentProcessError::InvalidEvent(format!(
            "expected OrderAccepted schema version 1, received {} version {}",
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
    .map_err(|error| FulfillmentProcessError::InvalidEvent(error.to_string()))?;

    let mut transaction = pool.begin().await?;
    let new_marker = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO processed_events (consumer_name, event_id) \
         VALUES ($1, $2) \
         ON CONFLICT (consumer_name, event_id) DO NOTHING \
         RETURNING event_id",
    )
    .bind(FULFILLMENT_CONSUMER_NAME)
    .bind(event.event_id)
    .fetch_optional(&mut *transaction)
    .await?;

    if new_marker.is_none() {
        transaction.commit().await?;
        return Ok(FulfillmentOutcome::Duplicate);
    }

    let order = sqlx::query_as::<_, LockedOrder>(
        "SELECT customer_id, status, total_cents, currency \
         FROM orders WHERE id = $1 FOR NO KEY UPDATE",
    )
    .bind(event.order_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or_else(|| {
        FulfillmentProcessError::InvalidEvent(format!("order {} does not exist", event.order_id))
    })?;

    if order.customer_id != event.data.customer_id {
        return Err(FulfillmentProcessError::InvalidEvent(format!(
            "customer {} does not match order {}",
            event.data.customer_id, event.order_id
        )));
    }

    if order.status == OrderStatus::Shipped.as_str() {
        transaction.commit().await?;
        return Ok(FulfillmentOutcome::IgnoredAlreadyShipped);
    }
    if order.status != OrderStatus::Accepted.as_str() {
        return Err(FulfillmentProcessError::InvalidEvent(format!(
            "order {} has status {}, expected accepted",
            event.order_id, order.status
        )));
    }

    let order_lines = sqlx::query_as::<_, OrderLine>(
        "SELECT product_id, quantity FROM order_items \
         WHERE order_id = $1 ORDER BY product_id ASC",
    )
    .bind(event.order_id)
    .fetch_all(&mut *transaction)
    .await?;
    ensure_event_lines_match(&request, &order_lines)?;

    let reservations = sqlx::query_as::<_, OrderLine>(
        "SELECT product_id, quantity FROM stock_reservations \
         WHERE order_id = $1 ORDER BY product_id ASC",
    )
    .bind(event.order_id)
    .fetch_all(&mut *transaction)
    .await?;
    if reservations != order_lines {
        return Err(FulfillmentProcessError::InvalidEvent(format!(
            "stock reservations do not match order {}",
            event.order_id
        )));
    }

    let tracking_reference = format!(
        "SIM-{}",
        event.order_id.simple().to_string().to_ascii_uppercase()
    );
    sqlx::query("INSERT INTO shipments (order_id, tracking_reference) VALUES ($1, $2)")
        .bind(event.order_id)
        .bind(&tracking_reference)
        .execute(&mut *transaction)
        .await?;

    let update = sqlx::query(
        "UPDATE orders SET status = $2, updated_at = transaction_timestamp() \
         WHERE id = $1 AND status = $3",
    )
    .bind(event.order_id)
    .bind(OrderStatus::Shipped.as_str())
    .bind(OrderStatus::Accepted.as_str())
    .execute(&mut *transaction)
    .await?;
    if update.rows_affected() != 1 {
        return Err(sqlx::Error::RowNotFound.into());
    }

    enqueue_order_event(
        &mut transaction,
        Uuid::new_v4(),
        event.order_id,
        "OrderShipped",
        event.correlation_id,
        Some(event.event_id),
        serde_json::json!({
            "customer_id": order.customer_id.to_string(),
            "total_cents": order.total_cents,
            "currency": order.currency.trim(),
            "tracking_reference": tracking_reference,
        }),
    )
    .await?;

    transaction.commit().await?;
    Ok(FulfillmentOutcome::Shipped { tracking_reference })
}

fn ensure_event_lines_match(
    request: &ValidatedOrderRequest,
    order_lines: &[OrderLine],
) -> Result<(), FulfillmentProcessError> {
    let lines_match = request.items().len() == order_lines.len()
        && request
            .items()
            .iter()
            .zip(order_lines)
            .all(|(event_item, order_line)| {
                event_item.product_id() == order_line.product_id
                    && event_item.quantity().get() == order_line.quantity
            });

    if lines_match {
        Ok(())
    } else {
        Err(FulfillmentProcessError::InvalidEvent(
            "event items do not match persisted order items".to_owned(),
        ))
    }
}
