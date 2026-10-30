use std::{error::Error, fmt};

use motogear_domain::Money;
use serde::Deserialize;
use serde_json::Value;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

/// Stable consumer identity shared by notification-worker replicas.
pub const NOTIFICATIONS_CONSUMER_NAME: &str = "motogear-notifications-v1";

#[derive(Clone, Debug, Deserialize)]
pub struct OrderLifecycleEvent {
    pub event_id: Uuid,
    pub event_type: String,
    pub schema_version: i32,
    pub order_id: Uuid,
    pub data: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NotificationOutcome {
    Created { kind: String },
    Duplicate,
}

#[derive(Debug)]
pub enum NotificationProcessError {
    InvalidEvent(String),
    Database(sqlx::Error),
}

#[derive(Debug)]
pub struct CustomerNotificationRecord {
    pub id: Uuid,
    pub order_id: Uuid,
    pub kind: String,
    pub message: String,
    pub created_at: String,
}

#[derive(Debug, FromRow)]
struct CustomerNotificationRow {
    id: Uuid,
    order_id: Uuid,
    kind: String,
    message: String,
    created_at: String,
}

impl fmt::Display for NotificationProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEvent(message) => {
                write!(formatter, "invalid order lifecycle event: {message}")
            }
            Self::Database(error) => write!(formatter, "{error}"),
        }
    }
}

impl Error for NotificationProcessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::InvalidEvent(_) => None,
        }
    }
}

impl From<sqlx::Error> for NotificationProcessError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

/// Returns `None` for a missing customer, otherwise their newest page of
/// notifications in deterministic order.
pub async fn list_customer_notifications(
    pool: &PgPool,
    customer_id: Uuid,
    limit: u32,
    offset: u32,
) -> Result<Option<Vec<CustomerNotificationRecord>>, sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await?;

    let customer = sqlx::query_scalar::<_, Uuid>("SELECT id FROM customers WHERE id = $1")
        .bind(customer_id)
        .fetch_optional(&mut *transaction)
        .await?;
    if customer.is_none() {
        transaction.commit().await?;
        return Ok(None);
    }

    let rows = sqlx::query_as::<_, CustomerNotificationRow>(
        "SELECT id, order_id, kind, message, \
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS created_at \
         FROM notifications WHERE customer_id = $1 \
         ORDER BY created_at DESC, id DESC LIMIT $2 OFFSET $3",
    )
    .bind(customer_id)
    .bind(i64::from(limit))
    .bind(i64::from(offset))
    .fetch_all(&mut *transaction)
    .await?;
    transaction.commit().await?;

    Ok(Some(
        rows.into_iter()
            .map(|row| CustomerNotificationRecord {
                id: row.id,
                order_id: row.order_id,
                kind: row.kind,
                message: row.message,
                created_at: row.created_at,
            })
            .collect(),
    ))
}

/// Persists one customer-facing notification and its deduplication marker
/// atomically. Notifications never modify inventory or order state.
pub async fn process_order_lifecycle_event(
    pool: &PgPool,
    event: &OrderLifecycleEvent,
) -> Result<NotificationOutcome, NotificationProcessError> {
    if event.schema_version != 1 {
        return Err(NotificationProcessError::InvalidEvent(format!(
            "unsupported schema version {}",
            event.schema_version
        )));
    }

    let customer_id = event_customer_id(event)?;
    let (kind, message) = notification_content(event)?;
    let mut transaction = pool.begin().await?;
    let new_marker = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO processed_events (consumer_name, event_id) \
         VALUES ($1, $2) \
         ON CONFLICT (consumer_name, event_id) DO NOTHING \
         RETURNING event_id",
    )
    .bind(NOTIFICATIONS_CONSUMER_NAME)
    .bind(event.event_id)
    .fetch_optional(&mut *transaction)
    .await?;

    if new_marker.is_none() {
        transaction.commit().await?;
        return Ok(NotificationOutcome::Duplicate);
    }

    let order_customer_id =
        sqlx::query_scalar::<_, Uuid>("SELECT customer_id FROM orders WHERE id = $1")
            .bind(event.order_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| {
                NotificationProcessError::InvalidEvent(format!(
                    "order {} does not exist",
                    event.order_id
                ))
            })?;
    if customer_id != order_customer_id {
        return Err(NotificationProcessError::InvalidEvent(format!(
            "event customer {} does not match order {}",
            customer_id, event.order_id
        )));
    }

    sqlx::query(
        "INSERT INTO notifications (customer_id, order_id, source_event_id, kind, message) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(customer_id)
    .bind(event.order_id)
    .bind(event.event_id)
    .bind(&kind)
    .bind(message)
    .execute(&mut *transaction)
    .await?;

    transaction.commit().await?;
    Ok(NotificationOutcome::Created { kind })
}

fn event_customer_id(event: &OrderLifecycleEvent) -> Result<Uuid, NotificationProcessError> {
    let customer_id = event
        .data
        .get("customer_id")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_event("data.customer_id must be a UUID"))?;
    Uuid::parse_str(customer_id).map_err(|_| invalid_event("data.customer_id must be a UUID"))
}

fn notification_content(
    event: &OrderLifecycleEvent,
) -> Result<(String, String), NotificationProcessError> {
    let order = event.order_id;
    let content = match event.event_type.as_str() {
        "OrderAccepted" => {
            let items = event
                .data
                .get("items")
                .and_then(Value::as_array)
                .filter(|items| !items.is_empty())
                .ok_or_else(|| {
                    invalid_event("OrderAccepted data.items must be a non-empty array")
                })?;
            for item in items {
                let product_id = item
                    .get("product_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid_event("each item must have a product UUID"))?;
                Uuid::parse_str(product_id)
                    .map_err(|_| invalid_event("each item must have a product UUID"))?;
                if item
                    .get("quantity")
                    .and_then(Value::as_i64)
                    .is_none_or(|q| q <= 0)
                {
                    return Err(invalid_event("each item quantity must be positive"));
                }
            }
            (
                "accepted",
                format!("Order {order} was accepted and its stock has been reserved."),
            )
        }
        "OrderRejected" => {
            let reason = event
                .data
                .get("reason")
                .and_then(Value::as_str)
                .filter(|reason| !reason.trim().is_empty())
                .ok_or_else(|| invalid_event("OrderRejected data.reason must be non-empty"))?;
            (
                "rejected",
                format!("Order {order} was rejected: {}", reason.trim()),
            )
        }
        "OrderShipped" => {
            let total_cents = event
                .data
                .get("total_cents")
                .and_then(Value::as_i64)
                .ok_or_else(|| {
                    invalid_event("OrderShipped data.total_cents must be non-negative")
                })?;
            let total = Money::new(total_cents)
                .map_err(|_| invalid_event("OrderShipped data.total_cents must be non-negative"))?;
            if event.data.get("currency").and_then(Value::as_str) != Some("EUR") {
                return Err(invalid_event("OrderShipped data.currency must be EUR"));
            }
            let tracking_reference = event
                .data
                .get("tracking_reference")
                .and_then(Value::as_str)
                .filter(|reference| !reference.trim().is_empty())
                .ok_or_else(|| {
                    invalid_event("OrderShipped data.tracking_reference must be non-empty")
                })?;
            (
                "shipped",
                format!(
                    "Order {order} has shipped. Simulated tracking reference: {}. Order value: {}.",
                    tracking_reference.trim(),
                    total
                ),
            )
        }
        other => {
            return Err(invalid_event(&format!("unsupported event type {other}")));
        }
    };

    Ok((content.0.to_owned(), content.1))
}

fn invalid_event(message: &str) -> NotificationProcessError {
    NotificationProcessError::InvalidEvent(message.to_owned())
}
