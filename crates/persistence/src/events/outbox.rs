use motogear_domain::orders::ValidatedOrderRequest;
use serde_json::Value;
use sqlx::{FromRow, PgConnection, PgPool, types::Json};
use uuid::Uuid;

const ORDER_EVENTS_TOPIC: &str = "shop.orders.v1";

/// An outbox event that is ready to publish to its recorded Kafka topic.
#[derive(Debug, FromRow)]
pub struct UnpublishedOutboxEvent {
    pub event_id: Uuid,
    pub aggregate_id: Uuid,
    pub target_topic: String,
    pub message_key: String,
    pub payload: Json<Value>,
    pub publish_attempts: i32,
}

/// Reads the oldest unpublished event. The publisher deliberately sends one
/// event at a time and should run as a single process to preserve sequence order.
pub async fn next_unpublished_event(
    pool: &PgPool,
) -> Result<Option<UnpublishedOutboxEvent>, sqlx::Error> {
    sqlx::query_as::<_, UnpublishedOutboxEvent>(
        "SELECT event_id, aggregate_id, target_topic, message_key, payload, publish_attempts \
         FROM outbox_events \
         WHERE published_at IS NULL \
         ORDER BY sequence_id ASC \
         LIMIT 1",
    )
    .fetch_optional(pool)
    .await
}

/// Marks an event published after Kafka has acknowledged it.
///
/// Returns `false` if the event was already marked or no longer exists.
pub async fn mark_outbox_event_published(
    pool: &PgPool,
    event_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE outbox_events \
         SET published_at = transaction_timestamp(), last_error = NULL \
         WHERE event_id = $1 AND published_at IS NULL",
    )
    .bind(event_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() == 1)
}

/// Records a failed Kafka delivery attempt for later diagnosis and retry.
pub async fn record_outbox_publish_failure(
    pool: &PgPool,
    event_id: Uuid,
    error: &str,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE outbox_events \
         SET publish_attempts = publish_attempts + 1, last_error = left($2, 1000) \
         WHERE event_id = $1 AND published_at IS NULL",
    )
    .bind(event_id)
    .bind(error)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() == 1)
}

/// Adds an OrderCreated envelope to the transaction's outbox.
pub(crate) async fn enqueue_order_created(
    connection: &mut PgConnection,
    event_id: Uuid,
    order_id: Uuid,
    request: &ValidatedOrderRequest,
) -> Result<(), sqlx::Error> {
    let payload = serde_json::json!({
        "event_id": event_id.to_string(),
        "event_type": "OrderCreated",
        "schema_version": 1,
        // PostgreSQL supplies the timestamp from this transaction's clock.
        "occurred_at": serde_json::Value::Null,
        "order_id": order_id.to_string(),
        "correlation_id": order_id.to_string(),
        "causation_id": serde_json::Value::Null,
        "data": {
            "customer_id": request.customer_id().to_string(),
            "items": request.items().iter().map(|item| {
                serde_json::json!({
                    "product_id": item.product_id().to_string(),
                    "quantity": item.quantity().get(),
                })
            }).collect::<Vec<_>>(),
        },
    });

    let target_topic = std::env::var("KAFKA_ORDERS_TOPIC")
        .ok()
        .filter(|topic| !topic.trim().is_empty())
        .unwrap_or_else(|| ORDER_EVENTS_TOPIC.to_owned());

    sqlx::query(
        "INSERT INTO outbox_events \
         (event_id, aggregate_id, event_type, schema_version, target_topic, message_key, payload) \
         VALUES ($1, $2, 'OrderCreated', 1, $3, $4, \
             jsonb_set($5, '{occurred_at}', to_jsonb(transaction_timestamp()), TRUE))",
    )
    .bind(event_id)
    .bind(order_id)
    .bind(target_topic)
    .bind(order_id.to_string())
    .bind(Json(payload))
    .execute(connection)
    .await?;

    Ok(())
}
