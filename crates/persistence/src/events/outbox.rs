use motogear_domain::orders::ValidatedOrderRequest;
use sqlx::{PgConnection, types::Json};
use uuid::Uuid;

const ORDER_EVENTS_TOPIC: &str = "shop.orders.v1";

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

    sqlx::query(
        "INSERT INTO outbox_events \
         (event_id, aggregate_id, event_type, schema_version, target_topic, message_key, payload) \
         VALUES ($1, $2, 'OrderCreated', 1, $3, $4, \
             jsonb_set($5, '{occurred_at}', to_jsonb(transaction_timestamp()), TRUE))",
    )
    .bind(event_id)
    .bind(order_id)
    .bind(ORDER_EVENTS_TOPIC)
    .bind(order_id.to_string())
    .bind(Json(payload))
    .execute(connection)
    .await?;

    Ok(())
}
