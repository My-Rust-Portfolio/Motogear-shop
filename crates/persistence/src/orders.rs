use std::{collections::HashMap, error::Error, fmt};

use motogear_domain::{Money, orders::ValidatedOrderRequest};
use sqlx::{FromRow, PgPool, types::Json};
use uuid::Uuid;

const ORDER_EVENTS_TOPIC: &str = "shop.orders.v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderSubmission {
    pub order_id: Uuid,
    pub status: String,
    /// True when this request reused an order stored for the same key/payload.
    pub replayed: bool,
}

#[derive(Debug)]
pub enum OrderSubmissionError {
    CustomerNotFound,
    ProductsNotOrderable(Vec<Uuid>),
    IdempotencyConflict,
    InvalidCatalogPrice(Uuid),
    TotalOverflow,
    Database(sqlx::Error),
}

impl fmt::Display for OrderSubmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CustomerNotFound => formatter.write_str("customer was not found"),
            Self::ProductsNotOrderable(product_ids) => write!(
                formatter,
                "one or more products are missing or inactive: {}",
                product_ids
                    .iter()
                    .map(Uuid::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::IdempotencyConflict => {
                formatter.write_str("idempotency key was already used for a different request")
            }
            Self::InvalidCatalogPrice(product_id) => {
                write!(formatter, "product {product_id} has an invalid price")
            }
            Self::TotalOverflow => formatter.write_str("order total exceeds the supported range"),
            Self::Database(error) => write!(formatter, "database operation failed: {error}"),
        }
    }
}

impl Error for OrderSubmissionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Debug, FromRow)]
struct ExistingOrder {
    id: Uuid,
    status: String,
    request_fingerprint: String,
}

#[derive(Debug, FromRow)]
struct ProductPrice {
    id: Uuid,
    price_cents: i64,
}

/// Stores a pending order, its purchase-time prices, and its creation event.
///
/// Prices are read while locking active product rows, so a concurrent price
/// edit cannot split the order across multiple catalogue versions. Inventory
/// remains unchanged here; the inventory worker handles allocation later.
pub async fn submit_order(
    pool: &PgPool,
    request: &ValidatedOrderRequest,
    idempotency_key: Uuid,
) -> Result<OrderSubmission, OrderSubmissionError> {
    let fingerprint = request.fingerprint();
    if let Some(existing) = find_existing_order(pool, request.customer_id(), idempotency_key)
        .await
        .map_err(OrderSubmissionError::Database)?
    {
        return replay_or_conflict(existing, &fingerprint);
    }

    let mut transaction = pool.begin().await.map_err(OrderSubmissionError::Database)?;

    let customer_exists =
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM customers WHERE id = $1 FOR KEY SHARE")
            .bind(request.customer_id())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(OrderSubmissionError::Database)?;
    if customer_exists.is_none() {
        return Err(OrderSubmissionError::CustomerNotFound);
    }

    let product_ids = request
        .items()
        .iter()
        .map(|item| item.product_id())
        .collect::<Vec<_>>();
    let product_prices = sqlx::query_as::<_, ProductPrice>(
        "SELECT id, price_cents \
         FROM products \
         WHERE active = TRUE AND id = ANY($1) \
         ORDER BY id ASC \
         FOR SHARE",
    )
    .bind(&product_ids)
    .fetch_all(&mut *transaction)
    .await
    .map_err(OrderSubmissionError::Database)?;

    let prices = product_prices
        .into_iter()
        .map(|product| (product.id, product.price_cents))
        .collect::<HashMap<_, _>>();
    let missing_products = product_ids
        .iter()
        .filter(|product_id| !prices.contains_key(product_id))
        .copied()
        .collect::<Vec<_>>();
    if !missing_products.is_empty() {
        return Err(OrderSubmissionError::ProductsNotOrderable(missing_products));
    }

    let mut total = Money::ZERO;
    let mut priced_items = Vec::with_capacity(request.items().len());
    for item in request.items() {
        let price_cents = prices
            .get(&item.product_id())
            .copied()
            .ok_or_else(|| OrderSubmissionError::ProductsNotOrderable(vec![item.product_id()]))?;
        let price = Money::new(price_cents)
            .map_err(|_| OrderSubmissionError::InvalidCatalogPrice(item.product_id()))?;
        let line_total = price
            .checked_mul(item.quantity())
            .map_err(|_| OrderSubmissionError::TotalOverflow)?;
        total = total
            .checked_add(line_total)
            .map_err(|_| OrderSubmissionError::TotalOverflow)?;
        priced_items.push((item.product_id(), item.quantity().get(), price_cents));
    }

    let order_id = Uuid::new_v4();
    let inserted_order = sqlx::query_as::<_, (Uuid, String)>(
        "INSERT INTO orders \
         (id, customer_id, status, total_cents, idempotency_key, request_fingerprint) \
         VALUES ($1, $2, 'pending', $3, $4, $5) \
         ON CONFLICT (customer_id, idempotency_key) DO NOTHING \
         RETURNING id, status",
    )
    .bind(order_id)
    .bind(request.customer_id())
    .bind(total.cents())
    .bind(idempotency_key)
    .bind(&fingerprint)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(OrderSubmissionError::Database)?;

    let Some((order_id, status)) = inserted_order else {
        let existing = sqlx::query_as::<_, ExistingOrder>(
            "SELECT id, status, request_fingerprint \
             FROM orders \
             WHERE customer_id = $1 AND idempotency_key = $2",
        )
        .bind(request.customer_id())
        .bind(idempotency_key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(OrderSubmissionError::Database)?
        .ok_or_else(|| OrderSubmissionError::Database(sqlx::Error::RowNotFound))?;

        let result = replay_or_conflict(existing, &fingerprint)?;
        transaction
            .commit()
            .await
            .map_err(OrderSubmissionError::Database)?;
        return Ok(result);
    };

    for (product_id, quantity, unit_price_cents) in priced_items {
        sqlx::query(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(order_id)
        .bind(product_id)
        .bind(quantity)
        .bind(unit_price_cents)
        .execute(&mut *transaction)
        .await
        .map_err(OrderSubmissionError::Database)?;
    }

    let event_id = Uuid::new_v4();
    let event_payload = serde_json::json!({
        "event_id": event_id.to_string(),
        "event_type": "OrderCreated",
        "schema_version": 1,
        // PostgreSQL replaces this placeholder in the insert below so the
        // event time comes from the same clock as the transaction.
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
    .bind(Json(event_payload))
    .execute(&mut *transaction)
    .await
    .map_err(OrderSubmissionError::Database)?;

    transaction
        .commit()
        .await
        .map_err(OrderSubmissionError::Database)?;

    Ok(OrderSubmission {
        order_id,
        status,
        replayed: false,
    })
}

async fn find_existing_order(
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

fn replay_or_conflict(
    existing: ExistingOrder,
    fingerprint: &str,
) -> Result<OrderSubmission, OrderSubmissionError> {
    if existing.request_fingerprint != fingerprint {
        return Err(OrderSubmissionError::IdempotencyConflict);
    }

    Ok(OrderSubmission {
        order_id: existing.id,
        status: existing.status,
        replayed: true,
    })
}
