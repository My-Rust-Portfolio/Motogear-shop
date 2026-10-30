use std::{collections::HashMap, error::Error, fmt};

use motogear_domain::orders::ValidatedOrderRequest;
use sqlx::PgPool;
use uuid::Uuid;

use crate::events::outbox::enqueue_order_created;

use super::{
    pricing::price_order,
    repository::{
        ExistingOrder, find_existing_order, find_existing_order_in_transaction, insert_order_items,
        insert_pending_order, lock_active_product_prices, lock_customer,
    },
};

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

/// Coordinates idempotency, pricing, order writes, and outbox insertion.
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

    if !lock_customer(&mut *transaction, request.customer_id())
        .await
        .map_err(OrderSubmissionError::Database)?
    {
        return Err(OrderSubmissionError::CustomerNotFound);
    }

    let product_ids = request
        .items()
        .iter()
        .map(|item| item.product_id())
        .collect::<Vec<_>>();
    let product_prices = lock_active_product_prices(&mut *transaction, &product_ids)
        .await
        .map_err(OrderSubmissionError::Database)?;
    let active_prices: HashMap<Uuid, i64> = product_prices
        .into_iter()
        .map(|product| (product.id, product.price_cents))
        .collect();
    let missing_products = product_ids
        .iter()
        .filter(|product_id| !active_prices.contains_key(product_id))
        .copied()
        .collect::<Vec<_>>();
    if !missing_products.is_empty() {
        return Err(OrderSubmissionError::ProductsNotOrderable(missing_products));
    }

    let priced_order = price_order(request, &active_prices)?;
    let order_id = Uuid::new_v4();
    let inserted_order = insert_pending_order(
        &mut *transaction,
        order_id,
        request.customer_id(),
        priced_order.total_cents,
        idempotency_key,
        &fingerprint,
    )
    .await
    .map_err(OrderSubmissionError::Database)?;

    let Some((order_id, status)) = inserted_order else {
        let existing = find_existing_order_in_transaction(
            &mut *transaction,
            request.customer_id(),
            idempotency_key,
        )
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

    insert_order_items(&mut *transaction, order_id, &priced_order.items)
        .await
        .map_err(OrderSubmissionError::Database)?;
    enqueue_order_created(&mut *transaction, Uuid::new_v4(), order_id, request)
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
