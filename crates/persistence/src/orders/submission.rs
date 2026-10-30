use std::collections::HashMap;

use motogear_domain::orders::ValidatedOrderRequest;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{PersistenceError, events::outbox::enqueue_order_created};

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

/// Coordinates idempotency, pricing, order writes, and outbox insertion.
pub async fn submit_order(
    pool: &PgPool,
    request: &ValidatedOrderRequest,
    idempotency_key: Uuid,
) -> Result<OrderSubmission, PersistenceError> {
    let fingerprint = request.fingerprint();
    if let Some(existing) =
        find_existing_order(pool, request.customer_id(), idempotency_key).await?
    {
        return replay_or_conflict(existing, &fingerprint);
    }

    let mut transaction = pool.begin().await?;

    if !lock_customer(&mut transaction, request.customer_id()).await? {
        return Err(PersistenceError::CustomerNotFound);
    }

    let product_ids = request
        .items()
        .iter()
        .map(|item| item.product_id())
        .collect::<Vec<_>>();
    let product_prices = lock_active_product_prices(&mut transaction, &product_ids).await?;
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
        return Err(PersistenceError::ProductsNotOrderable(missing_products));
    }

    let priced_order = price_order(request, &active_prices)?;
    let order_id = Uuid::new_v4();
    let inserted_order = insert_pending_order(
        &mut transaction,
        order_id,
        request.customer_id(),
        priced_order.total_cents,
        idempotency_key,
        &fingerprint,
    )
    .await?;

    let Some((order_id, status)) = inserted_order else {
        let existing = find_existing_order_in_transaction(
            &mut transaction,
            request.customer_id(),
            idempotency_key,
        )
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;

        let result = replay_or_conflict(existing, &fingerprint)?;
        transaction.commit().await?;
        return Ok(result);
    };

    insert_order_items(&mut transaction, order_id, &priced_order.items).await?;
    enqueue_order_created(&mut transaction, Uuid::new_v4(), order_id, request).await?;

    transaction.commit().await?;

    Ok(OrderSubmission {
        order_id,
        status,
        replayed: false,
    })
}

fn replay_or_conflict(
    existing: ExistingOrder,
    fingerprint: &str,
) -> Result<OrderSubmission, PersistenceError> {
    if existing.request_fingerprint != fingerprint {
        return Err(PersistenceError::IdempotencyConflict);
    }

    Ok(OrderSubmission {
        order_id: existing.id,
        status: existing.status,
        replayed: true,
    })
}
