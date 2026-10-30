use std::collections::HashMap;

use motogear_domain::{Money, orders::ValidatedOrderRequest};
use uuid::Uuid;

use super::submission::OrderSubmissionError;

pub(super) struct PricedItem {
    pub product_id: Uuid,
    pub quantity: i32,
    pub unit_price_cents: i64,
}

pub(super) struct PricedOrder {
    pub total_cents: i64,
    pub items: Vec<PricedItem>,
}

pub(super) fn price_order(
    request: &ValidatedOrderRequest,
    active_product_prices: &HashMap<Uuid, i64>,
) -> Result<PricedOrder, OrderSubmissionError> {
    let mut total = Money::ZERO;
    let mut items = Vec::with_capacity(request.items().len());

    for item in request.items() {
        let price_cents = active_product_prices
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

        items.push(PricedItem {
            product_id: item.product_id(),
            quantity: item.quantity().get(),
            unit_price_cents: price_cents,
        });
    }

    Ok(PricedOrder {
        total_cents: total.cents(),
        items,
    })
}
