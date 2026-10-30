//! Checkout request shape validation and canonicalization.
//!
//! Persistence verifies referenced customers and active products. Inventory
//! availability is decided later by the inventory worker.

use std::fmt::{self, Write as _};

use uuid::Uuid;

use crate::{Quantity, QuantityError};

/// The maximum number of distinct products accepted in one order request.
pub const MAX_ORDER_ITEMS: usize = 50;

/// Demo safety limit per product line; this is not a business-derived cap.
///
/// Persistence must still check product activity and available inventory.
pub const MAX_ORDER_ITEM_QUANTITY: i32 = 1_000;

/// Untrusted order data received from a caller, before validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderRequest {
    pub customer_id: Uuid,
    pub items: Vec<OrderRequestItem>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderRequestItem {
    pub product_id: Uuid,
    pub quantity: i32,
}

impl OrderRequest {
    /// Validates the request's shape and quantities, then sorts items by ID.
    ///
    /// Sorting makes equivalent requests produce the same fingerprint even
    /// when the browser submits their lines in a different order.
    /// Product existence and active status are checked by persistence.
    pub fn validate(&self) -> Result<ValidatedOrderRequest, OrderRequestError> {
        if self.items.is_empty() {
            return Err(OrderRequestError::EmptyItems);
        }
        if self.items.len() > MAX_ORDER_ITEMS {
            return Err(OrderRequestError::TooManyItems {
                maximum: MAX_ORDER_ITEMS,
            });
        }

        // Keep the original request intact; the demo limit makes this copy small.
        let mut items = self.items.clone();
        items.sort_by_key(|item| item.product_id);

        let mut validated_items = Vec::with_capacity(items.len());
        let mut previous_product_id = None;
        for item in items {
            if previous_product_id == Some(item.product_id) {
                return Err(OrderRequestError::DuplicateProduct(item.product_id));
            }
            previous_product_id = Some(item.product_id);

            let quantity = Quantity::new(item.quantity).map_err(|source| {
                OrderRequestError::InvalidQuantity {
                    product_id: item.product_id,
                    source,
                }
            })?;
            if quantity.get() > MAX_ORDER_ITEM_QUANTITY {
                return Err(OrderRequestError::QuantityTooLarge {
                    product_id: item.product_id,
                    maximum: MAX_ORDER_ITEM_QUANTITY,
                });
            }

            validated_items.push(ValidatedOrderRequestItem {
                product_id: item.product_id,
                quantity,
            });
        }

        Ok(ValidatedOrderRequest {
            customer_id: self.customer_id,
            items: validated_items,
        })
    }
}

/// An order request whose lines are valid and in canonical product-ID order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedOrderRequest {
    customer_id: Uuid,
    items: Vec<ValidatedOrderRequestItem>,
}

impl ValidatedOrderRequest {
    pub const fn customer_id(&self) -> Uuid {
        self.customer_id
    }

    pub fn items(&self) -> &[ValidatedOrderRequestItem] {
        &self.items
    }

    /// Returns a stable, readable representation for idempotency comparisons.
    ///
    /// The representation contains only validated customer, product, and
    /// quantity values. It deliberately excludes prices, which may change
    /// after the original order is submitted. This is not a cryptographic hash
    /// or tamper-proof token; the API should compute it and compare retries to
    /// the value stored with the first accepted request.
    pub fn fingerprint(&self) -> String {
        let mut fingerprint = format!("v1|{}|", self.customer_id);
        for (index, item) in self.items.iter().enumerate() {
            if index != 0 {
                fingerprint.push(',');
            }
            // Writing to a String cannot fail.
            let _ = write!(fingerprint, "{}:{}", item.product_id, item.quantity.get());
        }
        fingerprint
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedOrderRequestItem {
    product_id: Uuid,
    quantity: Quantity,
}

impl ValidatedOrderRequestItem {
    pub const fn product_id(&self) -> Uuid {
        self.product_id
    }

    pub const fn quantity(&self) -> Quantity {
        self.quantity
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderRequestError {
    EmptyItems,
    TooManyItems {
        maximum: usize,
    },
    DuplicateProduct(Uuid),
    InvalidQuantity {
        product_id: Uuid,
        source: QuantityError,
    },
    QuantityTooLarge {
        product_id: Uuid,
        maximum: i32,
    },
}

impl fmt::Display for OrderRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyItems => formatter.write_str("an order must contain at least one item"),
            Self::TooManyItems { maximum } => {
                write!(
                    formatter,
                    "an order cannot contain more than {maximum} items"
                )
            }
            Self::DuplicateProduct(product_id) => {
                write!(formatter, "product {product_id} appears more than once")
            }
            Self::InvalidQuantity { product_id, source } => {
                write!(
                    formatter,
                    "invalid quantity for product {product_id}: {source}"
                )
            }
            Self::QuantityTooLarge {
                product_id,
                maximum,
            } => write!(
                formatter,
                "quantity for product {product_id} cannot exceed {maximum}"
            ),
        }
    }
}

impl std::error::Error for OrderRequestError {}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::QuantityError;

    use super::{
        MAX_ORDER_ITEM_QUANTITY, MAX_ORDER_ITEMS, OrderRequest, OrderRequestError, OrderRequestItem,
    };

    fn item(product_id: u128, quantity: i32) -> OrderRequestItem {
        OrderRequestItem {
            product_id: Uuid::from_u128(product_id),
            quantity,
        }
    }

    fn request(customer_id: u128, items: Vec<OrderRequestItem>) -> OrderRequest {
        OrderRequest {
            customer_id: Uuid::from_u128(customer_id),
            items,
        }
    }

    #[test]
    fn accepts_a_valid_order_request() {
        let customer_id = Uuid::from_u128(10);
        let product_a = Uuid::from_u128(1);
        let product_b = Uuid::from_u128(2);
        let validated = request(10, vec![item(2, 3), item(1, 1)])
            .validate()
            .unwrap();

        assert_eq!(validated.customer_id(), customer_id);
        assert_eq!(validated.items().len(), 2);
        assert_eq!(validated.items()[0].product_id(), product_a);
        assert_eq!(validated.items()[0].quantity().get(), 1);
        assert_eq!(validated.items()[1].product_id(), product_b);
        assert_eq!(validated.items()[1].quantity().get(), 3);
    }

    #[test]
    fn rejects_an_empty_order() {
        assert_eq!(
            request(1, vec![]).validate(),
            Err(OrderRequestError::EmptyItems)
        );
    }

    #[test]
    fn rejects_too_many_items() {
        let items = (1..=MAX_ORDER_ITEMS as u128 + 1)
            .map(|product_id| item(product_id, 1))
            .collect();

        assert_eq!(
            request(1, items).validate(),
            Err(OrderRequestError::TooManyItems {
                maximum: MAX_ORDER_ITEMS,
            })
        );
    }

    #[test]
    fn rejects_duplicate_products() {
        assert_eq!(
            request(1, vec![item(42, 1), item(42, 2)]).validate(),
            Err(OrderRequestError::DuplicateProduct(Uuid::from_u128(42)))
        );
    }

    #[test]
    fn rejects_zero_quantity() {
        assert_eq!(
            request(1, vec![item(42, 0)]).validate(),
            Err(OrderRequestError::InvalidQuantity {
                product_id: Uuid::from_u128(42),
                source: QuantityError::Zero,
            })
        );
    }

    #[test]
    fn rejects_negative_quantity() {
        assert_eq!(
            request(1, vec![item(42, -1)]).validate(),
            Err(OrderRequestError::InvalidQuantity {
                product_id: Uuid::from_u128(42),
                source: QuantityError::Negative,
            })
        );
    }

    #[test]
    fn rejects_quantity_above_limit() {
        assert_eq!(
            request(1, vec![item(42, MAX_ORDER_ITEM_QUANTITY + 1)]).validate(),
            Err(OrderRequestError::QuantityTooLarge {
                product_id: Uuid::from_u128(42),
                maximum: MAX_ORDER_ITEM_QUANTITY,
            })
        );
    }

    #[test]
    fn fingerprint_is_independent_of_item_order() {
        let first = request(10, vec![item(1, 2), item(2, 3)])
            .validate()
            .unwrap();
        let reordered = request(10, vec![item(2, 3), item(1, 2)])
            .validate()
            .unwrap();

        assert_eq!(first.fingerprint(), reordered.fingerprint());
    }

    #[test]
    fn fingerprint_changes_when_customer_changes() {
        let alice = request(10, vec![item(1, 2)]).validate().unwrap();
        let bob = request(11, vec![item(1, 2)]).validate().unwrap();

        assert_ne!(alice.fingerprint(), bob.fingerprint());
    }

    #[test]
    fn fingerprint_changes_when_quantity_changes() {
        let one = request(10, vec![item(1, 1)]).validate().unwrap();
        let two = request(10, vec![item(1, 2)]).validate().unwrap();

        assert_ne!(one.fingerprint(), two.fingerprint());
    }
}
