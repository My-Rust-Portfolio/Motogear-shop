use std::fmt;

/// A non-negative EUR amount stored as whole cents.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Money {
    cents: i64,
}

impl Money {
    pub const ZERO: Self = Self { cents: 0 };

    /// Creates an amount from cents, rejecting negative values.
    pub const fn new(cents: i64) -> Result<Self, MoneyError> {
        if cents < 0 {
            Err(MoneyError::NegativeAmount)
        } else {
            Ok(Self { cents })
        }
    }

    pub const fn cents(self) -> i64 {
        self.cents
    }

    pub const fn checked_add(self, other: Self) -> Result<Self, MoneyError> {
        match self.cents.checked_add(other.cents) {
            Some(cents) => Ok(Self { cents }),
            None => Err(MoneyError::Overflow),
        }
    }

    pub const fn checked_mul(self, quantity: Quantity) -> Result<Self, MoneyError> {
        match self.cents.checked_mul(quantity.get() as i64) {
            Some(cents) => Ok(Self { cents }),
            None => Err(MoneyError::Overflow),
        }
    }
}

impl fmt::Display for Money {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Money is currently non-negative, but format the sign explicitly so
        // a future signed-money extension cannot produce e.g. "EUR -1.-50".
        let sign = if self.cents < 0 { "-" } else { "" };
        let absolute_cents = self.cents.unsigned_abs();
        write!(
            formatter,
            "EUR {}{}.{:02}",
            sign,
            absolute_cents / 100,
            absolute_cents % 100
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MoneyError {
    NegativeAmount,
    Overflow,
}

impl fmt::Display for MoneyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeAmount => formatter.write_str("money amount cannot be negative"),
            Self::Overflow => formatter.write_str("money calculation exceeded the supported range"),
        }
    }
}

impl std::error::Error for MoneyError {}

/// A positive line-item quantity that fits PostgreSQL's `INTEGER` column.
///
/// Taking `i32` at construction makes the database boundary explicit and
/// ensures every `Quantity` is both positive and persistable without a cast
/// that could overflow.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Quantity(i32);

impl Quantity {
    pub const fn new(value: i32) -> Result<Self, QuantityError> {
        if value == 0 {
            Err(QuantityError::Zero)
        } else if value < 0 {
            Err(QuantityError::Negative)
        } else {
            Ok(Self(value))
        }
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuantityError {
    Zero,
    Negative,
}

impl fmt::Display for QuantityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => formatter.write_str("quantity must be greater than zero"),
            Self::Negative => formatter.write_str("quantity cannot be negative"),
        }
    }
}

impl std::error::Error for QuantityError {}

/// Lifecycle states for a shop order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderStatus {
    Pending,
    Accepted,
    Rejected,
    Shipped,
}

impl OrderStatus {
    /// Returns whether the lifecycle permits moving directly to `next`.
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::Accepted)
                | (Self::Pending, Self::Rejected)
                | (Self::Accepted, Self::Shipped)
        )
    }

    /// Validates one lifecycle step and returns the new status.
    pub const fn transition(self, next: Self) -> Result<Self, InvalidOrderTransition> {
        if self.can_transition_to(next) {
            Ok(next)
        } else {
            Err(InvalidOrderTransition {
                from: self,
                to: next,
            })
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Shipped => "shipped",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidOrderTransition {
    pub from: OrderStatus,
    pub to: OrderStatus,
}

impl fmt::Display for InvalidOrderTransition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "order cannot transition from {} to {}",
            self.from.as_str(),
            self.to.as_str()
        )
    }
}

impl std::error::Error for InvalidOrderTransition {}

#[cfg(test)]
mod tests {
    use super::{InvalidOrderTransition, Money, MoneyError, OrderStatus, Quantity, QuantityError};

    #[test]
    fn money_accepts_zero_and_positive_values() {
        assert_eq!(Money::new(0).unwrap().cents(), 0);
        assert_eq!(Money::new(14_900).unwrap().cents(), 14_900);
    }

    #[test]
    fn money_rejects_negative_values() {
        assert_eq!(Money::new(-1), Err(MoneyError::NegativeAmount));
    }

    #[test]
    fn money_addition_is_checked() {
        let left = Money::new(149).unwrap();
        let right = Money::new(51).unwrap();
        assert_eq!(left.checked_add(right).unwrap().cents(), 200);

        let largest = Money::new(i64::MAX).unwrap();
        assert_eq!(
            largest.checked_add(Money::new(1).unwrap()),
            Err(MoneyError::Overflow)
        );
    }

    #[test]
    fn money_multiplication_uses_a_validated_quantity_and_checks_overflow() {
        let price = Money::new(149).unwrap();
        let quantity = Quantity::new(3).unwrap();
        assert_eq!(price.checked_mul(quantity).unwrap().cents(), 447);

        let largest_amount = Money::new(i64::MAX).unwrap();
        assert_eq!(
            largest_amount.checked_mul(Quantity::new(2).unwrap()),
            Err(MoneyError::Overflow)
        );
    }

    #[test]
    fn quantity_rejects_zero_and_negative_values() {
        assert_eq!(Quantity::new(0), Err(QuantityError::Zero));
        assert_eq!(Quantity::new(-1), Err(QuantityError::Negative));
    }

    #[test]
    fn money_display_formats_eur_cents_and_sign() {
        assert_eq!(Money::new(14_900).unwrap().to_string(), "EUR 149.00");
        assert_eq!(Money::new(5).unwrap().to_string(), "EUR 0.05");

        let negative = Money { cents: -150 };
        assert_eq!(negative.to_string(), "EUR -1.50");
    }

    #[test]
    fn order_status_allows_only_the_documented_lifecycle() {
        assert_eq!(
            OrderStatus::Pending
                .transition(OrderStatus::Accepted)
                .unwrap(),
            OrderStatus::Accepted
        );
        assert_eq!(
            OrderStatus::Pending
                .transition(OrderStatus::Rejected)
                .unwrap(),
            OrderStatus::Rejected
        );
        assert_eq!(
            OrderStatus::Accepted
                .transition(OrderStatus::Shipped)
                .unwrap(),
            OrderStatus::Shipped
        );

        assert_eq!(
            OrderStatus::Pending.transition(OrderStatus::Shipped),
            Err(InvalidOrderTransition {
                from: OrderStatus::Pending,
                to: OrderStatus::Shipped,
            })
        );
        assert!(
            OrderStatus::Rejected
                .transition(OrderStatus::Accepted)
                .is_err()
        );
        assert!(
            OrderStatus::Shipped
                .transition(OrderStatus::Pending)
                .is_err()
        );
    }

    #[test]
    fn every_order_status_has_a_stable_string_value() {
        assert_eq!(OrderStatus::Pending.as_str(), "pending");
        assert_eq!(OrderStatus::Accepted.as_str(), "accepted");
        assert_eq!(OrderStatus::Rejected.as_str(), "rejected");
        assert_eq!(OrderStatus::Shipped.as_str(), "shipped");
    }
}
