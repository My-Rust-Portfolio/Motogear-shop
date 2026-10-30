use std::fmt;

use crate::Quantity;

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
        // a future signed-money extension cannot produce e.g. `EUR -1.-50`.
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

#[cfg(test)]
mod tests {
    use crate::Quantity;

    use super::{Money, MoneyError};

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
    fn money_display_formats_eur_cents_and_sign() {
        assert_eq!(Money::new(14_900).unwrap().to_string(), "EUR 149.00");
        assert_eq!(Money::new(5).unwrap().to_string(), "EUR 0.05");

        let negative = Money { cents: -150 };
        assert_eq!(negative.to_string(), "EUR -1.50");
    }
}
