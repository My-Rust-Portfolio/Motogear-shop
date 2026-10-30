use std::fmt;

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

#[cfg(test)]
mod tests {
    use super::{Quantity, QuantityError};

    #[test]
    fn quantity_rejects_zero_and_negative_values() {
        assert_eq!(Quantity::new(0), Err(QuantityError::Zero));
        assert_eq!(Quantity::new(-1), Err(QuantityError::Negative));
    }
}
