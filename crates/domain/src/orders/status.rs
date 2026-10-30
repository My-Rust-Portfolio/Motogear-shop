use std::fmt;

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
    use super::{InvalidOrderTransition, OrderStatus};

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
