use std::{error::Error, fmt, io};

use uuid::Uuid;

#[derive(Debug)]
pub enum PersistenceError {
    CustomerNotFound,
    ProductsNotOrderable(Vec<Uuid>),
    IdempotencyConflict,
    InvalidCatalogPrice(Uuid),
    TotalOverflow,
    Database(sqlx::Error),
    Migration(sqlx::migrate::MigrateError),
    Configuration(io::Error),
}

impl fmt::Display for PersistenceError {
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
            Self::Database(error) => write!(formatter, "{error}"),
            Self::Migration(error) => write!(formatter, "{error}"),
            Self::Configuration(error) => write!(formatter, "{error}"),
        }
    }
}

impl Error for PersistenceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::Migration(error) => Some(error),
            Self::Configuration(error) => Some(error),
            _ => None,
        }
    }
}

impl From<sqlx::Error> for PersistenceError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

impl From<sqlx::migrate::MigrateError> for PersistenceError {
    fn from(error: sqlx::migrate::MigrateError) -> Self {
        Self::Migration(error)
    }
}

impl From<io::Error> for PersistenceError {
    fn from(error: io::Error) -> Self {
        Self::Configuration(error)
    }
}
