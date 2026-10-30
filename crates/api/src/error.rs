use std::fmt::Display;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use motogear_persistence::PersistenceError;
use serde::Serialize;

#[derive(Debug)]
pub(crate) struct ApiError {
    status: StatusCode,
    code: String,
    message: String,
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorDetails,
}

#[derive(Serialize)]
struct ErrorDetails {
    code: String,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &str, message: &str) -> Self {
        Self {
            status,
            code: code.to_owned(),
            message: message.to_owned(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorEnvelope {
                error: ErrorDetails {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

impl From<PersistenceError> for ApiError {
    fn from(error: PersistenceError) -> Self {
        match error {
            PersistenceError::CustomerNotFound => Self::new(
                StatusCode::NOT_FOUND,
                "customer_not_found",
                "The customer was not found.",
            ),
            PersistenceError::ProductsNotOrderable(_) => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "products_not_orderable",
                "One or more products are missing or inactive.",
            ),
            PersistenceError::IdempotencyConflict => Self::new(
                StatusCode::CONFLICT,
                "idempotency_conflict",
                "This idempotency key was already used for a different request.",
            ),
            PersistenceError::InvalidCatalogPrice(_) => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid_catalog_price",
                "A product has an invalid catalogue price.",
            ),
            PersistenceError::TotalOverflow => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "order_total_out_of_range",
                "The order total exceeds the supported range.",
            ),
            PersistenceError::Database(error) => {
                eprintln!("database operation failed: {error}");
                Self::database_unavailable()
            }
            PersistenceError::Migration(error) => {
                eprintln!("database migration operation failed: {error}");
                Self::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "The server could not complete the request.",
                )
            }
            PersistenceError::Configuration(error) => {
                eprintln!("database configuration failed: {error}");
                Self::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "The server could not complete the request.",
                )
            }
        }
    }
}

impl ApiError {
    fn database_unavailable() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "database_unavailable",
            "The database is temporarily unavailable.",
        )
    }
}

pub(crate) fn api_error(status: StatusCode, code: &str, message: &str) -> Response {
    ApiError::new(status, code, message).into_response()
}

pub(crate) fn database_error_response(context: &str, error: impl Display) -> Response {
    eprintln!("{context}: {error}");
    ApiError::database_unavailable().into_response()
}
