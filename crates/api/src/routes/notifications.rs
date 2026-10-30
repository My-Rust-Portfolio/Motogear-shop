use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        rejection::{PathRejection, QueryRejection},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use motogear_persistence::notifications::list_customer_notifications;
use uuid::Uuid;

use super::catalogue::parse_pagination;
use crate::{
    dto::{catalogue::PageQuery, notifications::NotificationResponse},
    error::{api_error, database_error_response},
    state::AppState,
};

pub(super) fn router() -> Router<AppState> {
    Router::new().route(
        "/api/v1/customers/{customer_id}/notifications",
        get(customer_notifications),
    )
}

async fn customer_notifications(
    State(state): State<AppState>,
    path: Result<Path<Uuid>, PathRejection>,
    query: Result<Query<PageQuery>, QueryRejection>,
) -> Response {
    let Path(customer_id) = match path {
        Ok(path) => path,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_customer_id",
                "The customer ID must be a UUID.",
            );
        }
    };
    let (limit, offset) = match parse_pagination(query) {
        Ok(page) => page,
        Err(response) => return response,
    };

    match list_customer_notifications(&state.pool, customer_id, limit, offset).await {
        Ok(Some(records)) => Json(
            records
                .into_iter()
                .map(NotificationResponse::from)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Ok(None) => api_error(
            StatusCode::NOT_FOUND,
            "customer_not_found",
            "The customer was not found.",
        ),
        Err(error) => database_error_response("customer notification query failed", error),
    }
}
