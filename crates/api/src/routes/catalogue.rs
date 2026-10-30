use axum::{
    Json, Router,
    extract::{Query, State, rejection::QueryRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use motogear_persistence::catalogue::{list_active_products, list_customers};

use crate::{
    dto::catalogue::{CustomerResponse, PageQuery, ProductResponse},
    error::{api_error, database_error_response},
    state::AppState,
};

const DEFAULT_PAGE_SIZE: u32 = 20;
const MAX_PAGE_SIZE: u32 = 100;
const MAX_OFFSET: u32 = 100_000;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/v1/customers", get(customers))
        .route("/api/v1/products", get(products))
}

async fn customers(
    State(state): State<AppState>,
    query: Result<Query<PageQuery>, QueryRejection>,
) -> Response {
    let (limit, offset) = match parse_pagination(query) {
        Ok(page) => page,
        Err(response) => return response,
    };

    match list_customers(&state.pool, limit, offset).await {
        Ok(records) => {
            let customers = records
                .into_iter()
                .map(CustomerResponse::from)
                .collect::<Vec<_>>();
            Json(customers).into_response()
        }
        Err(error) => database_error_response("customer catalogue query failed", error),
    }
}

async fn products(
    State(state): State<AppState>,
    query: Result<Query<PageQuery>, QueryRejection>,
) -> Response {
    let (limit, offset) = match parse_pagination(query) {
        Ok(page) => page,
        Err(response) => return response,
    };

    match list_active_products(&state.pool, limit, offset).await {
        Ok(records) => {
            let products = records
                .into_iter()
                .map(ProductResponse::from)
                .collect::<Vec<_>>();
            Json(products).into_response()
        }
        Err(error) => database_error_response("product catalogue query failed", error),
    }
}

pub(super) fn parse_pagination(
    query: Result<Query<PageQuery>, QueryRejection>,
) -> Result<(u32, u32), Response> {
    let Query(query) = query.map_err(|_| {
        api_error(
            StatusCode::BAD_REQUEST,
            "invalid_pagination",
            "limit and offset must be whole numbers",
        )
    })?;

    let limit = query.limit.unwrap_or(DEFAULT_PAGE_SIZE);
    let offset = query.offset.unwrap_or(0);
    if limit == 0 || limit > MAX_PAGE_SIZE || offset > MAX_OFFSET {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "invalid_pagination",
            "limit must be between 1 and 100 and offset must not exceed 100000",
        ));
    }

    Ok((limit, offset))
}
