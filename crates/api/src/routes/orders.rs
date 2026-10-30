use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Json as JsonExtractor, Path, Query, State,
        rejection::{
            BytesRejection, FailedToBufferBody, JsonRejection, PathRejection, QueryRejection,
        },
    },
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use motogear_domain::orders::{OrderRequest, OrderRequestItem};
use motogear_persistence::orders::{get_order_details, list_customer_orders, submit_order};
use uuid::Uuid;

pub const MAX_ORDER_REQUEST_BYTES: usize = 64 * 1024;

use super::catalogue::parse_pagination;
use crate::{
    dto::{
        catalogue::PageQuery,
        orders::{CreateOrderRequest, OrderResponse, OrderSubmissionResponse},
    },
    error::{ApiError, api_error, database_error_response},
    state::AppState,
};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/v1/orders", post(create_order))
        .route("/api/v1/orders/{order_id}", get(order_detail))
        .route(
            "/api/v1/customers/{customer_id}/orders",
            get(customer_orders),
        )
        .layer(DefaultBodyLimit::max(MAX_ORDER_REQUEST_BYTES))
}

async fn order_detail(
    State(state): State<AppState>,
    path: Result<Path<Uuid>, PathRejection>,
) -> Response {
    let Path(order_id) = match path {
        Ok(path) => path,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_order_id",
                "The order ID must be a UUID.",
            );
        }
    };

    match get_order_details(&state.pool, order_id).await {
        Ok(Some(order)) => Json(OrderResponse::from(order)).into_response(),
        Ok(None) => api_error(
            StatusCode::NOT_FOUND,
            "order_not_found",
            "The order was not found.",
        ),
        Err(error) => database_error_response("order detail query failed", error),
    }
}

async fn customer_orders(
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

    match list_customer_orders(&state.pool, customer_id, limit, offset).await {
        Ok(Some(orders)) => Json(
            orders
                .into_iter()
                .map(OrderResponse::from)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Ok(None) => api_error(
            StatusCode::NOT_FOUND,
            "customer_not_found",
            "The customer was not found.",
        ),
        Err(error) => database_error_response("customer order query failed", error),
    }
}

async fn create_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Result<JsonExtractor<CreateOrderRequest>, JsonRejection>,
) -> Response {
    let idempotency_key = match parse_idempotency_key(&headers) {
        Ok(key) => key,
        Err(response) => return response,
    };

    let JsonExtractor(request) = match request {
        Ok(request) => request,
        Err(rejection) => return json_rejection_response(rejection),
    };
    let domain_request = OrderRequest {
        customer_id: request.customer_id,
        items: request
            .items
            .into_iter()
            .map(|item| OrderRequestItem {
                product_id: item.product_id,
                quantity: item.quantity,
            })
            .collect(),
    };
    let validated_request = match domain_request.validate() {
        Ok(request) => request,
        Err(error) => {
            return api_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_order",
                &error.to_string(),
            );
        }
    };

    let submission = match submit_order(&state.pool, &validated_request, idempotency_key).await {
        Ok(submission) => submission,
        Err(error) => return ApiError::from(error).into_response(),
    };

    let status = if submission.replayed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    let location = match HeaderValue::try_from(format!("/api/v1/orders/{}", submission.order_id)) {
        Ok(location) => location,
        Err(error) => {
            eprintln!("could not construct order Location header: {error}");
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "The server could not complete the request.",
            );
        }
    };
    let mut response = Json(OrderSubmissionResponse {
        id: submission.order_id,
        status: submission.status,
    })
    .into_response();
    *response.status_mut() = status;
    response.headers_mut().insert(header::LOCATION, location);
    response
}

fn parse_idempotency_key(headers: &HeaderMap) -> Result<Uuid, Response> {
    let mut values = headers.get_all("Idempotency-Key").iter();
    let Some(value) = values.next() else {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "missing_idempotency_key",
            "The Idempotency-Key header is required.",
        ));
    };
    if values.next().is_some() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "invalid_idempotency_key",
            "Exactly one Idempotency-Key header must be provided.",
        ));
    }
    let Ok(value) = value.to_str() else {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "invalid_idempotency_key",
            "The Idempotency-Key header must contain a UUID.",
        ));
    };
    Uuid::parse_str(value).map_err(|_| {
        api_error(
            StatusCode::BAD_REQUEST,
            "invalid_idempotency_key",
            "The Idempotency-Key header must contain a UUID.",
        )
    })
}

fn json_rejection_response(rejection: JsonRejection) -> Response {
    match rejection {
        JsonRejection::JsonSyntaxError(_) | JsonRejection::JsonDataError(_) => api_error(
            StatusCode::BAD_REQUEST,
            "invalid_json",
            "The request body must contain valid order JSON fields.",
        ),
        JsonRejection::MissingJsonContentType(_) => api_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "The request Content-Type must be application/json.",
        ),
        JsonRejection::BytesRejection(BytesRejection::FailedToBufferBody(
            FailedToBufferBody::LengthLimitError(_),
        )) => api_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_body_too_large",
            "The request body exceeds the maximum size of 65536 bytes.",
        ),
        JsonRejection::BytesRejection(_) => api_error(
            StatusCode::BAD_REQUEST,
            "invalid_json",
            "The request body could not be read as JSON.",
        ),
        _ => api_error(
            StatusCode::BAD_REQUEST,
            "invalid_json",
            "The request body could not be interpreted as order JSON.",
        ),
    }
}
