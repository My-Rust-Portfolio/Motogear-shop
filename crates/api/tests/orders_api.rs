mod common;

use axum::http::StatusCode;
use common::{
    TestDatabase, customer_id, json_body, order_body, post_order, post_order_with_headers,
    product_id, unavailable_pool,
};
use motogear_api::routes::MAX_ORDER_REQUEST_BYTES;
use motogear_persistence::PgPool;
use serde_json::Value;
use sqlx::{query_as, query_scalar, types::Json};
use uuid::Uuid;

async fn assert_order_was_persisted(
    pool: &PgPool,
    order_id: Uuid,
    customer_id: Uuid,
    product_id: Uuid,
    quantity: i32,
    unit_price_cents: i64,
    stock_before: i32,
) {
    let (order_count, total_cents, status): (i64, i64, String) = query_as(
        "SELECT COUNT(*), COALESCE(MAX(total_cents), 0), COALESCE(MAX(status), '') \
         FROM orders WHERE id = $1 AND customer_id = $2",
    )
    .bind(order_id)
    .bind(customer_id)
    .fetch_one(pool)
    .await
    .expect("order should be queryable");
    assert_eq!(
        order_count, 1,
        "one durable order should match the response"
    );
    assert_eq!(total_cents, unit_price_cents * i64::from(quantity));
    assert_eq!(status, "pending");

    let items: Vec<(Uuid, i32, i64)> = query_as(
        "SELECT product_id, quantity, unit_price_cents FROM order_items \
         WHERE order_id = $1 ORDER BY product_id",
    )
    .bind(order_id)
    .fetch_all(pool)
    .await
    .expect("saved order items should be queryable");
    assert_eq!(
        items,
        vec![(product_id, quantity, unit_price_cents)],
        "saved item should retain the requested quantity and purchase-time price"
    );

    let matching_event_count: i64 = query_scalar(
        "SELECT COUNT(*) FROM outbox_events \
         WHERE aggregate_id = $1 AND event_type = 'OrderCreated'",
    )
    .bind(order_id)
    .fetch_one(pool)
    .await
    .expect("matching outbox event count should be queryable");
    assert_eq!(matching_event_count, 1);
    let unpublished_event_count: i64 = query_scalar(
        "SELECT COUNT(*) FROM outbox_events \
         WHERE aggregate_id = $1 AND event_type = 'OrderCreated' AND published_at IS NULL",
    )
    .bind(order_id)
    .fetch_one(pool)
    .await
    .expect("unpublished outbox event count should be queryable");
    assert_eq!(unpublished_event_count, 1);

    let payload: Json<Value> = query_scalar(
        "SELECT payload FROM outbox_events \
         WHERE aggregate_id = $1 AND event_type = 'OrderCreated'",
    )
    .bind(order_id)
    .fetch_one(pool)
    .await
    .expect("event payload should be queryable");
    assert_eq!(payload.0["event_type"], "OrderCreated");
    assert_eq!(payload.0["order_id"], order_id.to_string());
    assert_eq!(payload.0["data"]["customer_id"], customer_id.to_string());
    assert_eq!(
        payload.0["data"]["items"][0]["product_id"],
        product_id.to_string()
    );
    assert_eq!(payload.0["data"]["items"][0]["quantity"], quantity);

    let stock_after: i32 = query_scalar("SELECT stock_available FROM products WHERE id = $1")
        .bind(product_id)
        .fetch_one(pool)
        .await
        .expect("product stock should still be queryable");
    assert_eq!(
        stock_after, stock_before,
        "order submission must not change stock"
    );
}

async fn stock(pool: &PgPool, product_id: Uuid) -> i32 {
    query_scalar("SELECT stock_available FROM products WHERE id = $1")
        .bind(product_id)
        .fetch_one(pool)
        .await
        .expect("seeded product stock should be queryable")
}

fn response_order_id(body: &Value) -> Uuid {
    Uuid::parse_str(
        body["id"]
            .as_str()
            .expect("response should contain order ID"),
    )
    .expect("order ID should be a UUID")
}

#[tokio::test]
async fn valid_order_response_matches_persisted_order_items_and_outbox_event() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "alice@example.test").await;
    let product = product_id(&database.app_pool, "HELMET-TOUR-01").await;
    let stock_before = stock(&database.app_pool, product).await;
    let key = Uuid::new_v4().to_string();

    let response = post_order(
        &database.app_pool,
        &order_body(customer, &[(product, 2)]),
        Some(&key),
    )
    .await;

    assert_eq!(response.status(), StatusCode::CREATED);
    let location = response
        .headers()
        .get(axum::http::header::LOCATION)
        .expect("created order should include Location")
        .to_str()
        .expect("Location should be valid text")
        .to_owned();
    let body = json_body(response).await;
    assert_eq!(body["status"], "pending");
    let order = response_order_id(&body);
    assert_eq!(location, format!("/api/v1/orders/{order}"));
    assert_order_was_persisted(
        &database.app_pool,
        order,
        customer,
        product,
        2,
        14_900,
        stock_before,
    )
    .await;
}

#[tokio::test]
async fn sequential_replay_returns_the_same_order_without_duplicate_side_effects() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "alice@example.test").await;
    let product = product_id(&database.app_pool, "HELMET-TOUR-01").await;
    let stock_before = stock(&database.app_pool, product).await;
    let body = order_body(customer, &[(product, 1)]);
    let key = Uuid::new_v4().to_string();

    let first = post_order(&database.app_pool, &body, Some(&key)).await;
    assert_eq!(first.status(), StatusCode::CREATED);
    let first_order_id = response_order_id(&json_body(first).await);
    let replay = post_order(&database.app_pool, &body, Some(&key)).await;

    assert_eq!(replay.status(), StatusCode::OK);
    let replay_order_id = response_order_id(&json_body(replay).await);
    assert_eq!(replay_order_id, first_order_id);
    assert_order_was_persisted(
        &database.app_pool,
        first_order_id,
        customer,
        product,
        1,
        14_900,
        stock_before,
    )
    .await;
    let order_count: i64 =
        query_scalar("SELECT COUNT(*) FROM orders WHERE customer_id = $1 AND idempotency_key = $2")
            .bind(customer)
            .bind(Uuid::parse_str(&key).unwrap())
            .fetch_one(&database.app_pool)
            .await
            .expect("order count should be queryable");
    assert_eq!(order_count, 1);
}

#[tokio::test]
async fn changed_payload_with_same_key_returns_conflict_and_keeps_one_order() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "alice@example.test").await;
    let helmet = product_id(&database.app_pool, "HELMET-TOUR-01").await;
    let gloves = product_id(&database.app_pool, "GLOVES-ROAD-01").await;
    let key = Uuid::new_v4().to_string();

    let first = post_order(
        &database.app_pool,
        &order_body(customer, &[(helmet, 1)]),
        Some(&key),
    )
    .await;
    assert_eq!(first.status(), StatusCode::CREATED);
    let first_order_id = response_order_id(&json_body(first).await);
    let conflict = post_order(
        &database.app_pool,
        &order_body(customer, &[(gloves, 1)]),
        Some(&key),
    )
    .await;

    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    assert_eq!(
        json_body(conflict).await["error"]["code"],
        "idempotency_conflict"
    );
    let order_count: i64 =
        query_scalar("SELECT COUNT(*) FROM orders WHERE customer_id = $1 AND idempotency_key = $2")
            .bind(customer)
            .bind(Uuid::parse_str(&key).unwrap())
            .fetch_one(&database.app_pool)
            .await
            .expect("order count should be queryable");
    assert_eq!(order_count, 1);
    let item_count: i64 = query_scalar("SELECT COUNT(*) FROM order_items WHERE order_id = $1")
        .bind(first_order_id)
        .fetch_one(&database.app_pool)
        .await
        .expect("order item count should be queryable");
    assert_eq!(item_count, 1);
}

#[tokio::test]
async fn concurrent_same_key_and_payload_creates_one_order_and_one_event() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "alice@example.test").await;
    let product = product_id(&database.app_pool, "HELMET-TOUR-01").await;
    let stock_before = stock(&database.app_pool, product).await;
    let body = order_body(customer, &[(product, 1)]);
    let key = Uuid::new_v4().to_string();

    let (first, second) = tokio::join!(
        post_order(&database.app_pool, &body, Some(&key)),
        post_order(&database.app_pool, &body, Some(&key)),
    );
    let first_status = first.status();
    let second_status = second.status();
    assert!(
        (first_status == StatusCode::CREATED && second_status == StatusCode::OK)
            || (first_status == StatusCode::OK && second_status == StatusCode::CREATED),
        "concurrent matching submissions should return one 201 and one 200"
    );
    let first_id = response_order_id(&json_body(first).await);
    let second_id = response_order_id(&json_body(second).await);
    assert_eq!(first_id, second_id);
    assert_order_was_persisted(
        &database.app_pool,
        first_id,
        customer,
        product,
        1,
        14_900,
        stock_before,
    )
    .await;
    let order_count: i64 =
        query_scalar("SELECT COUNT(*) FROM orders WHERE customer_id = $1 AND idempotency_key = $2")
            .bind(customer)
            .bind(Uuid::parse_str(&key).unwrap())
            .fetch_one(&database.app_pool)
            .await
            .expect("order count should be queryable");
    assert_eq!(order_count, 1);
}

#[tokio::test]
async fn concurrent_different_payloads_create_one_order_and_conflict_once() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "alice@example.test").await;
    let helmet = product_id(&database.app_pool, "HELMET-TOUR-01").await;
    let gloves = product_id(&database.app_pool, "GLOVES-ROAD-01").await;
    let helmet_stock = stock(&database.app_pool, helmet).await;
    let gloves_stock = stock(&database.app_pool, gloves).await;
    let key = Uuid::new_v4().to_string();
    let helmet_body = order_body(customer, &[(helmet, 1)]);
    let gloves_body = order_body(customer, &[(gloves, 1)]);
    let (first, second) = tokio::join!(
        post_order(&database.app_pool, &helmet_body, Some(&key)),
        post_order(&database.app_pool, &gloves_body, Some(&key)),
    );

    let (success, successful_product, successful_stock, successful_price) =
        match (first.status(), second.status()) {
            (StatusCode::CREATED, StatusCode::CONFLICT) => (first, helmet, helmet_stock, 14_900),
            (StatusCode::CONFLICT, StatusCode::CREATED) => (second, gloves, gloves_stock, 4_900),
            statuses => panic!("expected one created response and one conflict, got {statuses:?}"),
        };
    let order_id = response_order_id(&json_body(success).await);
    assert_order_was_persisted(
        &database.app_pool,
        order_id,
        customer,
        successful_product,
        1,
        successful_price,
        successful_stock,
    )
    .await;
    let order_count: i64 =
        query_scalar("SELECT COUNT(*) FROM orders WHERE customer_id = $1 AND idempotency_key = $2")
            .bind(customer)
            .bind(Uuid::parse_str(&key).unwrap())
            .fetch_one(&database.app_pool)
            .await
            .expect("order count should be queryable");
    assert_eq!(order_count, 1);
}

#[tokio::test]
async fn replay_after_price_change_returns_original_order_and_purchase_price() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "alice@example.test").await;
    let product = product_id(&database.app_pool, "HELMET-TOUR-01").await;
    let stock_before = stock(&database.app_pool, product).await;
    let body = order_body(customer, &[(product, 1)]);
    let key = Uuid::new_v4().to_string();
    let first = post_order(&database.app_pool, &body, Some(&key)).await;
    assert_eq!(first.status(), StatusCode::CREATED);
    let order_id = response_order_id(&json_body(first).await);

    sqlx::query("UPDATE products SET price_cents = 20_000 WHERE id = $1")
        .bind(product)
        .execute(&database.app_pool)
        .await
        .expect("product price should change for replay test");
    let replay = post_order(&database.app_pool, &body, Some(&key)).await;

    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(response_order_id(&json_body(replay).await), order_id);
    assert_order_was_persisted(
        &database.app_pool,
        order_id,
        customer,
        product,
        1,
        14_900,
        stock_before,
    )
    .await;
}

#[tokio::test]
async fn replay_after_product_deactivation_returns_original_order() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "alice@example.test").await;
    let product = product_id(&database.app_pool, "HELMET-TOUR-01").await;
    let stock_before = stock(&database.app_pool, product).await;
    let body = order_body(customer, &[(product, 1)]);
    let key = Uuid::new_v4().to_string();
    let first = post_order(&database.app_pool, &body, Some(&key)).await;
    assert_eq!(first.status(), StatusCode::CREATED);
    let order_id = response_order_id(&json_body(first).await);

    sqlx::query("UPDATE products SET active = FALSE WHERE id = $1")
        .bind(product)
        .execute(&database.app_pool)
        .await
        .expect("product should deactivate for replay test");
    let replay = post_order(&database.app_pool, &body, Some(&key)).await;

    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(response_order_id(&json_body(replay).await), order_id);
    assert_order_was_persisted(
        &database.app_pool,
        order_id,
        customer,
        product,
        1,
        14_900,
        stock_before,
    )
    .await;
}

#[tokio::test]
async fn order_requires_exactly_one_valid_idempotency_key() {
    let pool = unavailable_pool();
    let body = order_body(Uuid::new_v4(), &[(Uuid::new_v4(), 1)]);

    let missing = post_order(&pool, &body, None).await;
    assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(missing).await["error"]["code"],
        "missing_idempotency_key"
    );

    let invalid = post_order(&pool, &body, Some("not-a-uuid")).await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(invalid).await["error"]["code"],
        "invalid_idempotency_key"
    );

    let first_key = Uuid::new_v4().to_string();
    let second_key = Uuid::new_v4().to_string();
    let duplicate = post_order_with_headers(
        &pool,
        &body,
        Some("application/json"),
        &[&first_key, &second_key],
    )
    .await;
    assert_eq!(duplicate.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(duplicate).await["error"]["code"],
        "invalid_idempotency_key"
    );
}

#[tokio::test]
async fn malformed_json_and_invalid_fields_return_400() {
    let pool = unavailable_pool();
    let key = Uuid::new_v4().to_string();
    let malformed = post_order(&pool, "{\"customer_id\":", Some(&key)).await;
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json_body(malformed).await["error"]["code"], "invalid_json");

    let invalid_field = format!(
        "{{\"customer_id\":\"{}\",\"items\":[{{\"product_id\":\"not-a-uuid\",\"quantity\":1}}]}}",
        Uuid::new_v4()
    );
    let invalid_field = post_order(&pool, &invalid_field, Some(&key)).await;
    assert_eq!(invalid_field.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(invalid_field).await["error"]["code"],
        "invalid_json"
    );
}

#[tokio::test]
async fn unsupported_content_type_returns_415() {
    let pool = unavailable_pool();
    let key = Uuid::new_v4().to_string();
    let response = post_order_with_headers(&pool, "{}", Some("text/plain"), &[&key]).await;

    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(
        json_body(response).await["error"]["code"],
        "unsupported_media_type"
    );
}

#[tokio::test]
async fn oversized_request_body_returns_413() {
    let pool = unavailable_pool();
    let key = Uuid::new_v4().to_string();
    let body = format!(
        "{{\"padding\":\"{}\"}}",
        "x".repeat(MAX_ORDER_REQUEST_BYTES)
    );
    let response = post_order(&pool, &body, Some(&key)).await;

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        json_body(response).await["error"]["code"],
        "request_body_too_large"
    );
}

#[tokio::test]
async fn invalid_order_shape_returns_422_before_database_access() {
    let response = post_order(
        &unavailable_pool(),
        &order_body(Uuid::new_v4(), &[]),
        Some(&Uuid::new_v4().to_string()),
    )
    .await;

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(json_body(response).await["error"]["code"], "invalid_order");
}

#[tokio::test]
async fn missing_customer_maps_to_404() {
    let database = TestDatabase::new().await;
    let product = product_id(&database.app_pool, "HELMET-TOUR-01").await;

    let response = post_order(
        &database.app_pool,
        &order_body(Uuid::new_v4(), &[(product, 1)]),
        Some(&Uuid::new_v4().to_string()),
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        json_body(response).await["error"]["code"],
        "customer_not_found"
    );
}

#[tokio::test]
async fn inactive_product_maps_to_422() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "alice@example.test").await;
    let inactive_product = product_id(&database.app_pool, "AAA-INACTIVE").await;

    let response = post_order(
        &database.app_pool,
        &order_body(customer, &[(inactive_product, 1)]),
        Some(&Uuid::new_v4().to_string()),
    )
    .await;

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        json_body(response).await["error"]["code"],
        "products_not_orderable"
    );
}

#[tokio::test]
async fn database_failure_is_generic_503() {
    let response = post_order(
        &unavailable_pool(),
        &order_body(Uuid::new_v4(), &[(Uuid::new_v4(), 1)]),
        Some(&Uuid::new_v4().to_string()),
    )
    .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = json_body(response).await;
    assert_eq!(body["error"]["code"], "database_unavailable");
    assert_eq!(
        body["error"]["message"],
        "The database is temporarily unavailable."
    );
}
