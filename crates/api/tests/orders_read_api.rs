mod common;

use axum::http::StatusCode;
use common::{
    TestDatabase, customer_id, get, json_body, order_body, post_order, product_id, unavailable_pool,
};
use motogear_persistence::{
    PgPool,
    inventory::{OrderCreatedEvent, process_order_created},
};
use serde_json::Value;
use sqlx::{query, query_scalar, types::Json};
use uuid::Uuid;

async fn create_order(pool: &PgPool, customer: Uuid, product: Uuid, quantity: i32) -> Uuid {
    let key = Uuid::new_v4().to_string();
    let response = post_order(
        pool,
        &order_body(customer, &[(product, quantity)]),
        Some(&key),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = json_body(response).await;
    Uuid::parse_str(
        body["id"]
            .as_str()
            .expect("order response should contain an ID"),
    )
    .expect("order response ID should be a UUID")
}

async fn process_inventory_for_order(pool: &PgPool, order_id: Uuid) {
    let Json(payload): Json<Value> = query_scalar(
        "SELECT payload FROM outbox_events \
         WHERE aggregate_id = $1 AND event_type = 'OrderCreated'",
    )
    .bind(order_id)
    .fetch_one(pool)
    .await
    .expect("created order should have an OrderCreated outbox event");
    let event: OrderCreatedEvent =
        serde_json::from_value(payload).expect("outbox payload should match OrderCreated");
    process_order_created(pool, &event)
        .await
        .expect("inventory worker should process the order event");
}

#[tokio::test]
async fn order_detail_returns_items_and_purchase_time_prices() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "alice@example.test").await;
    let product = product_id(&database.app_pool, "HELMET-TOUR-01").await;
    let order_id = create_order(&database.app_pool, customer, product, 2).await;

    query("UPDATE products SET price_cents = 20_000 WHERE id = $1")
        .bind(product)
        .execute(&database.app_pool)
        .await
        .expect("catalogue price should change after checkout");

    let response = get(&database.app_pool, &format!("/api/v1/orders/{order_id}")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["id"], order_id.to_string());
    assert_eq!(body["customer_id"], customer.to_string());
    assert_eq!(body["status"], "pending");
    assert_eq!(body["total_cents"], 29_800);
    assert_eq!(
        body["items"]
            .as_array()
            .expect("items should be an array")
            .len(),
        1
    );
    assert_eq!(body["items"][0]["product_id"], product.to_string());
    assert_eq!(body["items"][0]["sku"], "HELMET-TOUR-01");
    assert_eq!(body["items"][0]["quantity"], 2);
    assert_eq!(body["items"][0]["unit_price_cents"], 14_900);
}

#[tokio::test]
async fn order_detail_returns_not_found_and_invalid_uuid_errors() {
    let database = TestDatabase::new().await;
    let missing = get(
        &database.app_pool,
        &format!("/api/v1/orders/{}", Uuid::new_v4()),
    )
    .await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(json_body(missing).await["error"]["code"], "order_not_found");

    let invalid = get(&unavailable_pool(), "/api/v1/orders/not-a-uuid").await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(invalid).await["error"]["code"],
        "invalid_order_id"
    );
}

#[tokio::test]
async fn customer_order_history_is_newest_first_and_paginated() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "alice@example.test").await;
    let helmet = product_id(&database.app_pool, "HELMET-TOUR-01").await;
    let gloves = product_id(&database.app_pool, "GLOVES-ROAD-01").await;
    let older_order = create_order(&database.app_pool, customer, helmet, 1).await;
    let newer_order = create_order(&database.app_pool, customer, gloves, 2).await;

    query("UPDATE orders SET created_at = TIMESTAMPTZ '2025-01-01 00:00:00+00' WHERE id = $1")
        .bind(older_order)
        .execute(&database.app_pool)
        .await
        .expect("older order timestamp should be set for deterministic ordering");
    query("UPDATE orders SET created_at = TIMESTAMPTZ '2025-01-02 00:00:00+00' WHERE id = $1")
        .bind(newer_order)
        .execute(&database.app_pool)
        .await
        .expect("newer order timestamp should be set for deterministic ordering");

    let response = get(
        &database.app_pool,
        &format!("/api/v1/customers/{customer}/orders?limit=10&offset=0"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let history = json_body(response).await;
    assert_eq!(history[0]["id"], newer_order.to_string());
    assert_eq!(history[1]["id"], older_order.to_string());

    let second_page = get(
        &database.app_pool,
        &format!("/api/v1/customers/{customer}/orders?limit=1&offset=1"),
    )
    .await;
    assert_eq!(second_page.status(), StatusCode::OK);
    let second_page = json_body(second_page).await;
    assert_eq!(
        second_page
            .as_array()
            .expect("page should be an array")
            .len(),
        1
    );
    assert_eq!(second_page[0]["id"], older_order.to_string());
}

#[tokio::test]
async fn customer_order_history_is_empty_for_known_customer_and_not_found_for_unknown_customer() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "bob@example.test").await;
    let empty = get(
        &database.app_pool,
        &format!("/api/v1/customers/{customer}/orders"),
    )
    .await;
    assert_eq!(empty.status(), StatusCode::OK);
    assert_eq!(json_body(empty).await, serde_json::json!([]));

    let missing = get(
        &database.app_pool,
        &format!("/api/v1/customers/{}/orders", Uuid::new_v4()),
    )
    .await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        json_body(missing).await["error"]["code"],
        "customer_not_found"
    );
}

#[tokio::test]
async fn customer_order_history_rejects_invalid_pagination() {
    for query_string in ["limit=0", "limit=101", "offset=100001", "limit=abc"] {
        let response = get(
            &unavailable_pool(),
            &format!("/api/v1/customers/{}/orders?{query_string}", Uuid::new_v4()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{query_string}");
        assert_eq!(
            json_body(response).await["error"]["code"],
            "invalid_pagination",
            "{query_string}"
        );
    }
}

#[tokio::test]
async fn order_history_reflects_inventory_acceptance() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "alice@example.test").await;
    let helmet = product_id(&database.app_pool, "HELMET-TOUR-01").await;
    let order_id = create_order(&database.app_pool, customer, helmet, 1).await;
    process_inventory_for_order(&database.app_pool, order_id).await;

    let response = get(
        &database.app_pool,
        &format!("/api/v1/customers/{customer}/orders"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body[0]["id"], order_id.to_string());
    assert_eq!(body[0]["status"], "accepted");
    assert_eq!(body[0]["items"][0]["product_id"], helmet.to_string());
    assert_eq!(body[0]["rejection_reason"], Value::Null);
}

#[tokio::test]
async fn order_detail_reflects_inventory_rejection_reason() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "alice@example.test").await;
    let helmet = product_id(&database.app_pool, "HELMET-TOUR-01").await;
    let order_id = create_order(&database.app_pool, customer, helmet, 4).await;
    process_inventory_for_order(&database.app_pool, order_id).await;

    let response = get(&database.app_pool, &format!("/api/v1/orders/{order_id}")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["status"], "rejected");
    assert!(body["rejection_reason"].as_str().is_some_and(|reason| {
        reason.contains("Insufficient stock") && reason.contains("requested 4")
    }));
}

#[tokio::test]
async fn order_detail_includes_shipment_tracking_details() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "alice@example.test").await;
    let helmet = product_id(&database.app_pool, "HELMET-TOUR-01").await;
    let order_id = create_order(&database.app_pool, customer, helmet, 1).await;

    query("UPDATE orders SET status = 'shipped' WHERE id = $1")
        .bind(order_id)
        .execute(&database.app_pool)
        .await
        .expect("order should be marked shipped for the read API test");
    query(
        "INSERT INTO shipments (order_id, tracking_reference, shipped_at) \
         VALUES ($1, 'MOTO-TRACK-123', TIMESTAMPTZ '2025-01-02 03:04:05.123+00')",
    )
    .bind(order_id)
    .execute(&database.app_pool)
    .await
    .expect("shipment fixture should insert");

    let response = get(&database.app_pool, &format!("/api/v1/orders/{order_id}")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["status"], "shipped");
    assert_eq!(body["tracking_reference"], "MOTO-TRACK-123");
    assert_eq!(body["shipped_at"], "2025-01-02T03:04:05.123Z");
}
