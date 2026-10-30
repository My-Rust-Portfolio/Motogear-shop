mod common;

use axum::http::StatusCode;
use common::{
    TestDatabase, customer_id, get, json_body, order_body, post_order, product_id, unavailable_pool,
};
use motogear_persistence::PgPool;
use sqlx::{query, query_scalar};
use uuid::Uuid;

async fn create_order(pool: &PgPool, customer: Uuid, product: Uuid) -> Uuid {
    let key = Uuid::new_v4().to_string();
    let response = post_order(pool, &order_body(customer, &[(product, 1)]), Some(&key)).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = json_body(response).await;
    Uuid::parse_str(
        body["id"]
            .as_str()
            .expect("order response should include an ID"),
    )
    .expect("order response ID should be a UUID")
}

async fn insert_notification(
    pool: &PgPool,
    customer: Uuid,
    order: Uuid,
    kind: &str,
    message: &str,
    created_at: &str,
) -> Uuid {
    let id = Uuid::new_v4();
    query(
        "INSERT INTO notifications \
         (id, customer_id, order_id, source_event_id, kind, message, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7::timestamptz)",
    )
    .bind(id)
    .bind(customer)
    .bind(order)
    .bind(Uuid::new_v4())
    .bind(kind)
    .bind(message)
    .bind(created_at)
    .execute(pool)
    .await
    .expect("notification fixture should insert");
    id
}

#[tokio::test]
async fn customer_notifications_are_newest_first_and_paginated() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "alice@example.test").await;
    let product = product_id(&database.app_pool, "HELMET-TOUR-01").await;
    let order = create_order(&database.app_pool, customer, product).await;
    let older = insert_notification(
        &database.app_pool,
        customer,
        order,
        "accepted",
        "Order accepted.",
        "2025-01-01T00:00:00Z",
    )
    .await;
    let newer = insert_notification(
        &database.app_pool,
        customer,
        order,
        "shipped",
        "Order shipped.",
        "2025-01-02T00:00:00Z",
    )
    .await;

    let response = get(
        &database.app_pool,
        &format!("/api/v1/customers/{customer}/notifications?limit=10&offset=0"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body[0]["id"], newer.to_string());
    assert_eq!(body[0]["order_id"], order.to_string());
    assert_eq!(body[0]["kind"], "shipped");
    assert_eq!(body[0]["message"], "Order shipped.");
    assert_eq!(body[1]["id"], older.to_string());

    let second_page = get(
        &database.app_pool,
        &format!("/api/v1/customers/{customer}/notifications?limit=1&offset=1"),
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
    assert_eq!(second_page[0]["id"], older.to_string());
}

#[tokio::test]
async fn notifications_returns_empty_for_known_customer_and_404_for_unknown_customer() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.app_pool, "bob@example.test").await;
    let empty = get(
        &database.app_pool,
        &format!("/api/v1/customers/{customer}/notifications"),
    )
    .await;
    assert_eq!(empty.status(), StatusCode::OK);
    assert_eq!(json_body(empty).await, serde_json::json!([]));

    let missing = get(
        &database.app_pool,
        &format!("/api/v1/customers/{}/notifications", Uuid::new_v4()),
    )
    .await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        json_body(missing).await["error"]["code"],
        "customer_not_found"
    );
}

#[tokio::test]
async fn notifications_rejects_invalid_customer_ids_and_pagination() {
    let invalid_customer = get(
        &unavailable_pool(),
        "/api/v1/customers/not-a-uuid/notifications",
    )
    .await;
    assert_eq!(invalid_customer.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(invalid_customer).await["error"]["code"],
        "invalid_customer_id"
    );

    for query_string in ["limit=0", "limit=101", "offset=100001", "limit=abc"] {
        let response = get(
            &unavailable_pool(),
            &format!(
                "/api/v1/customers/{}/notifications?{query_string}",
                Uuid::new_v4()
            ),
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
async fn notification_database_failure_returns_503_envelope() {
    let response = get(
        &unavailable_pool(),
        &format!("/api/v1/customers/{}/notifications", Uuid::new_v4()),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json_body(response).await["error"]["code"],
        "database_unavailable"
    );
}

#[tokio::test]
async fn notification_query_checks_unknown_customer_before_listing() {
    let database = TestDatabase::new().await;
    let order_owner = customer_id(&database.app_pool, "alice@example.test").await;
    let product = product_id(&database.app_pool, "HELMET-TOUR-01").await;
    let order = create_order(&database.app_pool, order_owner, product).await;
    let unknown_customer = Uuid::new_v4();
    query(
        "INSERT INTO notifications (customer_id, order_id, source_event_id, kind, message) \
         VALUES ($1, $2, $3, 'accepted', 'Should not be visible')",
    )
    .bind(order_owner)
    .bind(order)
    .bind(Uuid::new_v4())
    .execute(&database.app_pool)
    .await
    .expect("notification fixture should insert");

    let response = get(
        &database.app_pool,
        &format!("/api/v1/customers/{unknown_customer}/notifications"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let count: i64 = query_scalar("SELECT COUNT(*) FROM notifications")
        .fetch_one(&database.app_pool)
        .await
        .expect("notification count should be queryable");
    assert_eq!(count, 1);
}
