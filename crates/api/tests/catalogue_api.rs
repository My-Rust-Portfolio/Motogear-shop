mod common;

use axum::http::StatusCode;
use common::{TestDatabase, get, json_body, unavailable_pool};

#[tokio::test]
async fn products_returns_seeded_products_and_excludes_inactive_products() {
    let database = TestDatabase::new().await;
    let response = get(&database.app_pool, "/api/v1/products").await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    let products = body
        .as_array()
        .expect("products response should be an array");
    let skus = products
        .iter()
        .map(|product| product["sku"].as_str().expect("product has a SKU"))
        .collect::<Vec<_>>();

    assert_eq!(products.len(), 8);
    assert!(skus.contains(&"HELMET-TOUR-01"));
    assert!(!skus.contains(&"AAA-INACTIVE"));
}

#[tokio::test]
async fn product_pagination_uses_deterministic_sku_order() {
    let database = TestDatabase::new().await;

    let first_response = get(&database.app_pool, "/api/v1/products?limit=1&offset=0").await;
    assert_eq!(first_response.status(), StatusCode::OK);
    let first = json_body(first_response).await;
    assert_eq!(first[0]["sku"], "BAG-TAIL-01");

    let second_response = get(&database.app_pool, "/api/v1/products?limit=1&offset=1").await;
    assert_eq!(second_response.status(), StatusCode::OK);
    let second = json_body(second_response).await;
    assert_eq!(second[0]["sku"], "BOOTS-TOUR-01");
}

#[tokio::test]
async fn invalid_product_pagination_returns_400() {
    let pool = unavailable_pool();
    for uri in [
        "/api/v1/products?limit=0",
        "/api/v1/products?limit=101",
        "/api/v1/products?offset=100001",
        "/api/v1/products?limit=abc",
    ] {
        let response = get(&pool, uri).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let body = json_body(response).await;
        assert_eq!(body["error"]["code"], "invalid_pagination", "{uri}");
    }
}

#[tokio::test]
async fn customers_returns_seeded_customers() {
    let database = TestDatabase::new().await;
    let response = get(&database.app_pool, "/api/v1/customers").await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    let customers = body
        .as_array()
        .expect("customers response should be an array");
    let names = customers
        .iter()
        .map(|customer| customer["name"].as_str().expect("customer has a name"))
        .collect::<Vec<_>>();

    assert_eq!(customers.len(), 3);
    assert_eq!(names, ["Alice Morgan", "Bob Nguyen", "Casey Eriksson"]);
}

#[tokio::test]
async fn database_failure_returns_503_error_envelope() {
    let response = get(&unavailable_pool(), "/api/v1/products").await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = json_body(response).await;
    assert_eq!(body["error"]["code"], "database_unavailable");
    assert_eq!(
        body["error"]["message"],
        "The database is temporarily unavailable."
    );
}
