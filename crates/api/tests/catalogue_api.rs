#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use motogear_persistence::{run_migrations, seed::seed_demo_data};
    use serde_json::Value;
    use sqlx::{
        AssertSqlSafe,
        postgres::{PgConnectOptions, PgPoolOptions},
        query,
    };
    use tower::ServiceExt;

    use axum::http::StatusCode;
    use motogear_api::create_router;
    use motogear_persistence::PgPool;
    use uuid::Uuid;

    struct TestDatabase {
        admin_pool: sqlx::PgPool,
        app_pool: PgPool,
        schema: String,
    }

    impl TestDatabase {
        async fn new() -> Self {
            let database_url = std::env::var("TEST_DATABASE_URL").expect(
                "set TEST_DATABASE_URL to a PostgreSQL database where tests may create schemas",
            );
            let options = PgConnectOptions::from_str(&database_url)
                .expect("TEST_DATABASE_URL must be a PostgreSQL connection URL");
            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(options.clone())
                .await
                .expect("TEST_DATABASE_URL must point to a reachable PostgreSQL database");

            let schema = format!("catalogue_test_{}", Uuid::new_v4().simple());
            let create_schema = format!("CREATE SCHEMA \"{schema}\"");
            // This identifier comes from a UUID, contains only hex digits,
            // and is quoted; SQLx requires an explicit assertion for DDL.
            query(AssertSqlSafe(create_schema))
                .execute(&admin_pool)
                .await
                .expect("test database role must be allowed to create schemas");

            let isolated_options = options.options([("search_path", schema.as_str())]);
            let app_pool = PgPoolOptions::new()
                .max_connections(4)
                .connect_with(isolated_options)
                .await
                .expect("could not connect using the isolated test schema");

            run_migrations(&app_pool)
                .await
                .expect("catalogue migrations should apply to the test schema");
            seed_demo_data(&app_pool)
                .await
                .expect("demo seed should insert test customers and products");
            query(
                "INSERT INTO products \
                 (sku, name, description, price_cents, stock_available, active) \
                 VALUES ('AAA-INACTIVE', 'Inactive test item', '', 100, 1, FALSE)",
            )
            .execute(&app_pool)
            .await
            .expect("inactive product fixture should insert");

            Self {
                admin_pool,
                app_pool,
                schema,
            }
        }

        async fn cleanup(self) {
            self.app_pool.close().await;
            let drop_schema = format!("DROP SCHEMA \"{}\" CASCADE", self.schema);
            query(AssertSqlSafe(drop_schema))
                .execute(&self.admin_pool)
                .await
                .expect("test schema cleanup should succeed");
            self.admin_pool.close().await;
        }
    }

    async fn get(pool: &PgPool, uri: &str) -> axum::response::Response {
        let request = Request::builder()
            .uri(uri)
            .body(Body::empty())
            .expect("test request should be valid");
        create_router(pool.clone())
            .oneshot(request)
            .await
            .expect("router should return a response")
    }

    async fn json_body(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("response body should be readable");
        serde_json::from_slice(&bytes).expect("response should contain JSON")
    }

    fn unavailable_pool() -> PgPool {
        PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(2))
            .connect_lazy_with(
                PgConnectOptions::new()
                    .host("127.0.0.1")
                    .port(1)
                    .username("unused")
                    .database("unused"),
            )
    }

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
        database.cleanup().await;
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

        database.cleanup().await;
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
        database.cleanup().await;
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
}
