#![allow(dead_code)]

use std::{str::FromStr, thread, time::Duration};

use axum::{
    body::{Body, to_bytes},
    http::{HeaderValue, Request},
};
use motogear_api::create_router;
use motogear_persistence::{PgPool, run_migrations, seed::seed_demo_data};
use serde_json::Value;
use sqlx::{
    AssertSqlSafe,
    postgres::{PgConnectOptions, PgPoolOptions},
    query, query_scalar,
};
use tower::ServiceExt;
use uuid::Uuid;

pub struct TestDatabase {
    pub app_pool: PgPool,
    _schema_cleanup: SchemaCleanup,
}

impl TestDatabase {
    pub async fn new() -> Self {
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

        let schema = format!("api_test_{}", Uuid::new_v4().simple());
        let create_schema = format!("CREATE SCHEMA \"{schema}\"");
        // This identifier comes from a UUID, contains only hex digits,
        // and is quoted; SQLx requires an explicit assertion for DDL.
        query(AssertSqlSafe(create_schema))
            .execute(&admin_pool)
            .await
            .expect("test database role must be allowed to create schemas");

        // The guard is established immediately after CREATE SCHEMA. If any
        // later setup step panics, its Drop implementation removes the schema.
        let schema_cleanup = SchemaCleanup {
            options: options.clone(),
            schema: schema.clone(),
            application_name: format!("api_test_{}", Uuid::new_v4().simple()),
        };
        admin_pool.close().await;
        let isolated_options = options
            .options([("search_path", schema.as_str())])
            .application_name(&schema_cleanup.application_name);
        let app_pool = PgPoolOptions::new()
            .max_connections(8)
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
            app_pool,
            _schema_cleanup: schema_cleanup,
        }
    }
}

struct SchemaCleanup {
    options: PgConnectOptions,
    schema: String,
    application_name: String,
}

impl Drop for SchemaCleanup {
    fn drop(&mut self) {
        let options = self.options.clone();
        let schema = self.schema.clone();
        let application_name = self.application_name.clone();
        let cleanup = thread::Builder::new()
            .name("api-test-schema-cleanup".to_owned())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        eprintln!("could not start test schema cleanup runtime: {error}");
                        return;
                    }
                };
                let result = runtime.block_on(async move {
                    let pool = PgPoolOptions::new()
                        .max_connections(1)
                        .connect_with(options)
                        .await?;
                    query_scalar::<_, bool>(
                        "SELECT pg_terminate_backend(pid) \
                         FROM pg_stat_activity \
                         WHERE application_name = $1 AND pid <> pg_backend_pid()",
                    )
                    .bind(application_name)
                    .fetch_all(&pool)
                    .await?;
                    let drop_schema = format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE");
                    query(AssertSqlSafe(drop_schema)).execute(&pool).await?;
                    pool.close().await;
                    Ok::<(), sqlx::Error>(())
                });
                if let Err(error) = result {
                    eprintln!("could not remove API test schema: {error}");
                }
            });

        match cleanup {
            Ok(cleanup) => {
                if cleanup.join().is_err() {
                    eprintln!("API test schema cleanup thread panicked");
                }
            }
            Err(error) => eprintln!("could not start API test schema cleanup thread: {error}"),
        }
    }
}

pub async fn customer_id(pool: &PgPool, email: &str) -> Uuid {
    query_scalar("SELECT id FROM customers WHERE email = $1")
        .bind(email)
        .fetch_one(pool)
        .await
        .expect("seeded customer should exist")
}

pub async fn product_id(pool: &PgPool, sku: &str) -> Uuid {
    query_scalar("SELECT id FROM products WHERE sku = $1")
        .bind(sku)
        .fetch_one(pool)
        .await
        .expect("seeded product should exist")
}

pub async fn get(pool: &PgPool, uri: &str) -> axum::response::Response {
    let request = Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("test request should be valid");
    create_router(pool.clone())
        .oneshot(request)
        .await
        .expect("router should return a response")
}

pub async fn post_order(
    pool: &PgPool,
    body: &str,
    idempotency_key: Option<&str>,
) -> axum::response::Response {
    let keys = idempotency_key.into_iter().collect::<Vec<_>>();
    post_order_with_headers(pool, body, Some("application/json"), &keys).await
}

pub async fn post_order_with_headers(
    pool: &PgPool,
    body: &str,
    content_type: Option<&str>,
    idempotency_keys: &[&str],
) -> axum::response::Response {
    let mut builder = Request::builder().method("POST").uri("/api/v1/orders");
    if let Some(content_type) = content_type {
        builder = builder.header("Content-Type", content_type);
    }
    let mut request = builder
        .body(Body::from(body.to_owned()))
        .expect("test order request should be valid");
    for key in idempotency_keys {
        request.headers_mut().append(
            "Idempotency-Key",
            HeaderValue::from_str(key).expect("valid test header"),
        );
    }
    create_router(pool.clone())
        .oneshot(request)
        .await
        .expect("router should return a response")
}

pub fn order_body(customer_id: Uuid, items: &[(Uuid, i32)]) -> String {
    serde_json::json!({
        "customer_id": customer_id.to_string(),
        "items": items.iter().map(|(product_id, quantity)| {
            serde_json::json!({
                "product_id": product_id.to_string(),
                "quantity": quantity,
            })
        }).collect::<Vec<_>>(),
    })
    .to_string()
}

pub async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body should be readable");
    serde_json::from_slice(&bytes).expect("response should contain JSON")
}

pub fn unavailable_pool() -> PgPool {
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
