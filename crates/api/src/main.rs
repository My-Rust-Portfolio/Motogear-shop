use std::{env, error::Error, fmt::Display};

use axum::{
    Json, Router,
    extract::{Query, State, rejection::QueryRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use motogear_persistence::{
    PgPool,
    catalogue::{CustomerRecord, ProductRecord, list_active_products, list_customers},
    config::postgres_options_from_env,
    connect_with, schema_is_ready,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::services::{ServeDir, ServeFile};
use uuid::Uuid;

const DEFAULT_PAGE_SIZE: u32 = 20;
const MAX_PAGE_SIZE: u32 = 100;
const MAX_OFFSET: u32 = 100_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Loading `.env` is optional so Compose can provide variables directly.
    let _ = dotenvy::dotenv();

    let pool = connect_with(postgres_options_from_env()?).await?;
    let bind_address = env::var("API_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());
    let listener = TcpListener::bind(&bind_address).await?;

    println!("MotoGear API listening on http://{bind_address}");
    axum::serve(listener, routes(pool)).await?;
    Ok(())
}

fn routes(pool: PgPool) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/api/v1/customers", get(customers))
        .route("/api/v1/products", get(products))
        .route_service("/", ServeFile::new("web/index.html"))
        .nest_service("/static", ServeDir::new("web"))
        .with_state(pool)
}

async fn live() -> (StatusCode, &'static str) {
    (StatusCode::OK, "live")
}

async fn ready(State(pool): State<PgPool>) -> (StatusCode, &'static str) {
    match schema_is_ready(&pool).await {
        Ok(true) => (StatusCode::OK, "ready"),
        Ok(false) => {
            eprintln!("database readiness check failed: migrations are incomplete");
            (StatusCode::SERVICE_UNAVAILABLE, "not ready")
        }
        Err(error) => {
            eprintln!("database readiness check failed: {error}");
            (StatusCode::SERVICE_UNAVAILABLE, "not ready")
        }
    }
}

async fn customers(
    State(pool): State<PgPool>,
    query: Result<Query<PageQuery>, QueryRejection>,
) -> Response {
    let (limit, offset) = match parse_pagination(query) {
        Ok(page) => page,
        Err(response) => return response,
    };

    match list_customers(&pool, limit, offset).await {
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
    State(pool): State<PgPool>,
    query: Result<Query<PageQuery>, QueryRejection>,
) -> Response {
    let (limit, offset) = match parse_pagination(query) {
        Ok(page) => page,
        Err(response) => return response,
    };

    match list_active_products(&pool, limit, offset).await {
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

#[derive(Debug, Deserialize)]
struct PageQuery {
    limit: Option<u32>,
    offset: Option<u32>,
}

fn parse_pagination(
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

#[derive(Serialize)]
struct CustomerResponse {
    id: Uuid,
    name: String,
    email: String,
}

impl From<CustomerRecord> for CustomerResponse {
    fn from(record: CustomerRecord) -> Self {
        Self {
            id: record.id,
            name: record.name,
            email: record.email,
        }
    }
}

#[derive(Serialize)]
struct ProductResponse {
    id: Uuid,
    /// Stock Keeping Unit
    sku: String,
    name: String,
    description: String,
    price_cents: i64,
    currency: &'static str,
    stock_available: i32,
}

impl From<ProductRecord> for ProductResponse {
    fn from(record: ProductRecord) -> Self {
        Self {
            id: record.id,
            sku: record.sku,
            name: record.name,
            description: record.description,
            price_cents: record.price_cents,
            currency: "EUR",
            stock_available: record.stock_available,
        }
    }
}

#[derive(Serialize)]
struct ErrorEnvelope<'a> {
    error: ErrorDetails<'a>,
}

#[derive(Serialize)]
struct ErrorDetails<'a> {
    code: &'a str,
    message: &'a str,
}

fn api_error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(ErrorEnvelope {
            error: ErrorDetails { code, message },
        }),
    )
        .into_response()
}

fn database_error_response(context: &str, error: impl Display) -> Response {
    eprintln!("{context}: {error}");
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "database_unavailable",
        "The database is temporarily unavailable.",
    )
}

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

    use super::{PgPool, StatusCode, Uuid, routes};

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
        routes(pool.clone())
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
