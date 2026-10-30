use std::{str::FromStr, time::Duration};

use motogear_domain::orders::{
    OrderRequest, OrderRequestError, OrderRequestItem, ValidatedOrderRequest,
};
use motogear_persistence::{
    PgPool,
    orders::{OrderSubmissionError, submit_order},
    run_migrations,
    seed::seed_demo_data,
};
use sqlx::{
    AssertSqlSafe,
    postgres::{PgConnectOptions, PgPoolOptions},
    query, query_as, query_scalar,
    types::Json,
};
use uuid::Uuid;

struct TestDatabase {
    admin_pool: sqlx::PgPool,
    pool: PgPool,
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

        let schema = format!("order_submission_test_{}", Uuid::new_v4().simple());
        let create_schema = format!("CREATE SCHEMA \"{schema}\"");
        query(AssertSqlSafe(create_schema))
            .execute(&admin_pool)
            .await
            .expect("test database role must be allowed to create schemas");

        let isolated_options = options.options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(isolated_options)
            .await
            .expect("could not connect using the isolated test schema");

        run_migrations(&pool)
            .await
            .expect("order migrations should apply to the test schema");
        seed_demo_data(&pool)
            .await
            .expect("demo data should be inserted into the test schema");

        Self {
            admin_pool,
            pool,
            schema,
        }
    }

    async fn cleanup(self) {
        self.pool.close().await;
        let drop_schema = format!("DROP SCHEMA \"{}\" CASCADE", self.schema);
        query(AssertSqlSafe(drop_schema))
            .execute(&self.admin_pool)
            .await
            .expect("test schema cleanup should succeed");
        self.admin_pool.close().await;
    }
}

async fn customer_id(pool: &PgPool) -> Uuid {
    query_scalar("SELECT id FROM customers WHERE email = 'alice@example.test'")
        .fetch_one(pool)
        .await
        .expect("seeded customer should exist")
}

async fn product_id(pool: &PgPool, sku: &str) -> Uuid {
    query_scalar("SELECT id FROM products WHERE sku = $1")
        .bind(sku)
        .fetch_one(pool)
        .await
        .expect("seeded product should exist")
}

fn valid_request(customer_id: Uuid, product_id: Uuid, quantity: i32) -> ValidatedOrderRequest {
    OrderRequest {
        customer_id,
        items: vec![OrderRequestItem {
            product_id,
            quantity,
        }],
    }
    .validate()
    .expect("test order request should be valid")
}

async fn count_orders_for_key(pool: &PgPool, customer_id: Uuid, key: Uuid) -> i64 {
    query_scalar("SELECT COUNT(*) FROM orders WHERE customer_id = $1 AND idempotency_key = $2")
        .bind(customer_id)
        .bind(key)
        .fetch_one(pool)
        .await
        .expect("order count query should succeed")
}

#[tokio::test]
async fn valid_submission_creates_pending_order_items_and_outbox_event() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let product = product_id(&database.pool, "HELMET-TOUR-01").await;
    let key = Uuid::new_v4();

    let outcome = submit_order(&database.pool, &valid_request(customer, product, 2), key)
        .await
        .expect("valid order should be saved");

    assert_eq!(outcome.status, "pending");
    assert!(!outcome.replayed);
    let (total_cents, status): (i64, String) =
        query_as("SELECT total_cents, status FROM orders WHERE id = $1")
            .bind(outcome.order_id)
            .fetch_one(&database.pool)
            .await
            .expect("saved order should be present");
    assert_eq!(total_cents, 29_800);
    assert_eq!(status, "pending");

    let (item_count, unit_price_cents): (i64, i64) =
        query_as("SELECT COUNT(*), MIN(unit_price_cents) FROM order_items WHERE order_id = $1")
            .bind(outcome.order_id)
            .fetch_one(&database.pool)
            .await
            .expect("saved order item should be present");
    assert_eq!(item_count, 1);
    assert_eq!(unit_price_cents, 14_900);

    let event: Json<serde_json::Value> =
        query_scalar("SELECT payload FROM outbox_events WHERE aggregate_id = $1")
            .bind(outcome.order_id)
            .fetch_one(&database.pool)
            .await
            .expect("order-created event should be present");
    assert_eq!(event.0["event_type"], "OrderCreated");
    assert_eq!(event.0["order_id"], outcome.order_id.to_string());
    assert!(event.0["occurred_at"].is_string());
    assert_eq!(event.0["data"]["customer_id"], customer.to_string());
    assert_eq!(
        event.0["data"]["items"][0]["product_id"],
        product.to_string()
    );
    database.cleanup().await;
}

#[tokio::test]
async fn same_key_and_payload_replays_the_existing_order() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let product = product_id(&database.pool, "HELMET-TOUR-01").await;
    let request = valid_request(customer, product, 1);
    let key = Uuid::new_v4();

    let first = submit_order(&database.pool, &request, key)
        .await
        .expect("first submission should succeed");
    let replay = submit_order(&database.pool, &request, key)
        .await
        .expect("identical retry should replay");

    assert_eq!(replay.order_id, first.order_id);
    assert_eq!(replay.status, first.status);
    assert!(replay.replayed);
    assert_eq!(count_orders_for_key(&database.pool, customer, key).await, 1);
    database.cleanup().await;
}

#[tokio::test]
async fn same_key_with_different_payload_returns_conflict() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let gloves = product_id(&database.pool, "GLOVES-ROAD-01").await;
    let key = Uuid::new_v4();

    submit_order(&database.pool, &valid_request(customer, helmet, 1), key)
        .await
        .expect("first submission should succeed");
    let result = submit_order(&database.pool, &valid_request(customer, gloves, 1), key).await;

    assert!(matches!(
        result,
        Err(OrderSubmissionError::IdempotencyConflict)
    ));
    assert_eq!(count_orders_for_key(&database.pool, customer, key).await, 1);
    database.cleanup().await;
}

#[tokio::test]
async fn missing_customer_is_rejected() {
    let database = TestDatabase::new().await;
    let product = product_id(&database.pool, "HELMET-TOUR-01").await;

    let result = submit_order(
        &database.pool,
        &valid_request(Uuid::new_v4(), product, 1),
        Uuid::new_v4(),
    )
    .await;

    assert!(matches!(
        result,
        Err(OrderSubmissionError::CustomerNotFound)
    ));
    database.cleanup().await;
}

#[tokio::test]
async fn missing_product_is_rejected() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let missing_product = Uuid::new_v4();

    let result = submit_order(
        &database.pool,
        &valid_request(customer, missing_product, 1),
        Uuid::new_v4(),
    )
    .await;

    assert!(matches!(
        result,
        Err(OrderSubmissionError::ProductsNotOrderable(ids)) if ids == vec![missing_product]
    ));
    database.cleanup().await;
}

#[tokio::test]
async fn inactive_product_is_rejected() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let inactive_product = Uuid::new_v4();
    query(
        "INSERT INTO products (id, sku, name, price_cents, stock_available, active) \
         VALUES ($1, $2, 'Inactive item', 100, 1, FALSE)",
    )
    .bind(inactive_product)
    .bind(format!("INACTIVE-{}", Uuid::new_v4()))
    .execute(&database.pool)
    .await
    .expect("inactive test product should be inserted");

    let result = submit_order(
        &database.pool,
        &valid_request(customer, inactive_product, 1),
        Uuid::new_v4(),
    )
    .await;

    assert!(matches!(
        result,
        Err(OrderSubmissionError::ProductsNotOrderable(ids)) if ids == vec![inactive_product]
    ));
    database.cleanup().await;
}

#[test]
fn duplicate_product_lines_are_rejected_by_domain_validation() {
    let product_id = Uuid::new_v4();
    let result = OrderRequest {
        customer_id: Uuid::new_v4(),
        items: vec![
            OrderRequestItem {
                product_id,
                quantity: 1,
            },
            OrderRequestItem {
                product_id,
                quantity: 2,
            },
        ],
    }
    .validate();

    assert_eq!(
        result.unwrap_err(),
        OrderRequestError::DuplicateProduct(product_id)
    );
}

#[tokio::test]
async fn order_total_uses_current_catalogue_prices() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let product = product_id(&database.pool, "HELMET-TOUR-01").await;
    query("UPDATE products SET price_cents = 12345 WHERE id = $1")
        .bind(product)
        .execute(&database.pool)
        .await
        .expect("product price should be updated for the test");

    let outcome = submit_order(
        &database.pool,
        &valid_request(customer, product, 2),
        Uuid::new_v4(),
    )
    .await
    .expect("order should use the updated product price");
    let (total, unit_price): (i64, i64) = query_as(
        "SELECT o.total_cents, i.unit_price_cents \
         FROM orders o JOIN order_items i ON i.order_id = o.id \
         WHERE o.id = $1",
    )
    .bind(outcome.order_id)
    .fetch_one(&database.pool)
    .await
    .expect("order and item should be saved");

    assert_eq!(unit_price, 12_345);
    assert_eq!(total, 24_690);
    database.cleanup().await;
}

#[tokio::test]
async fn concurrent_identical_submissions_create_one_order() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let product = product_id(&database.pool, "HELMET-TOUR-01").await;
    let request = valid_request(customer, product, 1);
    let key = Uuid::new_v4();
    let first_pool = database.pool.clone();
    let second_pool = database.pool.clone();
    let first_request = request.clone();
    let second_request = request;

    let first = tokio::spawn(async move { submit_order(&first_pool, &first_request, key).await });
    let second =
        tokio::spawn(async move { submit_order(&second_pool, &second_request, key).await });
    let first = first
        .await
        .expect("first task should complete")
        .expect("first order should succeed");
    let second = second
        .await
        .expect("second task should complete")
        .expect("retry should succeed");

    assert_eq!(first.order_id, second.order_id);
    assert_ne!(first.replayed, second.replayed);
    assert_eq!(count_orders_for_key(&database.pool, customer, key).await, 1);
    let event_count: i64 = query_scalar("SELECT COUNT(*) FROM outbox_events")
        .fetch_one(&database.pool)
        .await
        .expect("outbox count query should succeed");
    assert_eq!(event_count, 1);
    database.cleanup().await;
}

#[tokio::test]
async fn concurrent_different_payloads_with_same_key_create_one_and_conflict_once() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let gloves = product_id(&database.pool, "GLOVES-ROAD-01").await;
    let key = Uuid::new_v4();
    let first_pool = database.pool.clone();
    let second_pool = database.pool.clone();
    let first_request = valid_request(customer, helmet, 1);
    let second_request = valid_request(customer, gloves, 1);

    let first = tokio::spawn(async move { submit_order(&first_pool, &first_request, key).await });
    let second =
        tokio::spawn(async move { submit_order(&second_pool, &second_request, key).await });
    let first = first.await.expect("first task should complete");
    let second = second.await.expect("second task should complete");

    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert!(
        matches!(first, Err(OrderSubmissionError::IdempotencyConflict))
            || matches!(second, Err(OrderSubmissionError::IdempotencyConflict))
    );
    assert_eq!(count_orders_for_key(&database.pool, customer, key).await, 1);
    let event_count: i64 = query_scalar("SELECT COUNT(*) FROM outbox_events")
        .fetch_one(&database.pool)
        .await
        .expect("outbox count query should succeed");
    assert_eq!(event_count, 1);
    database.cleanup().await;
}

#[tokio::test]
async fn terminating_connection_before_commit_rolls_back_order_items_and_event() {
    let database = TestDatabase::new().await;
    query(AssertSqlSafe(
        "CREATE FUNCTION pause_order_outbox_insert() RETURNS trigger \
         LANGUAGE plpgsql AS $$ BEGIN \
             PERFORM set_config('application_name', 'motogear_order_crash_test', FALSE); \
             PERFORM pg_sleep(30); \
             RETURN NEW; \
         END; $$"
            .to_owned(),
    ))
    .execute(&database.pool)
    .await
    .expect("test trigger function should be created");
    query(AssertSqlSafe(
        "CREATE TRIGGER pause_order_outbox_insert \
         BEFORE INSERT ON outbox_events FOR EACH ROW \
         EXECUTE FUNCTION pause_order_outbox_insert()"
            .to_owned(),
    ))
    .execute(&database.pool)
    .await
    .expect("test trigger should be created");

    let customer = customer_id(&database.pool).await;
    let product = product_id(&database.pool, "HELMET-TOUR-01").await;
    let key = Uuid::new_v4();
    let pool = database.pool.clone();
    let request = valid_request(customer, product, 1);
    let submission = tokio::spawn(async move { submit_order(&pool, &request, key).await });

    let backend_pid = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let pid = query_scalar::<_, i32>(
                "SELECT pid FROM pg_stat_activity \
                 WHERE application_name = 'motogear_order_crash_test' \
                   AND state = 'active' \
                   AND query LIKE 'INSERT INTO outbox_events%' \
                 LIMIT 1",
            )
            .fetch_optional(&database.admin_pool)
            .await
            .expect("activity query should succeed");
            if let Some(pid) = pid {
                break pid;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("submission should reach the outbox insert before timeout");
    let terminated: bool = query_scalar("SELECT pg_terminate_backend($1)")
        .bind(backend_pid)
        .fetch_one(&database.admin_pool)
        .await
        .expect("database should terminate the submission connection");
    assert!(terminated);

    let result = submission
        .await
        .expect("submission task should complete after connection termination");

    assert!(matches!(result, Err(OrderSubmissionError::Database(_))));
    assert_eq!(count_orders_for_key(&database.pool, customer, key).await, 0);
    let item_count: i64 = query_scalar("SELECT COUNT(*) FROM order_items")
        .fetch_one(&database.pool)
        .await
        .expect("item count query should succeed");
    let event_count: i64 = query_scalar("SELECT COUNT(*) FROM outbox_events")
        .fetch_one(&database.pool)
        .await
        .expect("outbox count query should succeed");
    assert_eq!(item_count, 0);
    assert_eq!(event_count, 0);
    database.cleanup().await;
}

#[tokio::test]
async fn submitting_an_order_does_not_change_stock() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let product = product_id(&database.pool, "HELMET-TOUR-01").await;
    let before: i32 = query_scalar("SELECT stock_available FROM products WHERE id = $1")
        .bind(product)
        .fetch_one(&database.pool)
        .await
        .expect("seeded stock should be present");

    submit_order(
        &database.pool,
        &valid_request(customer, product, 2),
        Uuid::new_v4(),
    )
    .await
    .expect("valid order should be saved");
    let after: i32 = query_scalar("SELECT stock_available FROM products WHERE id = $1")
        .bind(product)
        .fetch_one(&database.pool)
        .await
        .expect("stock should still be present");

    assert_eq!(after, before);
    database.cleanup().await;
}
