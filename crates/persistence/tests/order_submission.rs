use std::{str::FromStr, time::Duration};

use motogear_domain::orders::{
    OrderRequest, OrderRequestError, OrderRequestItem, ValidatedOrderRequest,
};
use motogear_persistence::{
    PgPool,
    events::outbox::{
        mark_outbox_event_published, next_unpublished_event, record_outbox_publish_failure,
    },
    fulfillment::{
        FULFILLMENT_CONSUMER_NAME, FulfillmentOutcome, FulfillmentProcessError, OrderAcceptedData,
        OrderAcceptedEvent, OrderAcceptedItem, fulfillment_event_is_processed,
        process_order_accepted,
    },
    inventory::{InventoryOutcome, OrderCreatedEvent, process_order_created},
    notifications::{
        NOTIFICATIONS_CONSUMER_NAME, NotificationOutcome, NotificationProcessError,
        OrderLifecycleEvent, process_order_lifecycle_event,
    },
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
use std::sync::Arc;
use tokio::sync::Barrier;
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

async fn order_created_event(pool: &PgPool, order_id: Uuid) -> OrderCreatedEvent {
    let payload: Json<serde_json::Value> = query_scalar(
        "SELECT payload FROM outbox_events \
         WHERE aggregate_id = $1 AND event_type = 'OrderCreated'",
    )
    .bind(order_id)
    .fetch_one(pool)
    .await
    .expect("OrderCreated event should exist");

    serde_json::from_value(payload.0).expect("stored event should match the OrderCreated contract")
}

async fn order_accepted_event(pool: &PgPool, order_id: Uuid) -> OrderAcceptedEvent {
    let payload: Json<serde_json::Value> = query_scalar(
        "SELECT payload FROM outbox_events \
         WHERE aggregate_id = $1 AND event_type = 'OrderAccepted'",
    )
    .bind(order_id)
    .fetch_one(pool)
    .await
    .expect("OrderAccepted event should exist");

    serde_json::from_value(payload.0).expect("stored event should match the OrderAccepted contract")
}

async fn order_lifecycle_event(
    pool: &PgPool,
    order_id: Uuid,
    event_type: &str,
) -> OrderLifecycleEvent {
    let payload: Json<serde_json::Value> = query_scalar(
        "SELECT payload FROM outbox_events \
         WHERE aggregate_id = $1 AND event_type = $2",
    )
    .bind(order_id)
    .bind(event_type)
    .fetch_one(pool)
    .await
    .expect("order lifecycle event should exist");

    serde_json::from_value(payload.0).expect("event should match the lifecycle envelope")
}

fn accepted_event_for_order_created(event: OrderCreatedEvent) -> OrderAcceptedEvent {
    OrderAcceptedEvent {
        event_id: Uuid::new_v4(),
        event_type: "OrderAccepted".to_owned(),
        schema_version: 1,
        order_id: event.order_id,
        correlation_id: event.correlation_id,
        data: OrderAcceptedData {
            customer_id: event.data.customer_id,
            items: event
                .data
                .items
                .into_iter()
                .map(|item| OrderAcceptedItem {
                    product_id: item.product_id,
                    quantity: item.quantity,
                })
                .collect(),
        },
    }
}

async fn make_accepted_order(
    pool: &PgPool,
    customer_id: Uuid,
    product_id: Uuid,
    quantity: i32,
) -> (Uuid, OrderAcceptedEvent) {
    let submitted = submit_order(
        pool,
        &valid_request(customer_id, product_id, quantity),
        Uuid::new_v4(),
    )
    .await
    .expect("order submission should succeed");
    let created_event = order_created_event(pool, submitted.order_id).await;
    assert_eq!(
        process_order_created(pool, &created_event)
            .await
            .expect("inventory processing should succeed"),
        InventoryOutcome::Accepted
    );
    let accepted_event = order_accepted_event(pool, submitted.order_id).await;
    (submitted.order_id, accepted_event)
}

async fn stock_available(pool: &PgPool, product_id: Uuid) -> i32 {
    query_scalar("SELECT stock_available FROM products WHERE id = $1")
        .bind(product_id)
        .fetch_one(pool)
        .await
        .expect("product stock should be queryable")
}

async fn count_order_reservations(pool: &PgPool, order_id: Uuid) -> i64 {
    query_scalar("SELECT COUNT(*) FROM stock_reservations WHERE order_id = $1")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("reservation count query should succeed")
}

async fn count_order_events(pool: &PgPool, order_id: Uuid, event_type: &str) -> i64 {
    query_scalar("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = $2")
        .bind(order_id)
        .bind(event_type)
        .fetch_one(pool)
        .await
        .expect("outbox event count query should succeed")
}

async fn count_processed_inventory_events(pool: &PgPool, event_id: Uuid) -> i64 {
    query_scalar(
        "SELECT COUNT(*) FROM processed_events \
         WHERE consumer_name = 'motogear-inventory-v1' AND event_id = $1",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await
    .expect("processed-event count query should succeed")
}

async fn count_processed_fulfillment_events(pool: &PgPool, event_id: Uuid) -> i64 {
    query_scalar("SELECT COUNT(*) FROM processed_events WHERE consumer_name = $1 AND event_id = $2")
        .bind(FULFILLMENT_CONSUMER_NAME)
        .bind(event_id)
        .fetch_one(pool)
        .await
        .expect("fulfillment processed-event count query should succeed")
}

async fn count_notifications_for_event(pool: &PgPool, event_id: Uuid) -> i64 {
    query_scalar("SELECT COUNT(*) FROM notifications WHERE source_event_id = $1")
        .bind(event_id)
        .fetch_one(pool)
        .await
        .expect("notification count query should succeed")
}

async fn count_processed_notification_events(pool: &PgPool, event_id: Uuid) -> i64 {
    query_scalar("SELECT COUNT(*) FROM processed_events WHERE consumer_name = $1 AND event_id = $2")
        .bind(NOTIFICATIONS_CONSUMER_NAME)
        .bind(event_id)
        .fetch_one(pool)
        .await
        .expect("notification processed-event count query should succeed")
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
async fn outbox_publisher_state_transitions_are_recorded() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let product = product_id(&database.pool, "HELMET-TOUR-01").await;
    let order = submit_order(
        &database.pool,
        &valid_request(customer, product, 1),
        Uuid::new_v4(),
    )
    .await
    .expect("valid order should be saved");

    let event = next_unpublished_event(&database.pool)
        .await
        .expect("outbox query should succeed")
        .expect("created order should have an unpublished event");
    assert_eq!(event.aggregate_id, order.order_id);
    assert_eq!(event.publish_attempts, 0);

    assert!(
        record_outbox_publish_failure(&database.pool, event.event_id, "broker unavailable")
            .await
            .expect("failure details should be recorded")
    );
    let event_after_failure = next_unpublished_event(&database.pool)
        .await
        .expect("outbox query should succeed after a failed attempt")
        .expect("failed event should remain unpublished");
    assert_eq!(event_after_failure.event_id, event.event_id);
    assert_eq!(event_after_failure.publish_attempts, 1);

    assert!(
        mark_outbox_event_published(&database.pool, event.event_id)
            .await
            .expect("published event should be marked")
    );
    assert!(
        next_unpublished_event(&database.pool)
            .await
            .expect("outbox query should succeed after publishing")
            .is_none(),
        "published events should no longer be selected"
    );

    let (published, attempts): (bool, i32) = query_as(
        "SELECT published_at IS NOT NULL, publish_attempts \
         FROM outbox_events WHERE event_id = $1",
    )
    .bind(event.event_id)
    .fetch_one(&database.pool)
    .await
    .expect("outbox row should remain available for inspection");
    assert!(published);
    assert_eq!(attempts, 1);

    database.cleanup().await;
}

#[tokio::test]
async fn inventory_accepts_order_and_reserves_all_requested_stock() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let gloves = product_id(&database.pool, "GLOVES-ROAD-01").await;
    let outcome = submit_order(
        &database.pool,
        &OrderRequest {
            customer_id: customer,
            items: vec![
                OrderRequestItem {
                    product_id: helmet,
                    quantity: 1,
                },
                OrderRequestItem {
                    product_id: gloves,
                    quantity: 2,
                },
            ],
        }
        .validate()
        .expect("valid multi-product request should pass domain validation"),
        Uuid::new_v4(),
    )
    .await
    .expect("valid order should be saved");
    let event = order_created_event(&database.pool, outcome.order_id).await;

    assert_eq!(
        process_order_created(&database.pool, &event)
            .await
            .expect("inventory processing should succeed"),
        InventoryOutcome::Accepted
    );
    assert_eq!(stock_available(&database.pool, helmet).await, 2);
    assert_eq!(stock_available(&database.pool, gloves).await, 6);
    assert_eq!(
        count_order_reservations(&database.pool, outcome.order_id).await,
        2
    );
    assert_eq!(
        count_order_events(&database.pool, outcome.order_id, "OrderAccepted").await,
        1
    );

    let (status, rejection_reason): (String, Option<String>) =
        query_as("SELECT status, rejection_reason FROM orders WHERE id = $1")
            .bind(outcome.order_id)
            .fetch_one(&database.pool)
            .await
            .expect("inventory-updated order should be present");
    assert_eq!(status, "accepted");
    assert_eq!(rejection_reason, None);
    assert_eq!(
        count_processed_inventory_events(&database.pool, event.event_id).await,
        1
    );

    database.cleanup().await;
}

#[tokio::test]
async fn inventory_rejects_short_order_without_partial_stock_changes() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let phone_mount = product_id(&database.pool, "MOUNT-PHONE-01").await;
    let outcome = submit_order(
        &database.pool,
        &OrderRequest {
            customer_id: customer,
            items: vec![
                OrderRequestItem {
                    product_id: helmet,
                    quantity: 1,
                },
                OrderRequestItem {
                    product_id: phone_mount,
                    quantity: 2,
                },
            ],
        }
        .validate()
        .expect("valid multi-product request should pass domain validation"),
        Uuid::new_v4(),
    )
    .await
    .expect("order should persist before inventory is checked");
    let event = order_created_event(&database.pool, outcome.order_id).await;

    let result = process_order_created(&database.pool, &event)
        .await
        .expect("stock shortage should be a business rejection");
    assert!(matches!(result, InventoryOutcome::Rejected { .. }));
    assert_eq!(stock_available(&database.pool, helmet).await, 3);
    assert_eq!(stock_available(&database.pool, phone_mount).await, 1);
    assert_eq!(
        count_order_reservations(&database.pool, outcome.order_id).await,
        0
    );
    assert_eq!(
        count_order_events(&database.pool, outcome.order_id, "OrderRejected").await,
        1
    );

    let (status, rejection_reason): (String, Option<String>) =
        query_as("SELECT status, rejection_reason FROM orders WHERE id = $1")
            .bind(outcome.order_id)
            .fetch_one(&database.pool)
            .await
            .expect("rejected order should be present");
    assert_eq!(status, "rejected");
    assert!(
        rejection_reason
            .as_deref()
            .is_some_and(|reason| reason.contains(&phone_mount.to_string()))
    );

    database.cleanup().await;
}

#[tokio::test]
async fn duplicate_inventory_event_does_not_reserve_stock_twice() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let outcome = submit_order(
        &database.pool,
        &valid_request(customer, helmet, 1),
        Uuid::new_v4(),
    )
    .await
    .expect("valid order should be saved");
    let event = order_created_event(&database.pool, outcome.order_id).await;

    assert_eq!(
        process_order_created(&database.pool, &event)
            .await
            .expect("first processing should succeed"),
        InventoryOutcome::Accepted
    );
    assert_eq!(
        process_order_created(&database.pool, &event)
            .await
            .expect("replayed event should be a safe no-op"),
        InventoryOutcome::Duplicate
    );
    assert_eq!(stock_available(&database.pool, helmet).await, 2);
    assert_eq!(
        count_order_reservations(&database.pool, outcome.order_id).await,
        1
    );
    assert_eq!(
        count_order_events(&database.pool, outcome.order_id, "OrderAccepted").await,
        1
    );

    database.cleanup().await;
}

#[tokio::test]
async fn inventory_ignores_order_already_accepted() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let order = submit_order(
        &database.pool,
        &valid_request(customer, helmet, 1),
        Uuid::new_v4(),
    )
    .await
    .expect("valid order should be saved");
    let event = order_created_event(&database.pool, order.order_id).await;
    query("UPDATE orders SET status = 'accepted' WHERE id = $1")
        .bind(order.order_id)
        .execute(&database.pool)
        .await
        .expect("test order status should be updated");

    assert_eq!(
        process_order_created(&database.pool, &event)
            .await
            .expect("non-pending order should be ignored"),
        InventoryOutcome::IgnoredNoLongerPending
    );
    assert_eq!(stock_available(&database.pool, helmet).await, 3);
    assert_eq!(
        count_order_reservations(&database.pool, order.order_id).await,
        0
    );
    assert_eq!(
        count_order_events(&database.pool, order.order_id, "OrderAccepted").await,
        0
    );

    database.cleanup().await;
}

#[tokio::test]
async fn inventory_ignores_order_already_rejected() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let order = submit_order(
        &database.pool,
        &valid_request(customer, helmet, 1),
        Uuid::new_v4(),
    )
    .await
    .expect("valid order should be saved");
    let event = order_created_event(&database.pool, order.order_id).await;
    query("UPDATE orders SET status = 'rejected', rejection_reason = 'test' WHERE id = $1")
        .bind(order.order_id)
        .execute(&database.pool)
        .await
        .expect("test order status should be updated");

    assert_eq!(
        process_order_created(&database.pool, &event)
            .await
            .expect("non-pending order should be ignored"),
        InventoryOutcome::IgnoredNoLongerPending
    );
    assert_eq!(stock_available(&database.pool, helmet).await, 3);
    assert_eq!(
        count_order_reservations(&database.pool, order.order_id).await,
        0
    );
    assert_eq!(
        count_order_events(&database.pool, order.order_id, "OrderAccepted").await,
        0
    );

    database.cleanup().await;
}

#[tokio::test]
async fn mismatched_event_items_roll_back_inventory_deduplication_marker() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let order = submit_order(
        &database.pool,
        &valid_request(customer, helmet, 1),
        Uuid::new_v4(),
    )
    .await
    .expect("valid order should be saved");
    let mut event = order_created_event(&database.pool, order.order_id).await;
    event.data.items[0].quantity = 2;

    assert!(matches!(
        process_order_created(&database.pool, &event).await,
        Err(motogear_persistence::inventory::InventoryProcessError::InvalidEvent(_))
    ));
    assert_eq!(stock_available(&database.pool, helmet).await, 3);
    assert_eq!(
        count_order_reservations(&database.pool, order.order_id).await,
        0
    );
    assert_eq!(
        count_order_events(&database.pool, order.order_id, "OrderAccepted").await,
        0
    );
    assert_eq!(
        count_processed_inventory_events(&database.pool, event.event_id).await,
        0
    );

    database.cleanup().await;
}

#[tokio::test]
async fn mismatched_event_customer_rolls_back_inventory_deduplication_marker() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let another_customer =
        query_scalar("SELECT id FROM customers WHERE email = 'bob@example.test'")
            .fetch_one(&database.pool)
            .await
            .expect("second seeded customer should exist");
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let order = submit_order(
        &database.pool,
        &valid_request(customer, helmet, 1),
        Uuid::new_v4(),
    )
    .await
    .expect("valid order should be saved");
    let mut event = order_created_event(&database.pool, order.order_id).await;
    event.data.customer_id = another_customer;

    assert!(matches!(
        process_order_created(&database.pool, &event).await,
        Err(motogear_persistence::inventory::InventoryProcessError::InvalidEvent(_))
    ));
    assert_eq!(stock_available(&database.pool, helmet).await, 3);
    assert_eq!(
        count_order_reservations(&database.pool, order.order_id).await,
        0
    );
    assert_eq!(
        count_order_events(&database.pool, order.order_id, "OrderAccepted").await,
        0
    );
    assert_eq!(
        count_processed_inventory_events(&database.pool, event.event_id).await,
        0
    );

    database.cleanup().await;
}

#[tokio::test]
async fn concurrent_duplicate_inventory_events_reserve_stock_once() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let order = submit_order(
        &database.pool,
        &valid_request(customer, helmet, 1),
        Uuid::new_v4(),
    )
    .await
    .expect("valid order should be saved");
    let event = order_created_event(&database.pool, order.order_id).await;
    let barrier = Arc::new(Barrier::new(2));

    let first_pool = database.pool.clone();
    let first_event = event.clone();
    let first_barrier = barrier.clone();
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        process_order_created(&first_pool, &first_event).await
    });

    let second_pool = database.pool.clone();
    let second_event = event.clone();
    let second_barrier = barrier.clone();
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        process_order_created(&second_pool, &second_event).await
    });

    let first = first
        .await
        .expect("first inventory task should finish")
        .expect("first task should succeed");
    let second = second
        .await
        .expect("second inventory task should finish")
        .expect("second task should succeed");
    assert!(
        (first == InventoryOutcome::Accepted && second == InventoryOutcome::Duplicate)
            || (first == InventoryOutcome::Duplicate && second == InventoryOutcome::Accepted)
    );
    assert_eq!(stock_available(&database.pool, helmet).await, 2);
    assert_eq!(
        count_order_reservations(&database.pool, order.order_id).await,
        1
    );
    assert_eq!(
        count_order_events(&database.pool, order.order_id, "OrderAccepted").await,
        1
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

#[tokio::test]
async fn fulfillment_ships_accepted_order_and_emits_order_shipped() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let (order_id, event) = make_accepted_order(&database.pool, customer, helmet, 1).await;
    let stock_before_shipment = stock_available(&database.pool, helmet).await;
    assert!(
        !fulfillment_event_is_processed(&database.pool, event.event_id)
            .await
            .expect("deduplication marker lookup should succeed")
    );

    let result = process_order_accepted(&database.pool, &event)
        .await
        .expect("accepted order should be fulfilled");
    let tracking_reference = format!("SIM-{}", order_id.simple().to_string().to_ascii_uppercase());
    assert_eq!(
        result,
        FulfillmentOutcome::Shipped {
            tracking_reference: tracking_reference.clone()
        }
    );

    let (status,): (String,) = query_as("SELECT status FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(&database.pool)
        .await
        .expect("order status should be queryable");
    assert_eq!(status, "shipped");
    let (shipment_count, saved_tracking): (i64, String) = query_as(
        "SELECT COUNT(*), COALESCE(MAX(tracking_reference), '') \
         FROM shipments WHERE order_id = $1",
    )
    .bind(order_id)
    .fetch_one(&database.pool)
    .await
    .expect("shipment row should be queryable");
    assert_eq!(shipment_count, 1);
    assert_eq!(saved_tracking, tracking_reference);
    assert_eq!(
        count_order_events(&database.pool, order_id, "OrderShipped").await,
        1
    );
    assert_eq!(
        stock_available(&database.pool, helmet).await,
        stock_before_shipment
    );
    assert_eq!(
        count_processed_fulfillment_events(&database.pool, event.event_id).await,
        1
    );
    assert!(
        fulfillment_event_is_processed(&database.pool, event.event_id)
            .await
            .expect("deduplication marker lookup should succeed")
    );

    let payload: Json<serde_json::Value> = query_scalar(
        "SELECT payload FROM outbox_events \
         WHERE aggregate_id = $1 AND event_type = 'OrderShipped'",
    )
    .bind(order_id)
    .fetch_one(&database.pool)
    .await
    .expect("OrderShipped payload should be queryable");
    assert_eq!(payload.0["causation_id"], event.event_id.to_string());
    assert_eq!(payload.0["data"]["customer_id"], customer.to_string());
    assert_eq!(payload.0["data"]["total_cents"], 14_900);
    assert_eq!(payload.0["data"]["currency"], "EUR");
    assert_eq!(payload.0["data"]["tracking_reference"], tracking_reference);

    database.cleanup().await;
}

#[tokio::test]
async fn duplicate_fulfillment_delivery_does_not_create_a_second_shipment() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let (order_id, event) = make_accepted_order(&database.pool, customer, helmet, 1).await;

    assert!(matches!(
        process_order_accepted(&database.pool, &event).await,
        Ok(FulfillmentOutcome::Shipped { .. })
    ));
    assert_eq!(
        process_order_accepted(&database.pool, &event)
            .await
            .expect("duplicate should be a safe no-op"),
        FulfillmentOutcome::Duplicate
    );
    let shipment_count: i64 = query_scalar("SELECT COUNT(*) FROM shipments WHERE order_id = $1")
        .bind(order_id)
        .fetch_one(&database.pool)
        .await
        .expect("shipment count should be queryable");
    assert_eq!(shipment_count, 1);
    assert_eq!(
        count_order_events(&database.pool, order_id, "OrderShipped").await,
        1
    );

    database.cleanup().await;
}

#[tokio::test]
async fn distinct_accepted_event_for_shipped_order_does_not_create_another_shipment() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let (order_id, event) = make_accepted_order(&database.pool, customer, helmet, 1).await;
    process_order_accepted(&database.pool, &event)
        .await
        .expect("first event should ship the order");
    let mut second_event = event.clone();
    second_event.event_id = Uuid::new_v4();

    assert_eq!(
        process_order_accepted(&database.pool, &second_event)
            .await
            .expect("already shipped order should be ignored"),
        FulfillmentOutcome::IgnoredAlreadyShipped
    );
    let shipment_count: i64 = query_scalar("SELECT COUNT(*) FROM shipments WHERE order_id = $1")
        .bind(order_id)
        .fetch_one(&database.pool)
        .await
        .expect("shipment count should be queryable");
    assert_eq!(shipment_count, 1);
    assert_eq!(
        count_order_events(&database.pool, order_id, "OrderShipped").await,
        1
    );

    database.cleanup().await;
}

#[tokio::test]
async fn fulfillment_rejects_pending_order_without_shipment() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let submitted = submit_order(
        &database.pool,
        &valid_request(customer, helmet, 1),
        Uuid::new_v4(),
    )
    .await
    .expect("order submission should succeed");
    let event = accepted_event_for_order_created(
        order_created_event(&database.pool, submitted.order_id).await,
    );

    assert!(matches!(
        process_order_accepted(&database.pool, &event).await,
        Err(FulfillmentProcessError::InvalidEvent(_))
    ));
    assert_eq!(
        count_processed_fulfillment_events(&database.pool, event.event_id).await,
        0
    );
    assert_eq!(
        count_order_events(&database.pool, submitted.order_id, "OrderShipped").await,
        0
    );
    let shipment_count: i64 = query_scalar("SELECT COUNT(*) FROM shipments WHERE order_id = $1")
        .bind(submitted.order_id)
        .fetch_one(&database.pool)
        .await
        .expect("shipment count should be queryable");
    assert_eq!(shipment_count, 0);

    database.cleanup().await;
}

#[tokio::test]
async fn fulfillment_rejects_rejected_order_without_shipment() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let submitted = submit_order(
        &database.pool,
        &valid_request(customer, helmet, 4),
        Uuid::new_v4(),
    )
    .await
    .expect("order submission should succeed");
    let created = order_created_event(&database.pool, submitted.order_id).await;
    assert!(matches!(
        process_order_created(&database.pool, &created)
            .await
            .expect("short order should be rejected"),
        InventoryOutcome::Rejected { .. }
    ));
    let event = accepted_event_for_order_created(created);

    assert!(matches!(
        process_order_accepted(&database.pool, &event).await,
        Err(FulfillmentProcessError::InvalidEvent(_))
    ));
    assert_eq!(
        count_processed_fulfillment_events(&database.pool, event.event_id).await,
        0
    );
    assert_eq!(
        count_order_events(&database.pool, submitted.order_id, "OrderShipped").await,
        0
    );
    let shipment_count: i64 = query_scalar("SELECT COUNT(*) FROM shipments WHERE order_id = $1")
        .bind(submitted.order_id)
        .fetch_one(&database.pool)
        .await
        .expect("shipment count should be queryable");
    assert_eq!(shipment_count, 0);

    database.cleanup().await;
}

#[tokio::test]
async fn mismatched_fulfillment_event_lines_roll_back_deduplication_marker() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let (order_id, mut event) = make_accepted_order(&database.pool, customer, helmet, 1).await;
    event.data.items[0].quantity = 2;

    assert!(matches!(
        process_order_accepted(&database.pool, &event).await,
        Err(FulfillmentProcessError::InvalidEvent(_))
    ));
    assert_eq!(
        count_processed_fulfillment_events(&database.pool, event.event_id).await,
        0
    );
    assert_eq!(
        count_order_events(&database.pool, order_id, "OrderShipped").await,
        0
    );
    let shipment_count: i64 = query_scalar("SELECT COUNT(*) FROM shipments WHERE order_id = $1")
        .bind(order_id)
        .fetch_one(&database.pool)
        .await
        .expect("shipment count should be queryable");
    assert_eq!(shipment_count, 0);

    database.cleanup().await;
}

#[tokio::test]
async fn mismatched_stock_reservations_roll_back_fulfillment() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let (order_id, event) = make_accepted_order(&database.pool, customer, helmet, 1).await;
    query("DELETE FROM stock_reservations WHERE order_id = $1")
        .bind(order_id)
        .execute(&database.pool)
        .await
        .expect("test reservation should be removed");

    assert!(matches!(
        process_order_accepted(&database.pool, &event).await,
        Err(FulfillmentProcessError::InvalidEvent(_))
    ));
    assert_eq!(
        count_processed_fulfillment_events(&database.pool, event.event_id).await,
        0
    );
    assert_eq!(
        count_order_events(&database.pool, order_id, "OrderShipped").await,
        0
    );
    let shipment_count: i64 = query_scalar("SELECT COUNT(*) FROM shipments WHERE order_id = $1")
        .bind(order_id)
        .fetch_one(&database.pool)
        .await
        .expect("shipment count should be queryable");
    assert_eq!(shipment_count, 0);
    let status: String = query_scalar("SELECT status FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(&database.pool)
        .await
        .expect("order status should be queryable");
    assert_eq!(status, "accepted");

    database.cleanup().await;
}

#[tokio::test]
async fn concurrent_duplicate_fulfillment_events_create_one_shipment_and_event() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let (order_id, event) = make_accepted_order(&database.pool, customer, helmet, 1).await;

    let (first, second) = tokio::join!(
        process_order_accepted(&database.pool, &event),
        process_order_accepted(&database.pool, &event),
    );
    let outcomes = [
        first.expect("first concurrent delivery should succeed"),
        second.expect("second concurrent delivery should succeed"),
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, FulfillmentOutcome::Shipped { .. }))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, FulfillmentOutcome::Duplicate))
            .count(),
        1
    );
    let shipment_count: i64 = query_scalar("SELECT COUNT(*) FROM shipments WHERE order_id = $1")
        .bind(order_id)
        .fetch_one(&database.pool)
        .await
        .expect("shipment count should be queryable");
    assert_eq!(shipment_count, 1);
    assert_eq!(
        count_order_events(&database.pool, order_id, "OrderShipped").await,
        1
    );

    database.cleanup().await;
}

#[tokio::test]
async fn accepted_event_creates_customer_notification() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let (order_id, _) = make_accepted_order(&database.pool, customer, helmet, 1).await;
    let event = order_lifecycle_event(&database.pool, order_id, "OrderAccepted").await;

    assert_eq!(
        process_order_lifecycle_event(&database.pool, &event)
            .await
            .expect("accepted event should create a notification"),
        NotificationOutcome::Created {
            kind: "accepted".to_owned()
        }
    );
    let (kind, message): (String, String) =
        query_as("SELECT kind, message FROM notifications WHERE source_event_id = $1")
            .bind(event.event_id)
            .fetch_one(&database.pool)
            .await
            .expect("accepted notification should be queryable");
    assert_eq!(kind, "accepted");
    assert!(message.contains("was accepted"));
    assert_eq!(
        count_processed_notification_events(&database.pool, event.event_id).await,
        1
    );

    database.cleanup().await;
}

#[tokio::test]
async fn rejected_event_creates_notification_with_rejection_reason() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let submitted = submit_order(
        &database.pool,
        &valid_request(customer, helmet, 4),
        Uuid::new_v4(),
    )
    .await
    .expect("order submission should succeed");
    let created = order_created_event(&database.pool, submitted.order_id).await;
    assert!(matches!(
        process_order_created(&database.pool, &created)
            .await
            .expect("insufficient stock should reject the order"),
        InventoryOutcome::Rejected { .. }
    ));
    let event = order_lifecycle_event(&database.pool, submitted.order_id, "OrderRejected").await;

    assert_eq!(
        process_order_lifecycle_event(&database.pool, &event)
            .await
            .expect("rejected event should create a notification"),
        NotificationOutcome::Created {
            kind: "rejected".to_owned()
        }
    );
    let message: String =
        query_scalar("SELECT message FROM notifications WHERE source_event_id = $1")
            .bind(event.event_id)
            .fetch_one(&database.pool)
            .await
            .expect("rejection notification should be queryable");
    assert!(message.contains("was rejected"));
    assert!(message.contains("Insufficient stock"));

    database.cleanup().await;
}

#[tokio::test]
async fn shipped_event_creates_notification_with_simulated_tracking_and_eur_total() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let (order_id, accepted) = make_accepted_order(&database.pool, customer, helmet, 1).await;
    process_order_accepted(&database.pool, &accepted)
        .await
        .expect("accepted order should ship");
    let event = order_lifecycle_event(&database.pool, order_id, "OrderShipped").await;

    assert_eq!(
        process_order_lifecycle_event(&database.pool, &event)
            .await
            .expect("shipped event should create a notification"),
        NotificationOutcome::Created {
            kind: "shipped".to_owned()
        }
    );
    let message: String =
        query_scalar("SELECT message FROM notifications WHERE source_event_id = $1")
            .bind(event.event_id)
            .fetch_one(&database.pool)
            .await
            .expect("shipping notification should be queryable");
    assert!(message.contains("Simulated tracking reference: SIM-"));
    assert!(message.contains("Order value: EUR 149.00"));

    database.cleanup().await;
}

#[tokio::test]
async fn duplicate_notification_event_creates_only_one_row() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let (order_id, _) = make_accepted_order(&database.pool, customer, helmet, 1).await;
    let event = order_lifecycle_event(&database.pool, order_id, "OrderAccepted").await;

    process_order_lifecycle_event(&database.pool, &event)
        .await
        .expect("first delivery should create a notification");
    assert_eq!(
        process_order_lifecycle_event(&database.pool, &event)
            .await
            .expect("duplicate should be a safe no-op"),
        NotificationOutcome::Duplicate
    );
    assert_eq!(
        count_notifications_for_event(&database.pool, event.event_id).await,
        1
    );
    assert_eq!(
        count_processed_notification_events(&database.pool, event.event_id).await,
        1
    );

    database.cleanup().await;
}

#[tokio::test]
async fn mismatched_notification_customer_rolls_back_processed_marker() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let other_customer: Uuid =
        query_scalar("SELECT id FROM customers WHERE email = 'bob@example.test'")
            .fetch_one(&database.pool)
            .await
            .expect("second seeded customer should exist");
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let (order_id, _) = make_accepted_order(&database.pool, customer, helmet, 1).await;
    let mut event = order_lifecycle_event(&database.pool, order_id, "OrderAccepted").await;
    event.data["customer_id"] = serde_json::json!(other_customer);

    assert!(matches!(
        process_order_lifecycle_event(&database.pool, &event).await,
        Err(NotificationProcessError::InvalidEvent(_))
    ));
    assert_eq!(
        count_notifications_for_event(&database.pool, event.event_id).await,
        0
    );
    assert_eq!(
        count_processed_notification_events(&database.pool, event.event_id).await,
        0
    );

    database.cleanup().await;
}

#[tokio::test]
async fn invalid_notification_payload_rolls_back_without_notification() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let (order_id, _) = make_accepted_order(&database.pool, customer, helmet, 1).await;
    let mut event = order_lifecycle_event(&database.pool, order_id, "OrderAccepted").await;
    event.data["items"][0]["quantity"] = serde_json::json!(0);

    assert!(matches!(
        process_order_lifecycle_event(&database.pool, &event).await,
        Err(NotificationProcessError::InvalidEvent(_))
    ));
    assert_eq!(
        count_notifications_for_event(&database.pool, event.event_id).await,
        0
    );
    assert_eq!(
        count_processed_notification_events(&database.pool, event.event_id).await,
        0
    );

    database.cleanup().await;
}

#[tokio::test]
async fn concurrent_duplicate_notification_events_create_one_row() {
    let database = TestDatabase::new().await;
    let customer = customer_id(&database.pool).await;
    let helmet = product_id(&database.pool, "HELMET-TOUR-01").await;
    let (order_id, _) = make_accepted_order(&database.pool, customer, helmet, 1).await;
    let event = order_lifecycle_event(&database.pool, order_id, "OrderAccepted").await;

    let (first, second) = tokio::join!(
        process_order_lifecycle_event(&database.pool, &event),
        process_order_lifecycle_event(&database.pool, &event),
    );
    let outcomes = [
        first.expect("first concurrent delivery should succeed"),
        second.expect("second concurrent delivery should succeed"),
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, NotificationOutcome::Created { .. }))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, NotificationOutcome::Duplicate))
            .count(),
        1
    );
    assert_eq!(
        count_notifications_for_event(&database.pool, event.event_id).await,
        1
    );

    database.cleanup().await;
}
