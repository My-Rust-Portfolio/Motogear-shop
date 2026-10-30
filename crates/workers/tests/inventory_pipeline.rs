use motogear_workers::runtime::{EventHeader, EventSpec, ParsedRecord, parse_record};
use std::{str::FromStr, time::Duration};

use motogear_domain::orders::{OrderRequest, OrderRequestItem};
use motogear_persistence::{
    events::outbox::{mark_outbox_event_published, next_unpublished_event},
    fulfillment::{FulfillmentOutcome, OrderAcceptedEvent, process_order_accepted},
    inventory::{InventoryOutcome, OrderCreatedEvent, process_order_created},
    migrations::run_migrations,
    orders::submit_order,
    seed::seed_demo_data,
};
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Message,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use sqlx::{
    AssertSqlSafe,
    postgres::{PgConnectOptions, PgPoolOptions},
    query, query_as, query_scalar,
};
use tokio::time::timeout;
use uuid::Uuid;

fn inventory_event_spec() -> EventSpec<'static> {
    EventSpec {
        handled_types: &["OrderCreated"],
        all_known_types: &[
            "OrderCreated",
            "OrderAccepted",
            "OrderRejected",
            "OrderShipped",
        ],
        consumer_name: "inventory",
        payload_name: "OrderCreated",
    }
}

struct TestDatabase {
    admin_pool: sqlx::PgPool,
    pool: sqlx::PgPool,
    schema: String,
}

impl TestDatabase {
    async fn new() -> Self {
        let database_url = std::env::var("TEST_DATABASE_URL")
            .expect("set TEST_DATABASE_URL to an isolated test database");
        let options = PgConnectOptions::from_str(&database_url)
            .expect("TEST_DATABASE_URL must be a PostgreSQL URL");
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options.clone())
            .await
            .expect("TEST_DATABASE_URL must be reachable");

        let schema = format!("inventory_kafka_test_{}", Uuid::new_v4().simple());
        query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&admin_pool)
            .await
            .expect("test role must be allowed to create schemas");
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(options.options([("search_path", schema.as_str())]))
            .await
            .expect("isolated test schema should be reachable");
        run_migrations(&pool)
            .await
            .expect("migrations should apply to the test schema");
        seed_demo_data(&pool)
            .await
            .expect("demo data should seed the test schema");

        Self {
            admin_pool,
            pool,
            schema,
        }
    }

    async fn cleanup(self) {
        self.pool.close().await;
        query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.schema
        )))
        .execute(&self.admin_pool)
        .await
        .expect("test schema cleanup should succeed");
        self.admin_pool.close().await;
    }
}

fn test_consumer(
    brokers: &str,
    topic: &str,
    group: &str,
) -> Result<StreamConsumer, Box<dyn std::error::Error>> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;
    consumer.subscribe(&[topic])?;
    Ok(consumer)
}

async fn wait_for_assignment(consumer: &StreamConsumer) -> Result<(), Box<dyn std::error::Error>> {
    timeout(Duration::from_secs(15), async {
        loop {
            if consumer.assignment()?.count() > 0 {
                return Ok(());
            }
            match timeout(Duration::from_millis(100), consumer.recv()).await {
                Ok(Ok(message)) => {
                    return Err(std::io::Error::other(format!(
                        "consumer received unexpected record at offset {} before test publish",
                        message.offset()
                    ))
                    .into());
                }
                Ok(Err(error)) => return Err(Box::new(error) as Box<dyn std::error::Error>),
                Err(_) => {}
            }
        }
    })
    .await??;
    Ok(())
}

async fn publish_test_record(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    payload: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    producer
        .send(
            FutureRecord::to(topic)
                .partition(0)
                .key(key)
                .payload(payload),
            Timeout::After(Duration::from_secs(15)),
        )
        .await
        .map_err(|(error, _)| error)?;
    Ok(())
}

#[test]
fn malformed_record_is_classified_for_skip() {
    let record = parse_record::<OrderCreatedEvent>(b"{not-json", inventory_event_spec());

    let ParsedRecord::Skip(skipped) = record else {
        panic!("malformed record should be skipped");
    };
    assert_eq!(skipped.event_id, None);
    assert_eq!(skipped.event_type, None);
}

#[test]
fn unknown_event_type_is_classified_for_skip() {
    let event_id = Uuid::new_v4();
    let payload = serde_json::json!({
        "event_id": event_id,
        "event_type": "OrderCancelled",
        "schema_version": 1,
    })
    .to_string();

    let ParsedRecord::Skip(skipped) =
        parse_record::<OrderCreatedEvent>(payload.as_bytes(), inventory_event_spec())
    else {
        panic!("unknown event type should be skipped");
    };
    assert_eq!(skipped.event_id, Some(event_id));
    assert_eq!(skipped.event_type.as_deref(), Some("OrderCancelled"));
}

#[test]
fn known_event_for_another_worker_is_classified_for_skip() {
    let event_id = Uuid::new_v4();
    let payload = serde_json::json!({
        "event_id": event_id,
        "event_type": "OrderAccepted",
        "schema_version": 1,
    })
    .to_string();

    let ParsedRecord::Skip(skipped) =
        parse_record::<OrderCreatedEvent>(payload.as_bytes(), inventory_event_spec())
    else {
        panic!("known event for another worker should be skipped");
    };
    assert_eq!(skipped.event_id, Some(event_id));
    assert_eq!(skipped.event_type.as_deref(), Some("OrderAccepted"));
}

#[test]
fn valid_order_created_event_is_deserialized() {
    let event_id = Uuid::new_v4();
    let order_id = Uuid::new_v4();
    let customer_id = Uuid::new_v4();
    let product_id = Uuid::new_v4();
    let payload = serde_json::json!({
        "event_id": event_id,
        "event_type": "OrderCreated",
        "schema_version": 1,
        "occurred_at": "2026-10-30T00:00:00Z",
        "order_id": order_id,
        "correlation_id": order_id,
        "causation_id": null,
        "data": {
            "customer_id": customer_id,
            "items": [{ "product_id": product_id, "quantity": 2 }],
        },
    })
    .to_string();

    match parse_record::<OrderCreatedEvent>(payload.as_bytes(), inventory_event_spec()) {
        ParsedRecord::Event(event) => {
            assert_eq!(event.event_id, event_id);
            assert_eq!(event.order_id, order_id);
            assert_eq!(event.data.customer_id, customer_id);
            assert_eq!(event.data.items[0].product_id, product_id);
            assert_eq!(event.data.items[0].quantity, 2);
        }
        ParsedRecord::Skip(skipped) => {
            panic!("valid event was skipped: {}", skipped.reason)
        }
    }
}

#[tokio::test]
#[ignore = "requires a real Kafka broker and TEST_DATABASE_URL"]
async fn kafka_order_lifecycle_pipeline_and_consumer_restart_are_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenvy::dotenv();
    let database = TestDatabase::new().await;
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let topic = format!("inventory-pipeline-{}", Uuid::new_v4().simple());
    let group = format!("inventory-pipeline-test-{}", Uuid::new_v4().simple());
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()?;
    let new_topic = NewTopic::new(&topic, 1, TopicReplication::Fixed(1));
    for result in admin
        .create_topics([&new_topic], &AdminOptions::new())
        .await?
    {
        result.map_err(|(name, error)| {
            std::io::Error::other(format!("could not create test topic {name}: {error}"))
        })?;
    }

    let customer_id: Uuid =
        query_scalar("SELECT id FROM customers WHERE email = 'alice@example.test'")
            .fetch_one(&database.pool)
            .await?;
    let product_id: Uuid = query_scalar("SELECT id FROM products WHERE sku = 'HELMET-TOUR-01'")
        .fetch_one(&database.pool)
        .await?;
    let request = OrderRequest {
        customer_id,
        items: vec![OrderRequestItem {
            product_id,
            quantity: 1,
        }],
    }
    .validate()?;
    let order = submit_order(&database.pool, &request, Uuid::new_v4()).await?;
    query(
        "UPDATE outbox_events SET target_topic = $2 \
             WHERE aggregate_id = $1 AND event_type = 'OrderCreated'",
    )
    .bind(order.order_id)
    .bind(&topic)
    .execute(&database.pool)
    .await?;

    let created = next_unpublished_event(&database.pool)
        .await?
        .expect("order submission should create an outbox event");
    assert_eq!(created.target_topic, topic);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("acks", "all")
        .create()?;
    let consumer = test_consumer(&brokers, &topic, &group)?;
    wait_for_assignment(&consumer).await?;
    publish_test_record(
        &producer,
        &topic,
        &created.message_key,
        &created.payload.0.to_string(),
    )
    .await?;
    assert!(mark_outbox_event_published(&database.pool, created.event_id).await?);

    let message = timeout(Duration::from_secs(15), consumer.recv()).await??;
    let event = match parse_record::<OrderCreatedEvent>(
        message.payload().expect("record should have payload"),
        inventory_event_spec(),
    ) {
        ParsedRecord::Event(event) => event,
        ParsedRecord::Skip(skipped) => {
            return Err(std::io::Error::other(skipped.reason).into());
        }
    };
    assert_eq!(
        process_order_created(&database.pool, &event).await?,
        InventoryOutcome::Accepted
    );
    consumer.commit_message(&message, CommitMode::Sync)?;
    drop(message);
    drop(consumer);

    let accepted_id: Uuid = query_scalar(
        "SELECT event_id FROM outbox_events \
             WHERE aggregate_id = $1 AND event_type = 'OrderAccepted'",
    )
    .bind(order.order_id)
    .fetch_one(&database.pool)
    .await?;
    query("UPDATE outbox_events SET target_topic = $2 WHERE event_id = $1")
        .bind(accepted_id)
        .bind(&topic)
        .execute(&database.pool)
        .await?;
    let accepted = next_unpublished_event(&database.pool)
        .await?
        .expect("inventory transaction should create OrderAccepted");
    let accepted_payload = accepted.payload.0.to_string();

    // Recreate the same consumer group at its committed offset before
    // publishing the follow-up event. A replayed OrderCreated would appear
    // here instead of OrderAccepted and fail the assertion below.
    let restarted_consumer = test_consumer(&brokers, &topic, &group)?;
    wait_for_assignment(&restarted_consumer).await?;
    publish_test_record(&producer, &topic, &accepted.message_key, &accepted_payload).await?;
    assert!(mark_outbox_event_published(&database.pool, accepted.event_id).await?);

    let accepted_message = timeout(Duration::from_secs(15), restarted_consumer.recv()).await??;
    let accepted_payload = accepted_message
        .payload()
        .expect("record should have payload");
    let accepted_header: EventHeader = serde_json::from_slice(accepted_payload)?;
    assert_eq!(accepted_header.event_type, "OrderAccepted");
    assert_eq!(accepted_header.schema_version, 1);
    let accepted_event: OrderAcceptedEvent = serde_json::from_slice(accepted_payload)?;
    let fulfillment = process_order_accepted(&database.pool, &accepted_event).await?;
    let tracking_reference = match fulfillment {
        FulfillmentOutcome::Shipped { tracking_reference } => tracking_reference,
        outcome => {
            return Err(std::io::Error::other(format!(
                "expected shipment to be created, got {outcome:?}"
            ))
            .into());
        }
    };
    restarted_consumer.commit_message(&accepted_message, CommitMode::Sync)?;

    assert_eq!(stock_available(&database.pool, product_id).await, 2);
    assert_eq!(count_reservations(&database.pool, order.order_id).await, 1);
    let accepted_count: i64 = query_scalar(
        "SELECT COUNT(*) FROM outbox_events \
             WHERE aggregate_id = $1 AND event_type = 'OrderAccepted' AND published_at IS NOT NULL",
    )
    .bind(order.order_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(accepted_count, 1);

    let shipped_id: Uuid = query_scalar(
        "SELECT event_id FROM outbox_events \
             WHERE aggregate_id = $1 AND event_type = 'OrderShipped'",
    )
    .bind(order.order_id)
    .fetch_one(&database.pool)
    .await?;
    query("UPDATE outbox_events SET target_topic = $2 WHERE event_id = $1")
        .bind(shipped_id)
        .bind(&topic)
        .execute(&database.pool)
        .await?;
    let shipped = next_unpublished_event(&database.pool)
        .await?
        .expect("fulfillment transaction should create OrderShipped");
    publish_test_record(
        &producer,
        &topic,
        &shipped.message_key,
        &shipped.payload.0.to_string(),
    )
    .await?;
    assert!(mark_outbox_event_published(&database.pool, shipped.event_id).await?);

    let shipped_message = timeout(Duration::from_secs(15), restarted_consumer.recv()).await??;
    let shipped_payload = shipped_message
        .payload()
        .expect("OrderShipped Kafka record should have a payload");
    let shipped_header: EventHeader = serde_json::from_slice(shipped_payload)?;
    assert_eq!(shipped_header.event_type, "OrderShipped");
    assert_eq!(shipped_header.schema_version, 1);
    let shipped_body: serde_json::Value = serde_json::from_slice(shipped_payload)?;
    assert_eq!(
        shipped_body["data"]["tracking_reference"],
        tracking_reference
    );
    assert_eq!(shipped_body["data"]["total_cents"], 14_900);
    restarted_consumer.commit_message(&shipped_message, CommitMode::Sync)?;

    let (status, shipment_count): (String, i64) = query_as(
        "SELECT o.status, COUNT(s.id) FROM orders o \
             LEFT JOIN shipments s ON s.order_id = o.id \
             WHERE o.id = $1 GROUP BY o.status",
    )
    .bind(order.order_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(status, "shipped");
    assert_eq!(shipment_count, 1);

    drop(accepted_message);
    drop(shipped_message);
    drop(restarted_consumer);
    drop(producer);
    let _ = admin
        .delete_topics(&[topic.as_str()], &AdminOptions::new())
        .await;
    database.cleanup().await;
    Ok(())
}

async fn stock_available(pool: &sqlx::PgPool, product_id: Uuid) -> i32 {
    query_scalar("SELECT stock_available FROM products WHERE id = $1")
        .bind(product_id)
        .fetch_one(pool)
        .await
        .expect("product stock should be queryable")
}

async fn count_reservations(pool: &sqlx::PgPool, order_id: Uuid) -> i64 {
    query_scalar("SELECT COUNT(*) FROM stock_reservations WHERE order_id = $1")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("reservation count should be queryable")
}
