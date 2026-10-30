use std::{env, error::Error};

use motogear_persistence::{
    connect_with,
    inventory::{
        INVENTORY_CONSUMER_NAME, InventoryOutcome, InventoryProcessError, OrderCreatedEvent,
        process_order_created,
    },
    postgres_options_from_env,
};
use motogear_workers::runtime::{
    ConsumerSettings, EventLogContext, EventSpec, ParsedRecord, ReceiveOutcome, RetryOutcome,
    ShutdownSignal, build_consumer, commit_skipped_record, parse_record as parse_event_record,
    payload_or_skip, recv_or_shutdown, retry_database_operation,
};
use rdkafka::{
    consumer::{CommitMode, Consumer},
    message::Message,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Loading .env is optional because Compose can provide environment variables.
    let _ = dotenvy::dotenv();

    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let topic = env::var("KAFKA_ORDERS_TOPIC").unwrap_or_else(|_| "shop.orders.v1".to_owned());
    let pool = connect_with(postgres_options_from_env()?).await?;
    let consumer = build_consumer(ConsumerSettings {
        brokers: &brokers,
        group_id: INVENTORY_CONSUMER_NAME,
    })?;
    consumer.subscribe(&[&topic])?;

    println!("Inventory consumer subscribed to {topic} at {brokers}");
    let mut shutdown_signal = ShutdownSignal::new();

    'message_loop: loop {
        let message = match recv_or_shutdown(&consumer, &mut shutdown_signal).await? {
            ReceiveOutcome::Message(message) => message,
            ReceiveOutcome::Shutdown => {
                println!("Shutdown requested; inventory consumer stopped");
                break;
            }
        };

        let parsed_record = match payload_or_skip(message.payload()) {
            Ok(payload) => parse_record(payload),
            Err(skipped) => ParsedRecord::Skip(skipped),
        };
        let event = match parsed_record {
            ParsedRecord::Event(event) => event,
            ParsedRecord::Skip(skipped) => {
                commit_skipped_record(&consumer, &message, &skipped)?;
                continue;
            }
        };

        let context = EventLogContext {
            topic: message.topic().to_owned(),
            partition: message.partition(),
            offset: message.offset(),
            event_id: event.event_id,
            order_id: event.order_id,
            event_type: Some(event.event_type.clone()),
        };
        let outcome = match retry_database_operation(
            &context,
            &mut shutdown_signal,
            || process_order_created(&pool, &event),
            |error| matches!(error, InventoryProcessError::InvalidEvent(_)),
        )
        .await?
        {
            RetryOutcome::Success(outcome) => outcome,
            RetryOutcome::Invalid(skipped) => {
                commit_skipped_record(&consumer, &message, &skipped)?;
                continue 'message_loop;
            }
            RetryOutcome::Shutdown => return Ok(()),
        };

        consumer.commit_message(&message, CommitMode::Sync)?;
        println!(
            "topic={} partition={} offset={} event_id={} order_id={} result={}",
            message.topic(),
            message.partition(),
            message.offset(),
            event.event_id,
            event.order_id,
            outcome_name(&outcome)
        );
    }

    Ok(())
}

fn parse_record(payload: &[u8]) -> ParsedRecord<OrderCreatedEvent> {
    parse_event_record(
        payload,
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
        },
    )
}

fn outcome_name(outcome: &InventoryOutcome) -> &'static str {
    match outcome {
        InventoryOutcome::Accepted => "accepted",
        InventoryOutcome::Rejected { .. } => "rejected",
        InventoryOutcome::Duplicate => "duplicate",
        InventoryOutcome::IgnoredNoLongerPending => "ignored_not_pending",
    }
}
