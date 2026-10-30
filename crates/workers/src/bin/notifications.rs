use std::{env, error::Error};

use motogear_persistence::{
    connect_with,
    notifications::{
        NOTIFICATIONS_CONSUMER_NAME, NotificationOutcome, NotificationProcessError,
        OrderLifecycleEvent, process_order_lifecycle_event,
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
        group_id: NOTIFICATIONS_CONSUMER_NAME,
    })?;
    consumer.subscribe(&[&topic])?;

    println!("Notification consumer subscribed to {topic} at {brokers}");
    let mut shutdown_signal = ShutdownSignal::new();

    'message_loop: loop {
        let message = match recv_or_shutdown(&consumer, &mut shutdown_signal).await? {
            ReceiveOutcome::Message(message) => message,
            ReceiveOutcome::Shutdown => {
                println!("Shutdown requested; notification consumer stopped");
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
            || process_order_lifecycle_event(&pool, &event),
            |error| matches!(error, NotificationProcessError::InvalidEvent(_)),
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
        match &outcome {
            NotificationOutcome::Created { kind } => println!(
                "topic={} partition={} offset={} event_id={} order_id={} event_type={} result=notification_created kind={}",
                message.topic(),
                message.partition(),
                message.offset(),
                event.event_id,
                event.order_id,
                event.event_type,
                kind
            ),
            NotificationOutcome::Duplicate => println!(
                "topic={} partition={} offset={} event_id={} order_id={} event_type={} result=duplicate",
                message.topic(),
                message.partition(),
                message.offset(),
                event.event_id,
                event.order_id,
                event.event_type
            ),
        }
    }

    Ok(())
}

fn parse_record(payload: &[u8]) -> ParsedRecord<OrderLifecycleEvent> {
    parse_event_record(
        payload,
        EventSpec {
            handled_types: &["OrderAccepted", "OrderRejected", "OrderShipped"],
            all_known_types: &[
                "OrderCreated",
                "OrderAccepted",
                "OrderRejected",
                "OrderShipped",
            ],
            consumer_name: "notifications",
            payload_name: "order lifecycle",
        },
    )
}
