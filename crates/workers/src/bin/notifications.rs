use std::{
    env,
    error::Error,
    io,
    time::{Duration, Instant},
};

use motogear_persistence::{
    connect_with,
    notifications::{
        NOTIFICATIONS_CONSUMER_NAME, NotificationOutcome, NotificationProcessError,
        OrderLifecycleEvent, process_order_lifecycle_event,
    },
    postgres_options_from_env,
};
use rdkafka::{
    ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::{BorrowedMessage, Message},
};
use serde::Deserialize;
use tokio::time::sleep;
use uuid::Uuid;

const RETRY_START: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);
const MAX_RETRY_DURATION: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Deserialize)]
struct EventHeader {
    event_id: Uuid,
    event_type: String,
    schema_version: i32,
}

enum ParsedRecord {
    LifecycleEvent(OrderLifecycleEvent),
    Skip {
        event_id: Option<Uuid>,
        event_type: Option<String>,
        reason: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Loading .env is optional because Compose can provide environment variables.
    let _ = dotenvy::dotenv();

    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let topic = env::var("KAFKA_ORDERS_TOPIC").unwrap_or_else(|_| "shop.orders.v1".to_owned());
    let pool = connect_with(postgres_options_from_env()?).await?;
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", NOTIFICATIONS_CONSUMER_NAME)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;
    consumer.subscribe(&[&topic])?;

    println!("Notification consumer subscribed to {topic} at {brokers}");
    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);

    'message_loop: loop {
        let message = tokio::select! {
            biased;
            signal_result = &mut shutdown_signal => {
                signal_result?;
                println!("Shutdown requested; notification consumer stopped");
                break;
            },
            message = consumer.recv() => message?,
        };

        let parsed_record = match message.payload() {
            Some(payload) => parse_record(payload),
            None => ParsedRecord::Skip {
                event_id: None,
                event_type: None,
                reason: "Kafka record has no payload".to_owned(),
            },
        };
        let event = match parsed_record {
            ParsedRecord::LifecycleEvent(event) => event,
            ParsedRecord::Skip {
                event_id,
                event_type,
                reason,
            } => {
                commit_skipped_record(
                    &consumer,
                    &message,
                    event_id,
                    event_type.as_deref(),
                    &reason,
                )?;
                continue;
            }
        };

        let retry_started = Instant::now();
        let mut retry_delay = RETRY_START;
        let outcome = 'process_event: loop {
            let remaining = MAX_RETRY_DURATION.saturating_sub(retry_started.elapsed());
            if remaining.is_zero() {
                eprintln!(
                    "topic={} partition={} offset={} event_id={} order_id={} event_type={} result=retry_deadline_exceeded",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    event.event_id,
                    event.order_id,
                    event.event_type
                );
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "database processing exceeded five minutes for event {}",
                        event.event_id
                    ),
                )
                .into());
            }

            let processing =
                tokio::time::timeout(remaining, process_order_lifecycle_event(&pool, &event)).await;
            match processing {
                Ok(Ok(outcome)) => break 'process_event outcome,
                Ok(Err(error @ NotificationProcessError::InvalidEvent(_))) => {
                    commit_skipped_record(
                        &consumer,
                        &message,
                        Some(event.event_id),
                        Some(&event.event_type),
                        &error.to_string(),
                    )?;
                    continue 'message_loop;
                }
                Ok(Err(error @ NotificationProcessError::Database(_))) => {
                    if retry_started.elapsed() >= MAX_RETRY_DURATION {
                        return Err(Box::new(error) as Box<dyn Error>);
                    }
                    let remaining = MAX_RETRY_DURATION.saturating_sub(retry_started.elapsed());
                    let wait = retry_delay.min(remaining);
                    eprintln!(
                        "topic={} partition={} offset={} event_id={} order_id={} event_type={} result=retry_database_error error={error:?} retry_in_ms={}",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        event.event_id,
                        event.order_id,
                        event.event_type,
                        wait.as_millis()
                    );
                    tokio::select! {
                        biased;
                        signal_result = &mut shutdown_signal => {
                            signal_result?;
                            println!("Shutdown requested; current event remains uncommitted");
                            return Ok(());
                        },
                        _ = sleep(wait) => {}
                    }
                    retry_delay = retry_delay.saturating_mul(2).min(RETRY_MAX);
                }
                Err(_) => {
                    eprintln!(
                        "topic={} partition={} offset={} event_id={} order_id={} event_type={} result=retry_deadline_exceeded",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        event.event_id,
                        event.order_id,
                        event.event_type
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "database processing exceeded five minutes for event {}",
                            event.event_id
                        ),
                    )
                    .into());
                }
            }
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

fn parse_record(payload: &[u8]) -> ParsedRecord {
    let header: EventHeader = match serde_json::from_slice(payload) {
        Ok(header) => header,
        Err(error) => {
            return ParsedRecord::Skip {
                event_id: None,
                event_type: None,
                reason: format!("malformed event envelope: {error}"),
            };
        }
    };

    if header.schema_version != 1 {
        return ParsedRecord::Skip {
            event_id: Some(header.event_id),
            event_type: Some(header.event_type),
            reason: format!("unsupported schema version {}", header.schema_version),
        };
    }

    if !matches!(
        header.event_type.as_str(),
        "OrderAccepted" | "OrderRejected" | "OrderShipped"
    ) {
        let reason = if header.event_type == "OrderCreated" {
            "known event type is not handled by the notifications consumer".to_owned()
        } else {
            "unknown event type".to_owned()
        };
        return ParsedRecord::Skip {
            event_id: Some(header.event_id),
            event_type: Some(header.event_type),
            reason,
        };
    }

    match serde_json::from_slice(payload) {
        Ok(event) => ParsedRecord::LifecycleEvent(event),
        Err(error) => ParsedRecord::Skip {
            event_id: Some(header.event_id),
            event_type: Some(header.event_type),
            reason: format!("malformed order lifecycle payload: {error}"),
        },
    }
}

fn commit_skipped_record(
    consumer: &StreamConsumer,
    message: &BorrowedMessage<'_>,
    event_id: Option<Uuid>,
    event_type: Option<&str>,
    reason: &str,
) -> Result<(), rdkafka::error::KafkaError> {
    eprintln!(
        "topic={} partition={} offset={} event_id={} event_type={} result=skipped reason={reason:?}",
        message.topic(),
        message.partition(),
        message.offset(),
        event_id.map_or_else(|| "unknown".to_owned(), |id| id.to_string()),
        event_type.unwrap_or("unknown")
    );
    consumer.commit_message(message, CommitMode::Sync)
}
