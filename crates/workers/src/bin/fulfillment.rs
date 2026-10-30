use std::{
    env,
    error::Error,
    io,
    time::{Duration, Instant},
};

use motogear_persistence::{
    connect_with,
    fulfillment::{
        FULFILLMENT_CONSUMER_NAME, FulfillmentOutcome, FulfillmentProcessError, OrderAcceptedEvent,
        fulfillment_event_is_processed, process_order_accepted,
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
const DEFAULT_SIMULATION_DELAY: Duration = Duration::from_secs(2);
const MAX_SIMULATION_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize)]
struct EventHeader {
    event_id: Uuid,
    event_type: String,
    schema_version: i32,
}

enum ParsedRecord {
    OrderAccepted(OrderAcceptedEvent),
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
    let simulation_delay = simulation_delay_from_env()?;
    let pool = connect_with(postgres_options_from_env()?).await?;
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", FULFILLMENT_CONSUMER_NAME)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;
    consumer.subscribe(&[&topic])?;

    println!(
        "Fulfillment consumer subscribed to {topic} at {brokers}; simulation_delay_ms={}",
        simulation_delay.as_millis()
    );
    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);

    'message_loop: loop {
        let message = tokio::select! {
            biased;
            signal_result = &mut shutdown_signal => {
                signal_result?;
                println!("Shutdown requested; fulfillment consumer stopped");
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
            ParsedRecord::OrderAccepted(event) => event,
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

        // This lookup only avoids wasting the simulated delay on ordinary
        // replays. The transaction below remains authoritative under races.
        let should_delay = if simulation_delay.is_zero() {
            false
        } else {
            match fulfillment_event_is_processed(&pool, event.event_id).await {
                Ok(is_processed) => !is_processed,
                Err(error) => {
                    eprintln!(
                        "topic={} partition={} offset={} event_id={} order_id={} operation=dedupe_precheck result=failed error={error:?} delay_skipped=true",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        event.event_id,
                        event.order_id
                    );
                    false
                }
            }
        };
        if should_delay {
            tokio::select! {
                biased;
                signal_result = &mut shutdown_signal => {
                    signal_result?;
                    println!("Shutdown requested during simulated fulfillment; current event remains uncommitted");
                    break 'message_loop;
                },
                _ = sleep(simulation_delay) => {}
            }
        }

        let retry_started = Instant::now();
        let mut retry_delay = RETRY_START;
        let outcome = 'process_event: loop {
            let remaining = MAX_RETRY_DURATION.saturating_sub(retry_started.elapsed());
            if remaining.is_zero() {
                eprintln!(
                    "topic={} partition={} offset={} event_id={} order_id={} result=retry_deadline_exceeded",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    event.event_id,
                    event.order_id
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
                tokio::time::timeout(remaining, process_order_accepted(&pool, &event)).await;
            match processing {
                Ok(Ok(outcome)) => break 'process_event outcome,
                Ok(Err(error @ FulfillmentProcessError::InvalidEvent(_))) => {
                    commit_skipped_record(
                        &consumer,
                        &message,
                        Some(event.event_id),
                        Some(&event.event_type),
                        &error.to_string(),
                    )?;
                    continue 'message_loop;
                }
                Ok(Err(error @ FulfillmentProcessError::Database(_))) => {
                    if retry_started.elapsed() >= MAX_RETRY_DURATION {
                        return Err(Box::new(error) as Box<dyn Error>);
                    }
                    let remaining = MAX_RETRY_DURATION.saturating_sub(retry_started.elapsed());
                    let wait = retry_delay.min(remaining);
                    eprintln!(
                        "topic={} partition={} offset={} event_id={} order_id={} result=retry_database_error error={error:?} retry_in_ms={}",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        event.event_id,
                        event.order_id,
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
                        "topic={} partition={} offset={} event_id={} order_id={} result=retry_deadline_exceeded",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        event.event_id,
                        event.order_id
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
            FulfillmentOutcome::Shipped { tracking_reference } => println!(
                "topic={} partition={} offset={} event_id={} order_id={} result=shipped tracking_reference={}",
                message.topic(),
                message.partition(),
                message.offset(),
                event.event_id,
                event.order_id,
                tracking_reference
            ),
            FulfillmentOutcome::Duplicate => println!(
                "topic={} partition={} offset={} event_id={} order_id={} result=duplicate",
                message.topic(),
                message.partition(),
                message.offset(),
                event.event_id,
                event.order_id
            ),
            FulfillmentOutcome::IgnoredAlreadyShipped => println!(
                "topic={} partition={} offset={} event_id={} order_id={} result=already_shipped",
                message.topic(),
                message.partition(),
                message.offset(),
                event.event_id,
                event.order_id
            ),
        }
    }

    Ok(())
}

fn simulation_delay_from_env() -> Result<Duration, Box<dyn Error>> {
    let delay = match env::var("FULFILLMENT_DELAY_MS") {
        Ok(value) => Duration::from_millis(value.parse::<u64>()?),
        Err(env::VarError::NotPresent) => DEFAULT_SIMULATION_DELAY,
        Err(error) => return Err(error.into()),
    };
    if delay > MAX_SIMULATION_DELAY {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "FULFILLMENT_DELAY_MS must not exceed 30000",
        )
        .into());
    }
    Ok(delay)
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

    if header.event_type != "OrderAccepted" {
        let reason = if matches!(
            header.event_type.as_str(),
            "OrderCreated" | "OrderRejected" | "OrderShipped"
        ) {
            "known event type is not handled by the fulfillment consumer".to_owned()
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
        Ok(event) => ParsedRecord::OrderAccepted(event),
        Err(error) => ParsedRecord::Skip {
            event_id: Some(header.event_id),
            event_type: Some(header.event_type),
            reason: format!("malformed OrderAccepted payload: {error}"),
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
