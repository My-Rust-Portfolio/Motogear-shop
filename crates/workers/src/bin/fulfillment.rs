use std::{env, error::Error, io, time::Duration};

use motogear_persistence::{
    connect_with,
    fulfillment::{
        FULFILLMENT_CONSUMER_NAME, FulfillmentOutcome, FulfillmentProcessError, OrderAcceptedEvent,
        fulfillment_event_is_processed, process_order_accepted,
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
use tokio::time::sleep;

const DEFAULT_SIMULATION_DELAY: Duration = Duration::from_secs(2);
const MAX_SIMULATION_DELAY: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Loading .env is optional because Compose can provide environment variables.
    let _ = dotenvy::dotenv();

    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let topic = env::var("KAFKA_ORDERS_TOPIC").unwrap_or_else(|_| "shop.orders.v1".to_owned());
    let simulation_delay = simulation_delay_from_env()?;
    let pool = connect_with(postgres_options_from_env()?).await?;
    let consumer = build_consumer(ConsumerSettings {
        brokers: &brokers,
        group_id: FULFILLMENT_CONSUMER_NAME,
    })?;
    consumer.subscribe(&[&topic])?;

    println!(
        "Fulfillment consumer subscribed to {topic} at {brokers}; simulation_delay_ms={}",
        simulation_delay.as_millis()
    );
    let mut shutdown_signal = ShutdownSignal::new();

    'message_loop: loop {
        let message = match recv_or_shutdown(&consumer, &mut shutdown_signal).await? {
            ReceiveOutcome::Message(message) => message,
            ReceiveOutcome::Shutdown => {
                println!("Shutdown requested; fulfillment consumer stopped");
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
                signal_result = shutdown_signal.wait() => {
                    signal_result?;
                    println!("Shutdown requested during simulated fulfillment; current event remains uncommitted");
                    break 'message_loop;
                },
                _ = sleep(simulation_delay) => {}
            }
        }

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
            || process_order_accepted(&pool, &event),
            |error| matches!(error, FulfillmentProcessError::InvalidEvent(_)),
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

fn parse_record(payload: &[u8]) -> ParsedRecord<OrderAcceptedEvent> {
    parse_event_record(
        payload,
        EventSpec {
            handled_types: &["OrderAccepted"],
            all_known_types: &[
                "OrderCreated",
                "OrderAccepted",
                "OrderRejected",
                "OrderShipped",
            ],
            consumer_name: "fulfillment",
            payload_name: "OrderAccepted",
        },
    )
}
