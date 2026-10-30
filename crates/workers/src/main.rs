use std::{
    env,
    error::Error,
    time::{Duration, Instant},
};

use motogear_persistence::{
    connect_with,
    events::outbox::{
        mark_outbox_event_published, next_unpublished_event, record_outbox_publish_failure,
    },
    postgres_options_from_env,
};
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use tokio::time::sleep;

const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(500);
const RETRY_DELAY: Duration = Duration::from_secs(2);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Loading .env is optional because Compose can provide environment variables.
    let _ = dotenvy::dotenv();

    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let pool = connect_with(postgres_options_from_env()?).await?;
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "15000")
        .create()?;

    println!("Outbox publisher connected to Kafka brokers at {brokers}");
    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);

    loop {
        let next_event = tokio::select! {
            biased;
            signal_result = &mut shutdown_signal => {
                signal_result?;
                println!("Shutdown requested; publisher will stop before fetching another event");
                break;
            },
            result = next_unpublished_event(&pool) => result,
        };

        let event = match next_event {
            Ok(Some(event)) => event,
            Ok(None) => {
                tokio::select! {
                    biased;
                    signal_result = &mut shutdown_signal => {
                        signal_result?;
                        println!("Shutdown requested; publisher stopped");
                        break;
                    },
                    _ = sleep(IDLE_POLL_INTERVAL) => continue,
                }
            }
            Err(error) => {
                eprintln!("operation=read_outbox result=failed error={error:?}");
                sleep(RETRY_DELAY).await;
                continue;
            }
        };

        let attempt = event.publish_attempts.saturating_add(1);
        let started_at = Instant::now();
        let payload = event.payload.0.to_string();
        let record = FutureRecord::to(&event.target_topic)
            .key(&event.message_key)
            .payload(&payload);

        match producer
            .send(record, Timeout::After(DELIVERY_TIMEOUT))
            .await
        {
            Ok(_delivery) => match mark_outbox_event_published(&pool, event.event_id).await {
                Ok(true) => println!(
                    "event_id={} order_id={} topic={:?} attempt={} delivery_result=acknowledged elapsed_ms={}",
                    event.event_id,
                    event.aggregate_id,
                    event.target_topic,
                    attempt,
                    started_at.elapsed().as_millis()
                ),
                Ok(false) => eprintln!(
                    "event_id={} order_id={} topic={:?} attempt={} delivery_result=acknowledged outbox_update=missing elapsed_ms={}",
                    event.event_id,
                    event.aggregate_id,
                    event.target_topic,
                    attempt,
                    started_at.elapsed().as_millis()
                ),
                Err(error) => {
                    eprintln!(
                        "event_id={} order_id={} topic={:?} attempt={} delivery_result=acknowledged outbox_update=failed error={error:?} elapsed_ms={}; event may be sent again",
                        event.event_id,
                        event.aggregate_id,
                        event.target_topic,
                        attempt,
                        started_at.elapsed().as_millis()
                    );
                    sleep(RETRY_DELAY).await;
                }
            },
            Err((error, _message)) => {
                let error = error.to_string();
                eprintln!(
                    "event_id={} order_id={} topic={:?} attempt={} delivery_result=failed error={error:?} elapsed_ms={}",
                    event.event_id,
                    event.aggregate_id,
                    event.target_topic,
                    attempt,
                    started_at.elapsed().as_millis()
                );
                if let Err(database_error) =
                    record_outbox_publish_failure(&pool, event.event_id, &error).await
                {
                    eprintln!(
                        "event_id={} order_id={} operation=record_publish_failure result=failed error={database_error:?}",
                        event.event_id, event.aggregate_id
                    );
                }
                sleep(RETRY_DELAY).await;
            }
        }
    }

    Ok(())
}
