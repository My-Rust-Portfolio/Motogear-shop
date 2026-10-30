use std::{
    error::Error,
    fmt::Debug,
    future::Future,
    io,
    time::{Duration, Instant},
};

use tokio::time::sleep;
use uuid::Uuid;

use super::{ShutdownSignal, SkippedRecord};

const RETRY_START: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);
const MAX_RETRY_DURATION: Duration = Duration::from_secs(5 * 60);

pub struct EventLogContext {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub event_id: Uuid,
    pub order_id: Uuid,
    pub event_type: Option<String>,
}

impl EventLogContext {
    fn fields(&self) -> String {
        let mut fields = format!(
            "topic={} partition={} offset={} event_id={} order_id={}",
            self.topic, self.partition, self.offset, self.event_id, self.order_id
        );
        if let Some(event_type) = &self.event_type {
            fields.push_str(&format!(" event_type={event_type}"));
        }
        fields
    }
}

pub enum RetryOutcome<T> {
    Success(T),
    Invalid(SkippedRecord),
    Shutdown,
}

pub async fn retry_database_operation<T, E, F, Fut, IsInvalid>(
    context: &EventLogContext,
    shutdown: &mut ShutdownSignal,
    mut operation: F,
    is_invalid: IsInvalid,
) -> Result<RetryOutcome<T>, Box<dyn Error>>
where
    E: Debug + Error + 'static,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    IsInvalid: Fn(&E) -> bool,
{
    let retry_started = Instant::now();
    let mut retry_delay = RETRY_START;

    loop {
        let remaining = MAX_RETRY_DURATION.saturating_sub(retry_started.elapsed());
        if remaining.is_zero() {
            eprintln!("{} result=retry_deadline_exceeded", context.fields());
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "database processing exceeded five minutes for event {}",
                    context.event_id
                ),
            )
            .into());
        }

        match tokio::time::timeout(remaining, operation()).await {
            Ok(Ok(outcome)) => return Ok(RetryOutcome::Success(outcome)),
            Ok(Err(error)) if is_invalid(&error) => {
                return Ok(RetryOutcome::Invalid(SkippedRecord {
                    event_id: Some(context.event_id),
                    event_type: context.event_type.clone(),
                    reason: error.to_string(),
                }));
            }
            Ok(Err(error)) => {
                if retry_started.elapsed() >= MAX_RETRY_DURATION {
                    return Err(Box::new(error));
                }
                let remaining = MAX_RETRY_DURATION.saturating_sub(retry_started.elapsed());
                let wait = retry_delay.min(remaining);
                eprintln!(
                    "{} result=retry_database_error error={error:?} retry_in_ms={}",
                    context.fields(),
                    wait.as_millis()
                );
                tokio::select! {
                    biased;
                    signal_result = shutdown.wait() => {
                        signal_result?;
                        println!("Shutdown requested; current event remains uncommitted");
                        return Ok(RetryOutcome::Shutdown);
                    },
                    _ = sleep(wait) => {}
                }
                retry_delay = retry_delay.saturating_mul(2).min(RETRY_MAX);
            }
            Err(_) => {
                eprintln!("{} result=retry_deadline_exceeded", context.fields());
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "database processing exceeded five minutes for event {}",
                        context.event_id
                    ),
                )
                .into());
            }
        }
    }
}
