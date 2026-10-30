use std::error::Error;

use rdkafka::{
    ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::KafkaError,
    message::{BorrowedMessage, Message},
};
use uuid::Uuid;

use super::{ShutdownSignal, SkippedRecord};

pub struct ConsumerSettings<'a> {
    pub brokers: &'a str,
    pub group_id: &'a str,
}

pub fn build_consumer(settings: ConsumerSettings<'_>) -> Result<StreamConsumer, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", settings.brokers)
        .set("group.id", settings.group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .create()
}

pub enum ReceiveOutcome<'a> {
    Message(BorrowedMessage<'a>),
    Shutdown,
}

pub async fn recv_or_shutdown<'a>(
    consumer: &'a StreamConsumer,
    shutdown: &mut ShutdownSignal,
) -> Result<ReceiveOutcome<'a>, Box<dyn Error>> {
    tokio::select! {
        biased;
        signal_result = shutdown.wait() => {
            signal_result?;
            Ok(ReceiveOutcome::Shutdown)
        },
        message = consumer.recv() => Ok(ReceiveOutcome::Message(message?)),
    }
}

pub fn commit_skipped_record(
    consumer: &StreamConsumer,
    message: &BorrowedMessage<'_>,
    skipped: &SkippedRecord,
) -> Result<(), KafkaError> {
    eprintln!(
        "topic={} partition={} offset={} event_id={} event_type={} result=skipped reason={:?}",
        message.topic(),
        message.partition(),
        message.offset(),
        skipped
            .event_id
            .map_or_else(|| "unknown".to_owned(), |id: Uuid| id.to_string()),
        skipped.event_type.as_deref().unwrap_or("unknown"),
        skipped.reason
    );
    consumer.commit_message(message, CommitMode::Sync)
}

pub fn payload_or_skip(payload: Option<&[u8]>) -> Result<&[u8], SkippedRecord> {
    payload.ok_or_else(|| SkippedRecord {
        event_id: None,
        event_type: None,
        reason: "Kafka record has no payload".to_owned(),
    })
}
