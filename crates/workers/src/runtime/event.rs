use serde::{Deserialize, de::DeserializeOwned};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct EventHeader {
    pub event_id: Uuid,
    pub event_type: String,
    pub schema_version: i32,
}

pub struct EventSpec<'a> {
    pub handled_types: &'a [&'a str],
    pub all_known_types: &'a [&'a str],
    pub consumer_name: &'a str,
    pub payload_name: &'a str,
}

#[derive(Debug)]
pub struct SkippedRecord {
    pub event_id: Option<Uuid>,
    pub event_type: Option<String>,
    pub reason: String,
}

pub enum ParsedRecord<T> {
    Event(T),
    Skip(SkippedRecord),
}

pub fn parse_record<T: DeserializeOwned>(payload: &[u8], spec: EventSpec<'_>) -> ParsedRecord<T> {
    let header: EventHeader = match serde_json::from_slice(payload) {
        Ok(header) => header,
        Err(error) => {
            return ParsedRecord::Skip(SkippedRecord {
                event_id: None,
                event_type: None,
                reason: format!("malformed event envelope: {error}"),
            });
        }
    };

    if header.schema_version != 1 {
        return ParsedRecord::Skip(SkippedRecord {
            event_id: Some(header.event_id),
            event_type: Some(header.event_type),
            reason: format!("unsupported schema version {}", header.schema_version),
        });
    }

    if !spec.handled_types.contains(&header.event_type.as_str()) {
        let reason = if spec.all_known_types.contains(&header.event_type.as_str()) {
            format!(
                "known event type is not handled by the {} consumer",
                spec.consumer_name
            )
        } else {
            "unknown event type".to_owned()
        };
        return ParsedRecord::Skip(SkippedRecord {
            event_id: Some(header.event_id),
            event_type: Some(header.event_type),
            reason,
        });
    }

    match serde_json::from_slice(payload) {
        Ok(event) => ParsedRecord::Event(event),
        Err(error) => ParsedRecord::Skip(SkippedRecord {
            event_id: Some(header.event_id),
            event_type: Some(header.event_type),
            reason: format!("malformed {} payload: {error}", spec.payload_name),
        }),
    }
}
