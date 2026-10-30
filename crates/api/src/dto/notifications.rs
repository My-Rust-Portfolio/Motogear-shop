use motogear_persistence::notifications::CustomerNotificationRecord;
use serde::Serialize;
use uuid::Uuid;

#[derive(Serialize)]
pub struct NotificationResponse {
    pub id: Uuid,
    pub order_id: Uuid,
    pub kind: String,
    pub message: String,
    pub created_at: String,
}

impl From<CustomerNotificationRecord> for NotificationResponse {
    fn from(record: CustomerNotificationRecord) -> Self {
        Self {
            id: record.id,
            order_id: record.order_id,
            kind: record.kind,
            message: record.message,
            created_at: record.created_at,
        }
    }
}
