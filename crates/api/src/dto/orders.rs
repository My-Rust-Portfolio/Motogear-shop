use motogear_persistence::orders::{OrderItemRecord, OrderRecord};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct CreateOrderRequest {
    pub customer_id: Uuid,
    pub items: Vec<CreateOrderItem>,
}

#[derive(Debug, Deserialize)]
pub struct CreateOrderItem {
    pub product_id: Uuid,
    pub quantity: i32,
}

#[derive(Serialize)]
pub struct OrderSubmissionResponse {
    pub id: Uuid,
    pub status: String,
}

#[derive(Serialize)]
pub struct OrderResponse {
    pub id: Uuid,
    pub customer_id: Uuid,
    pub status: String,
    pub total_cents: i64,
    pub currency: String,
    pub rejection_reason: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub tracking_reference: Option<String>,
    pub shipped_at: Option<String>,
    pub items: Vec<OrderItemResponse>,
}

#[derive(Serialize)]
pub struct OrderItemResponse {
    pub product_id: Uuid,
    pub sku: String,
    pub name: String,
    pub quantity: i32,
    pub unit_price_cents: i64,
}

impl From<OrderRecord> for OrderResponse {
    fn from(record: OrderRecord) -> Self {
        Self {
            id: record.id,
            customer_id: record.customer_id,
            status: record.status,
            total_cents: record.total_cents,
            currency: record.currency,
            rejection_reason: record.rejection_reason,
            created_at: record.created_at,
            updated_at: record.updated_at,
            tracking_reference: record.tracking_reference,
            shipped_at: record.shipped_at,
            items: record
                .items
                .into_iter()
                .map(OrderItemResponse::from)
                .collect(),
        }
    }
}

impl From<OrderItemRecord> for OrderItemResponse {
    fn from(record: OrderItemRecord) -> Self {
        Self {
            product_id: record.product_id,
            sku: record.sku,
            name: record.name,
            quantity: record.quantity,
            unit_price_cents: record.unit_price_cents,
        }
    }
}
