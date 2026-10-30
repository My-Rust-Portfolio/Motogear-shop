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
