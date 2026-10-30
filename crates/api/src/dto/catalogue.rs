use motogear_persistence::catalogue::{CustomerRecord, ProductRecord};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct PageQuery {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Serialize)]
pub struct CustomerResponse {
    pub id: Uuid,
    pub name: String,
    pub email: String,
}

impl From<CustomerRecord> for CustomerResponse {
    fn from(record: CustomerRecord) -> Self {
        Self {
            id: record.id,
            name: record.name,
            email: record.email,
        }
    }
}

#[derive(Serialize)]
pub struct ProductResponse {
    pub id: Uuid,
    /// Stock Keeping Unit.
    pub sku: String,
    pub name: String,
    pub description: String,
    pub price_cents: i64,
    pub currency: &'static str,
    pub stock_available: i32,
}

impl From<ProductRecord> for ProductResponse {
    fn from(record: ProductRecord) -> Self {
        Self {
            id: record.id,
            sku: record.sku,
            name: record.name,
            description: record.description,
            price_cents: record.price_cents,
            currency: "EUR",
            stock_available: record.stock_available,
        }
    }
}
