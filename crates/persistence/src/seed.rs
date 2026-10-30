use sqlx::PgPool;

use crate::PersistenceError;

const CUSTOMERS: &[(&str, &str)] = &[
    ("Alice Morgan", "alice@example.test"),
    ("Bob Nguyen", "bob@example.test"),
    ("Casey Eriksson", "casey@example.test"),
];

const PRODUCTS: &[(&str, &str, &str, i64, i32)] = &[
    (
        "HELMET-TOUR-01",
        "Touring helmet",
        "A comfortable full-face helmet for long rides.",
        14_900,
        3,
    ),
    (
        "GLOVES-ROAD-01",
        "Road gloves",
        "Protective leather gloves with reinforced palms.",
        4_900,
        8,
    ),
    (
        "JACKET-TEXTILE-01",
        "Textile jacket",
        "Weather-resistant jacket with removable thermal lining.",
        18_900,
        4,
    ),
    (
        "BOOTS-TOUR-01",
        "Touring boots",
        "Water-resistant boots with ankle and toe protection.",
        12_900,
        5,
    ),
    (
        "BAG-TAIL-01",
        "Tail luggage bag",
        "Compact expandable luggage for weekend trips.",
        8_900,
        2,
    ),
    (
        "MOUNT-PHONE-01",
        "Handlebar phone mount",
        "A vibration-dampened mount for compatible phones.",
        3_900,
        1,
    ),
    (
        "LOCK-DISC-01",
        "Disc brake lock",
        "A compact alarmed lock for short stops.",
        2_900,
        10,
    ),
    (
        "SUIT-RAIN-01",
        "Rain suit",
        "A lightweight two-piece suit for wet-weather rides.",
        7_900,
        3,
    ),
];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SeedSummary {
    pub customers_inserted: u64,
    pub products_inserted: u64,
}

/// Inserts missing demo rows without changing existing customers or products.
///
/// Product conflicts are ignored by SKU, so running this after orders have
/// consumed stock never restores the original seed quantity.
pub async fn seed_demo_data(pool: &PgPool) -> Result<SeedSummary, PersistenceError> {
    let mut transaction = pool.begin().await?;
    let mut summary = SeedSummary::default();

    for &(name, email) in CUSTOMERS {
        let result = sqlx::query(
            "INSERT INTO customers (name, email) VALUES ($1, $2) \
             ON CONFLICT (email) DO NOTHING",
        )
        .bind(name)
        .bind(email)
        .execute(&mut *transaction)
        .await?;

        summary.customers_inserted += result.rows_affected();
    }

    for &(sku, name, description, price_cents, stock_available) in PRODUCTS {
        let result = sqlx::query(
            "INSERT INTO products (sku, name, description, price_cents, stock_available) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (sku) DO NOTHING",
        )
        .bind(sku)
        .bind(name)
        .bind(description)
        .bind(price_cents)
        .bind(stock_available)
        .execute(&mut *transaction)
        .await?;

        summary.products_inserted += result.rows_affected();
    }

    transaction.commit().await?;
    Ok(summary)
}
