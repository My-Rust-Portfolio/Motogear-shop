pub mod catalogue;
pub mod config;
pub mod error;
pub mod events;
pub mod fulfillment;
pub mod inventory;
pub mod migrations;
pub mod notifications;
pub mod orders;
pub mod seed;

pub use config::{connect, connect_with, postgres_options_from_env};
pub use error::PersistenceError;
pub use migrations::{run_migrations, schema_is_ready};
pub use sqlx::PgPool;
