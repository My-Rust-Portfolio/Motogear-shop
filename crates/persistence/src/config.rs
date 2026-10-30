//! Database settings shared by the local migration and seed commands.

use std::{env, io};

use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

/// Opens a PostgreSQL connection pool from a connection URL.
pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await
}

/// Opens a PostgreSQL pool from structured connection settings.
pub async fn connect_with(options: PgConnectOptions) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(10)
        .connect_with(options)
        .await
}

/// Builds PostgreSQL connection options from the Compose-compatible environment.
///
/// The defaults target PostgreSQL published on the host at `127.0.0.1:5433`.
/// A container can override `POSTGRES_HOST=postgres` and `POSTGRES_PORT=5432`.
pub fn postgres_options_from_env() -> Result<PgConnectOptions, io::Error> {
    let username = required_env("POSTGRES_USER")?;
    let password = required_env("POSTGRES_PASSWORD")?;
    let database = required_env("POSTGRES_DB")?;
    let host = env_or("POSTGRES_HOST", "127.0.0.1");
    let port = env_or("POSTGRES_PORT", "5433")
        .parse::<u16>()
        .map_err(|source| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("POSTGRES_PORT must be a valid port number: {source}"),
            )
        })?;

    Ok(PgConnectOptions::new()
        .host(&host)
        .port(port)
        .username(&username)
        .password(&password)
        .database(&database))
}

fn required_env(name: &str) -> Result<String, io::Error> {
    env::var(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be set in the environment or .env file"),
        )
    })
}

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}
