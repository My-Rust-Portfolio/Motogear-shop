use std::{env, error::Error, io, process};

use motogear_persistence::{connect_with, run_migrations};
use sqlx::postgres::PgConnectOptions;

fn main() {
    if let Err(error) = run() {
        eprintln!("database migration failed: {error}");
        process::exit(1);
    }
}

#[tokio::main]
async fn run() -> Result<(), Box<dyn Error>> {
    // Loading `.env` is optional so the same command also works in a container
    // where Compose supplies these variables directly.
    let _ = dotenvy::dotenv();

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

    let options = PgConnectOptions::new()
        .host(&host)
        .port(port)
        .username(&username)
        .password(&password)
        .database(&database);

    let pool = connect_with(options).await?;
    run_migrations(&pool).await?;
    pool.close().await;

    println!("Database migrations are up to date.");
    Ok(())
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
