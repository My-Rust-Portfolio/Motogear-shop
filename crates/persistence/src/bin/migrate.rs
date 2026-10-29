use std::{error::Error, process};

use motogear_persistence::{config::postgres_options_from_env, connect_with, run_migrations};

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

    let pool = connect_with(postgres_options_from_env()?).await?;
    run_migrations(&pool).await?;
    pool.close().await;

    println!("Database migrations are up to date.");
    Ok(())
}
