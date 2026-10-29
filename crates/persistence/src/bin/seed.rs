use std::{error::Error, process};

use motogear_persistence::{config::postgres_options_from_env, connect_with, seed::seed_demo_data};

fn main() {
    if let Err(error) = run() {
        eprintln!("demo seed failed: {error}");
        process::exit(1);
    }
}

#[tokio::main]
async fn run() -> Result<(), Box<dyn Error>> {
    // Loading `.env` is optional so the same command also works in a container
    // where Compose supplies these variables directly.
    let _ = dotenvy::dotenv();

    let pool = connect_with(postgres_options_from_env()?).await?;
    let summary = seed_demo_data(&pool).await?;
    pool.close().await;

    println!(
        "Seed complete: inserted {} customers and {} products. Existing rows were left unchanged.",
        summary.customers_inserted, summary.products_inserted
    );
    Ok(())
}
