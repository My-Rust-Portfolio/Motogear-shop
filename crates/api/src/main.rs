use std::error::Error;

use motogear_api::{config::Settings, create_router};
use motogear_persistence::{connect_with, postgres_options_from_env};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let settings = Settings::from_env();
    let pool = connect_with(postgres_options_from_env()?).await?;
    let listener = TcpListener::bind(&settings.api_bind_address).await?;

    println!(
        "MotoGear API listening on http://{}",
        settings.api_bind_address
    );
    axum::serve(listener, create_router(pool)).await?;
    Ok(())
}
