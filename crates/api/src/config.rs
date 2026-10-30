use std::env;

#[derive(Debug, Clone)]
pub struct Settings {
    pub api_bind_address: String,
}

impl Settings {
    pub fn from_env() -> Self {
        // Loading .env is optional so Compose can provide variables directly.
        let _ = dotenvy::dotenv();
        let api_bind_address =
            env::var("API_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());

        Self { api_bind_address }
    }
}
