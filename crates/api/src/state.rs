use motogear_persistence::PgPool;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) pool: PgPool,
}

impl AppState {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
