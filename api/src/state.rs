use std::sync::Arc;

use state_search_core::Db;

/// Shared application state injected into every Axum handler.
#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Db>,
}

impl AppState {
    pub fn new(db: Db) -> Self {
        Self { db: Arc::new(db) }
    }
}
