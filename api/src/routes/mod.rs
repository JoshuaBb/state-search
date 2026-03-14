pub mod health;
pub mod locations;
pub mod observations;
pub mod sources;

use axum::Router;

use crate::state::AppState;

/// All JSON API routes live under /api/*
pub fn api_router() -> Router<AppState> {
    Router::new()
        .nest("/health",       health::router())
        .nest("/sources",      sources::router())
        .nest("/locations",    locations::router())
        .nest("/observations", observations::router())
}
