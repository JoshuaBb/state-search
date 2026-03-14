mod error;
mod routes;
mod state;

use std::path::PathBuf;

use axum::Router;
use state_search_core::{config::AppConfig, db};
use tower_http::{
    compression::CompressionLayer,
    cors::CorsLayer,
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = AppConfig::load()?;

    let pool = db::connect(&cfg.database).await?;
    db::migrate(&pool).await?;

    let app_state = state::AppState::new(pool);

    // Serve the SvelteKit build — fall back to index.html for client-side routing
    let web_dist = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../web/dist");

    let static_service = ServeDir::new(&web_dist)
        .not_found_service(ServeFile::new(web_dist.join("index.html")));

    let app = Router::new()
        // API routes under /api
        .nest("/api", routes::api_router().with_state(app_state))
        // Everything else → SvelteKit SPA
        .fallback_service(static_service)
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive());

    let addr = cfg.server.bind_addr();
    tracing::info!("listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
