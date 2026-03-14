use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use state_search_core::{
    models::import::NewImportSource,
    repositories::import::ImportRepository,
};

use crate::{error::ApiResult, state::AppState};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/",     post(create).get(list))
        .route("/:name", get(find_by_name))
}

async fn create(
    State(state): State<AppState>,
    Json(body): Json<NewImportSource>,
) -> ApiResult<Json<serde_json::Value>> {
    let repo = ImportRepository::new(&state.db);
    let source = repo.create_source(body).await?;
    Ok(Json(serde_json::to_value(source).unwrap()))
}

async fn list(State(state): State<AppState>) -> ApiResult<Json<serde_json::Value>> {
    let repo = ImportRepository::new(&state.db);
    let sources = repo.list_sources().await?;
    Ok(Json(serde_json::to_value(sources).unwrap()))
}

async fn find_by_name(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let repo = ImportRepository::new(&state.db);
    let source = repo
        .find_source_by_name(&name)
        .await?
        .ok_or_else(|| state_search_core::CoreError::NotFound(format!("source '{name}' not found")))?;
    Ok(Json(serde_json::to_value(source).unwrap()))
}
