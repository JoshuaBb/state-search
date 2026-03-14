use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use state_search_core::repositories::location::LocationRepository;

use crate::{error::ApiResult, state::AppState};

pub fn router() -> Router<AppState> {
    Router::new().route("/", get(list))
}

#[derive(Debug, Deserialize)]
pub struct Pagination {
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 { 200 }

async fn list(
    State(state): State<AppState>,
    Query(params): Query<Pagination>,
) -> ApiResult<Json<serde_json::Value>> {
    let repo = LocationRepository::new(&state.db);
    let locs = repo.list(params.limit, params.offset).await?;
    Ok(Json(serde_json::to_value(locs).unwrap()))
}
