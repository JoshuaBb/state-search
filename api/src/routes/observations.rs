use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use state_search_core::repositories::observation::ObservationRepository;

use crate::{error::ApiResult, state::AppState};

pub fn router() -> Router<AppState> {
    Router::new().route("/", get(query))
}

#[derive(Debug, Deserialize)]
pub struct ObservationQuery {
    pub metric_name: Option<String>,
    pub source_name: Option<String>,
    pub location_id: Option<i64>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 { 100 }

async fn query(
    State(state): State<AppState>,
    Query(params): Query<ObservationQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let repo = ObservationRepository::new(&state.db);
    let results = repo
        .query(
            params.metric_name.as_deref(),
            params.source_name.as_deref(),
            params.location_id,
            params.limit,
            params.offset,
        )
        .await?;

    Ok(Json(serde_json::to_value(results).unwrap()))
}
