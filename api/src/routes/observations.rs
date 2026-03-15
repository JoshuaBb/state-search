use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;

use crate::{error::ApiResult, state::AppState};

pub fn router() -> Router<AppState> {
    Router::new().route("/", get(query))
}

#[derive(Debug, Deserialize)]
pub struct NormalizedImportQuery {
    pub source_name: Option<String>,
    pub location_id: Option<i64>,
    pub time_id:     Option<i64>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 { 100 }

async fn query(
    State(state): State<AppState>,
    Query(params): Query<NormalizedImportQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let rows = sqlx::query(
        "SELECT * FROM normalized_imports
         WHERE ($1::text IS NULL OR source_name = $1)
           AND ($2::bigint IS NULL OR location_id = $2)
           AND ($3::bigint IS NULL OR time_id = $3)
         ORDER BY id
         LIMIT $4 OFFSET $5",
    )
    .bind(params.source_name)
    .bind(params.location_id)
    .bind(params.time_id)
    .bind(params.limit)
    .bind(params.offset)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(state_search_core::error::CoreError::from)?;

    let json: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "id":              sqlx::Row::get::<i64, _>(r, "id"),
                "source_name":     sqlx::Row::get::<String, _>(r, "source_name"),
                "ingest_run_id":   sqlx::Row::get::<uuid::Uuid, _>(r, "ingest_run_id").to_string(),
                "location_id":     sqlx::Row::get::<Option<i64>, _>(r, "location_id"),
                "time_id":         sqlx::Row::get::<Option<i64>, _>(r, "time_id"),
                "normalized_data": sqlx::Row::get::<serde_json::Value, _>(r, "normalized_data"),
            })
        })
        .collect();

    Ok(Json(serde_json::to_value(json).unwrap()))
}
