use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::{error::ApiResult, state::AppState};

pub fn router() -> Router<AppState> {
    Router::new().route("/", get(query))
}

#[derive(Debug, Deserialize)]
pub struct NormalizedImportQuery {
    pub source_name: Option<String>,
    pub location_id: Option<Uuid>,
    pub time_id:     Option<Uuid>,
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
           AND ($2::uuid IS NULL OR location_id = $2)
           AND ($3::uuid IS NULL OR time_id = $3)
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
                "id":              sqlx::Row::get::<Uuid, _>(r, "id").to_string(),
                "source_name":     sqlx::Row::get::<String, _>(r, "source_name"),
                "ingest_run_id":   sqlx::Row::get::<Uuid, _>(r, "ingest_run_id").to_string(),
                "location_id":     sqlx::Row::get::<Option<Uuid>, _>(r, "location_id").map(|id| id.to_string()),
                "time_id":         sqlx::Row::get::<Option<Uuid>, _>(r, "time_id").map(|id| id.to_string()),
                "normalized_data": sqlx::Row::get::<serde_json::Value, _>(r, "normalized_data"),
            })
        })
        .collect();

    Ok(Json(serde_json::to_value(json).unwrap()))
}
