use axum::{routing::get, Json, Router};
use serde_json::{json, Value};

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/", get(handler))
}

async fn handler() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}
