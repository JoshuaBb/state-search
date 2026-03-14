use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;

/// One normalized metric value linking location + time + source.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Observation {
    pub id:            i64,
    pub raw_import_id: Option<i64>,
    pub location_id:   Option<i64>,
    pub time_id:       Option<i64>,
    pub source_name:   Option<String>,
    pub metric_name:   String,
    pub metric_value:  Option<f64>,
    /// Any additional fields that don't fit the normalized columns.
    pub attributes:    Option<Value>,
}

#[derive(Debug)]
pub struct NewObservation {
    pub raw_import_id: Option<i64>,
    pub location_id:   Option<i64>,
    pub time_id:       Option<i64>,
    pub source_name:   Option<String>,
    pub metric_name:   String,
    pub metric_value:  Option<f64>,
    pub attributes:    Option<Value>,
}
