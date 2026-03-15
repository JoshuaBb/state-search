use chrono::{DateTime, Utc};
use serde_json::Value;

pub struct NewRowContext {
    pub source_name: String,
    pub attributes:  Value,
}

pub struct RowContext {
    pub id:          i64,
    pub source_name: String,
    pub attributes:  Value,
    pub inserted_at: DateTime<Utc>,
}
