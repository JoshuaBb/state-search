use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;

/// Registry entry describing a CSV data source and how to map its columns.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ImportSource {
    pub id:          i32,
    pub name:        String,
    pub description: Option<String>,
    /// JSON map of source column names → canonical field names.
    /// e.g. { "STATE_ABBR": "state_code", "REPORT_YEAR": "year", "QTR": "quarter" }
    pub field_map:   Value,
    pub created_at:  DateTime<Utc>,
}

/// One raw CSV row stored verbatim alongside its normalized form.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct RawImport {
    pub id:              i64,
    pub source_id:       Option<i32>,
    pub source_file:     Option<String>,
    pub imported_at:     DateTime<Utc>,
    /// Original CSV row as JSON (column header → value).
    pub raw_data:        Value,
    /// Post-mapping, cleaned version. Populated after normalization.
    pub normalized_data: Option<Value>,
}

// ── Insert payloads ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct NewImportSource {
    pub name:        String,
    pub description: Option<String>,
    pub field_map:   Value,
}

#[derive(Debug)]
pub struct NewRawImport {
    pub source_id:   Option<i32>,
    pub source_file: Option<String>,
    pub raw_data:    Value,
}
