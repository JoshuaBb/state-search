use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct NewNormalizedImport {
    pub id:              Uuid,
    pub raw_import_id:   Option<i64>,
    pub location_id:     Option<Uuid>,
    pub time_id:         Option<Uuid>,
    pub source_name:     String,
    pub ingest_run_id:   Uuid,
    pub normalized_data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct NormalizedImport {
    pub id:              Uuid,
    pub raw_import_id:   Option<i64>,
    pub location_id:     Option<Uuid>,
    pub time_id:         Option<Uuid>,
    pub source_name:     String,
    pub ingest_run_id:   Uuid,
    pub normalized_data: serde_json::Value,
    pub inserted_at:     DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_normalized_import_fields() {
        let _ = NewNormalizedImport {
            id:              Uuid::new_v4(),
            raw_import_id:   Some(1),
            location_id:     Some(Uuid::new_v4()),
            time_id:         Some(Uuid::new_v4()),
            source_name:     "s".to_string(),
            ingest_run_id:   Uuid::new_v4(),
            normalized_data: serde_json::json!({"analyte": "TTHM"}),
        };
    }
}
