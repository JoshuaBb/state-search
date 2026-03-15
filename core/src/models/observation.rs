use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

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
    pub context_id:    Option<i64>,
    pub ingest_run_id: Option<Uuid>,
    pub inserted_at:   DateTime<Utc>,
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
    pub context_id:    Option<i64>,
    pub ingest_run_id: Uuid,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    #[test]
    fn new_observation_has_ingest_run_id() {
        let _ = NewObservation {
            raw_import_id: None,
            location_id:   None,
            time_id:       None,
            source_name:   None,
            metric_name:   "test".to_string(),
            metric_value:  None,
            attributes:    None,
            context_id:    None,
            ingest_run_id: Uuid::new_v4(),
        };
    }

    #[test]
    fn observation_has_ingest_run_id_and_inserted_at() {
        fn assert_fields(o: &Observation) {
            let _: &Option<Uuid> = &o.ingest_run_id;
            let _: &chrono::DateTime<Utc> = &o.inserted_at;
        }
        let _ = assert_fields;
    }
}
