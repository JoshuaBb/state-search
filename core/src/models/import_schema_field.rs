use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone)]
pub struct NewImportSchemaField {
    pub source_name: String,
    pub field_name:  String,
    pub field_type:  String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ImportSchemaField {
    pub id:          i32,
    pub source_name: String,
    pub field_name:  String,
    pub field_type:  String,
    pub created_at:  DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_import_schema_field_fields() {
        let _ = NewImportSchemaField {
            source_name: "co_public_drinking_water".to_string(),
            field_name:  "year".to_string(),
            field_type:  "smallint".to_string(),
        };
    }
}
