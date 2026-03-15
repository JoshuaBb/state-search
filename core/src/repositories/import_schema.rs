use std::collections::HashMap;
use sqlx::PgPool;

use crate::{error::Result, models::import_schema_field::NewImportSchemaField};

pub struct ImportSchemaRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> ImportSchemaRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// Upsert schema field definitions for a source.
    /// ON CONFLICT updates field_type; does not overwrite created_at or description.
    pub async fn upsert_fields(&self, fields: Vec<NewImportSchemaField>) -> Result<()> {
        for field in fields {
            sqlx::query(
                "INSERT INTO import_schema (source_name, field_name, field_type)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (source_name, field_name)
                 DO UPDATE SET field_type = EXCLUDED.field_type",
            )
            .bind(&field.source_name)
            .bind(&field.field_name)
            .bind(&field.field_type)
            .execute(self.pool)
            .await?;
        }
        Ok(())
    }

    /// Fetch all field definitions for a source, keyed by canonical field name.
    /// Returns canonical_name → postgres type string.
    pub async fn load_for_source(&self, source_name: &str) -> Result<HashMap<String, String>> {
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT field_name, field_type FROM import_schema WHERE source_name = $1",
        )
        .bind(source_name)
        .fetch_all(self.pool)
        .await?;

        Ok(rows.into_iter().collect())
    }
}
