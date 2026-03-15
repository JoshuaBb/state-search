use sqlx::PgPool;
use uuid::Uuid;

use crate::{error::Result, models::normalized_import::NewNormalizedImport};

pub struct NormalizedImportRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> NormalizedImportRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// Bulk insert normalized import rows using a single UNNEST query.
    pub async fn bulk_create(&self, rows: Vec<NewNormalizedImport>) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let len = rows.len();
        let mut raw_import_ids   = Vec::with_capacity(len);
        let mut location_ids     = Vec::with_capacity(len);
        let mut time_ids         = Vec::with_capacity(len);
        let mut source_names     = Vec::with_capacity(len);
        let mut ingest_run_ids   = Vec::with_capacity(len);
        let mut normalized_datas = Vec::with_capacity(len);

        for row in rows {
            raw_import_ids  .push(row.raw_import_id);
            location_ids    .push(row.location_id);
            time_ids        .push(row.time_id);
            source_names    .push(row.source_name);
            ingest_run_ids  .push(row.ingest_run_id);
            normalized_datas.push(row.normalized_data);
        }

        sqlx::query(
            "INSERT INTO normalized_imports
                 (raw_import_id, location_id, time_id, source_name, ingest_run_id, normalized_data)
             SELECT * FROM UNNEST(
                 $1::bigint[], $2::bigint[], $3::bigint[], $4::text[], $5::uuid[], $6::jsonb[]
             )",
        )
        .bind(&raw_import_ids)
        .bind(&location_ids)
        .bind(&time_ids)
        .bind(&source_names)
        .bind(&ingest_run_ids)
        .bind(&normalized_datas)
        .execute(self.pool)
        .await?;

        Ok(len as u64)
    }

    /// Delete all normalized imports belonging to a given ingest run.
    pub async fn delete_by_ingest_run(pool: &PgPool, ingest_run_id: Uuid) -> Result<u64> {
        let result = sqlx::query(
            "DELETE FROM normalized_imports WHERE ingest_run_id = $1",
        )
        .bind(ingest_run_id)
        .execute(pool)
        .await?;

        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_by_ingest_run_signature_exists() {
        let _ = NormalizedImportRepository::delete_by_ingest_run;
    }

    #[test]
    fn bulk_create_signature_exists() {
        let _ = NormalizedImportRepository::bulk_create;
    }
}
