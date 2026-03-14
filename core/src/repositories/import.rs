use sqlx::PgPool;

use crate::{
    error::Result,
    models::import::{ImportSource, NewImportSource, NewRawImport, RawImport},
};

pub struct ImportRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> ImportRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    // ── Sources ──────────────────────────────────────────────────────────────

    pub async fn create_source(&self, src: NewImportSource) -> Result<ImportSource> {
        let row = sqlx::query_as::<_, ImportSource>(
            "INSERT INTO import_sources (name, description, field_map)
             VALUES ($1, $2, $3)
             RETURNING *",
        )
        .bind(src.name)
        .bind(src.description)
        .bind(src.field_map)
        .fetch_one(self.pool)
        .await?;

        Ok(row)
    }

    pub async fn find_source_by_name(&self, name: &str) -> Result<Option<ImportSource>> {
        let row = sqlx::query_as::<_, ImportSource>(
            "SELECT * FROM import_sources WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(self.pool)
        .await?;

        Ok(row)
    }

    pub async fn list_sources(&self) -> Result<Vec<ImportSource>> {
        let rows = sqlx::query_as::<_, ImportSource>(
            "SELECT * FROM import_sources ORDER BY name",
        )
        .fetch_all(self.pool)
        .await?;

        Ok(rows)
    }

    // ── Raw imports ──────────────────────────────────────────────────────────

    pub async fn create_raw(&self, raw: NewRawImport) -> Result<RawImport> {
        let row = sqlx::query_as::<_, RawImport>(
            "INSERT INTO raw_imports (source_id, source_file, raw_data)
             VALUES ($1, $2, $3)
             RETURNING *",
        )
        .bind(raw.source_id)
        .bind(raw.source_file)
        .bind(raw.raw_data)
        .fetch_one(self.pool)
        .await?;

        Ok(row)
    }

    pub async fn set_normalized(&self, id: i64, normalized: serde_json::Value) -> Result<()> {
        sqlx::query("UPDATE raw_imports SET normalized_data = $1 WHERE id = $2")
            .bind(normalized)
            .bind(id)
            .execute(self.pool)
            .await?;

        Ok(())
    }
}
