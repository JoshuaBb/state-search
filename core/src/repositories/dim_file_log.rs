use sqlx::PgPool;

use crate::{error::Result, models::dim_file_log::NewDimFileLog};

pub struct DimFileLogRepository;

impl DimFileLogRepository {
    /// Append a log entry after a successful dim ingest run.
    pub async fn insert(pool: &PgPool, entry: &NewDimFileLog) -> Result<()> {
        sqlx::query(
            "INSERT INTO dim_file_log (source_name, target, file_path, row_count)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&entry.source_name)
        .bind(&entry.target)
        .bind(&entry.file_path)
        .bind(entry.row_count)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Returns true if this (source_name, file_path) pair has been previously ingested.
    /// The same physical file registered under a different source name is treated as distinct.
    pub async fn exists_for_file(
        pool: &PgPool,
        source_name: &str,
        file_path: &str,
    ) -> Result<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM dim_file_log WHERE source_name = $1 AND file_path = $2)",
        )
        .bind(source_name)
        .bind(file_path)
        .fetch_one(pool)
        .await?;
        Ok(exists)
    }
}
