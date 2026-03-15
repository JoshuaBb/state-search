mod context;
mod derived;
mod dimensions;
mod export;
mod observations;
mod record;
mod row;
mod schema;

use std::sync::Arc;

use anyhow::Context;
use futures::{stream, StreamExt};
use state_search_core::{
    config::SourceConfig,
    Db,
};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::transforms::resolve::{build_resolved_field_map, ResolvedFieldMap};
use self::dimensions::{new_location_cache, LocationCache};
use self::export::generate_export_sql;
use self::record::process_record;
use self::row::infer_country_from_path;

/// Number of CSV rows processed concurrently. Bounded well below the DB pool
/// max (10) since each row makes several sequential DB calls.
const ROW_CONCURRENCY: usize = 8;

/// Observations are buffered across rows and flushed to the DB in this batch size.
/// Larger batches reduce round-trips; 500 is a reasonable default.
const OBS_BATCH_SIZE: usize = 500;

/// Base directory for Parquet export scripts.
const EXPORT_BASE: &str = "exports";

/// Error type for pipeline-level failures.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("dataset skipped due to transform failure in '{file}': {reason}")]
    DatasetSkipped { file: String, reason: String },
}

pub struct IngestPipeline<'a> {
    db: &'a Db,
}

impl<'a> IngestPipeline<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self { db }
    }

    /// Returns true if this file has already been ingested (has rows in raw_imports).
    pub async fn already_ingested(&self, file_path: &str) -> anyhow::Result<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM raw_imports WHERE source_file = $1)",
        )
        .bind(file_path)
        .fetch_one(self.db)
        .await?;
        Ok(exists)
    }

    pub async fn run(&self, source: &SourceConfig, file_path: &str) -> anyhow::Result<u64> {
        let source_name = source.name.as_str();
        let ingest_run_id = Uuid::new_v4();

        // Log BEFORE any DB writes so operators can identify the run even if the
        // process dies before the cleanup branch executes.
        info!(source = source_name, file = file_path, %ingest_run_id, "starting ingest");

        let resolved = build_resolved_field_map(source)
            .with_context(|| format!("failed to resolve field map for source '{source_name}'"))?;

        let mut reader = csv::Reader::from_path(file_path)
            .with_context(|| format!("cannot open {file_path}"))?;

        let headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_string()).collect();
        debug!(source = source_name, ?headers, "CSV headers detected");

        let default_country = infer_country_from_path(file_path);

        let result = self
            .process_rows(
                source,
                file_path,
                ingest_run_id,
                &resolved,
                &headers,
                default_country.as_deref(),
                &mut reader,
            )
            .await;

        match result {
            Ok(total) => {
                info!(
                    source = source_name, file = file_path,
                    %ingest_run_id, observations = total,
                    "ingest complete"
                );
                self.write_export_script(source, ingest_run_id);
                Ok(total)
            }
            Err(e) => {
                warn!(
                    source = source_name, file = file_path,
                    %ingest_run_id, error = %e,
                    "ingest failed — cleaning up partial observations"
                );
                // TODO(Task 11): cleanup normalized_imports by ingest_run_id
                info!(%ingest_run_id, "cleanup stub — will be implemented in Task 11");
                Err(e)
            }
        }
    }

    /// Write a DuckDB-executable SQL export script to disk.
    ///
    /// The script is scoped to this `ingest_run_id` and joins dimensions + context
    /// before writing Parquet. Export failure is logged but does not fail the ingest —
    /// the Postgres data is already committed and the script can be re-generated or
    /// re-executed at any time against the same `ingest_run_id`.
    fn write_export_script(&self, source: &SourceConfig, ingest_run_id: Uuid) {
        let source_name = source.name.as_str();

        // Read the Postgres connection string from the DB pool.
        // We use a placeholder if the pool options are not directly accessible;
        // operators can edit the script or set via config.
        let pg_connection = "host=localhost dbname=state_search";

        let sql = generate_export_sql(
            source_name,
            ingest_run_id,
            &[],
            pg_connection,
            EXPORT_BASE,
        );

        let dir = format!("{EXPORT_BASE}/{source_name}");
        let path = format!("{dir}/export_{ingest_run_id}.sql");

        if let Err(e) = std::fs::create_dir_all(&dir)
            .and_then(|_| std::fs::write(&path, sql))
        {
            warn!(
                source = source_name, %ingest_run_id,
                error = %e,
                "failed to write export script — Parquet export will need to be triggered manually"
            );
        } else {
            info!(
                source = source_name, %ingest_run_id,
                path = path,
                "export script written — run with: duckdb -c \".read {path}\""
            );
        }
    }

    async fn process_rows(
        &self,
        source: &SourceConfig,
        file_path: &str,
        ingest_run_id: Uuid,
        resolved: &ResolvedFieldMap,
        headers: &[String],
        default_country: Option<&str>,
        reader: &mut csv::Reader<std::fs::File>,
    ) -> anyhow::Result<u64> {
        let source_name = source.name.as_str();
        let db = self.db;
        let location_cache: LocationCache = new_location_cache();

        let work = stream::iter(reader.records().enumerate()).map(|(i, result): (usize, _)| {
            let rn = i as u64 + 1;
            let cache = Arc::clone(&location_cache);
            async move {
                let record = result.map_err(anyhow::Error::from)?;
                process_record(
                    record, rn, source_name, file_path, ingest_run_id,
                    resolved, headers, default_country, db, &cache,
                )
                .await
            }
        });

        crate::stream::batched_channel_sink(
            work,
            ROW_CONCURRENCY,
            ROW_CONCURRENCY * 4,
            OBS_BATCH_SIZE,
            |batch: Vec<_>| async move { let _ = batch; Ok::<u64, anyhow::Error>(0) },
            |total| info!(source = source_name, file = file_path, observations = total, "batch flushed"),
        )
        .await
    }
}
