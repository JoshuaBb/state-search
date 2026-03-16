mod derived;
mod dimensions;
mod export;
mod record;
mod row;
mod schema;
mod uuid;

pub(super) use uuid::derive_uuid;

use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use futures::{stream, StreamExt};
use state_search_core::{
    config::SourceConfig,
    repositories::normalized_import::NormalizedImportRepository,
    Db,
};
use tracing::{info, warn};
use ::uuid::Uuid;

use crate::transforms::resolve::{build_resolved_field_map, ResolvedFieldMap};
use self::dimensions::{new_location_cache, LocationCache};
use self::export::generate_export_sql;
use self::record::process_record;
use self::row::infer_country_from_path;

const ROW_CONCURRENCY: usize = 8;
const OBS_BATCH_SIZE: usize = 500;
const EXPORT_BASE: &str = "exports";

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

        info!(source = source_name, file = file_path, %ingest_run_id, "starting ingest");

        // Seed import_schema and validate before opening the CSV
        let schema = schema::seed_and_validate(source, self.db)
            .await
            .with_context(|| format!("schema seed/validate failed for source '{source_name}'"))?;

        // Derived fields map: canonical_name → type (used for skip-set in schema functions)
        let derived_fields: HashMap<String, String> = source.derived.iter()
            .map(|(k, v)| (k.clone(), v.field_type.clone()))
            .collect();

        let resolved = build_resolved_field_map(source)
            .with_context(|| format!("failed to resolve field map for source '{source_name}'"))?;

        let mut reader = csv::Reader::from_path(file_path)
            .with_context(|| format!("cannot open {file_path}"))?;

        let headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_string()).collect();

        let default_country = infer_country_from_path(file_path);

        let result = self
            .process_rows(
                source,
                file_path,
                ingest_run_id,
                &resolved,
                &schema,
                &derived_fields,
                &headers,
                default_country.as_deref(),
                &mut reader,
            )
            .await;

        match result {
            Ok(total) => {
                info!(source = source_name, file = file_path, %ingest_run_id, rows = total, "ingest complete");
                self.write_export_script(source, ingest_run_id, &schema, &derived_fields);
                Ok(total)
            }
            Err(e) => {
                warn!(source = source_name, file = file_path, %ingest_run_id, error = %e,
                    "ingest failed — cleaning up partial normalized_imports");
                match NormalizedImportRepository::delete_by_ingest_run(self.db, ingest_run_id).await {
                    Ok(deleted) => info!(%ingest_run_id, deleted, "cleanup complete"),
                    Err(ce)     => warn!(%ingest_run_id, error = %ce, "cleanup failed — manual intervention required"),
                }
                Err(e)
            }
        }
    }

    fn write_export_script(
        &self,
        source: &SourceConfig,
        ingest_run_id: Uuid,
        schema: &HashMap<String, String>,
        derived_fields: &HashMap<String, String>,
    ) {
        let source_name = source.name.as_str();
        let pg_connection = "host=localhost dbname=state_search";

        let sql = generate_export_sql(source_name, ingest_run_id, schema, derived_fields, pg_connection, EXPORT_BASE);

        let dir = format!("{EXPORT_BASE}/{source_name}");
        let path = format!("{dir}/export_{ingest_run_id}.sql");

        if let Err(e) = std::fs::create_dir_all(&dir).and_then(|_| std::fs::write(&path, sql)) {
            warn!(source = source_name, %ingest_run_id, error = %e, "failed to write export script");
        } else {
            info!(source = source_name, %ingest_run_id, path, "export script written");
        }
    }

    async fn process_rows(
        &self,
        source: &SourceConfig,
        file_path: &str,
        ingest_run_id: Uuid,
        resolved: &ResolvedFieldMap,
        schema: &HashMap<String, String>,
        derived_fields: &HashMap<String, String>,
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
                let maybe_row = process_record(
                    record, rn, source, file_path, ingest_run_id,
                    resolved, headers, default_country, db, &cache,
                    schema, derived_fields,
                )
                .await?;
                // Convert Option<NewNormalizedImport> to Vec<NewNormalizedImport>
                Ok::<Vec<_>, anyhow::Error>(maybe_row.into_iter().collect())
            }
        });

        crate::stream::batched_channel_sink(
            work,
            ROW_CONCURRENCY,
            ROW_CONCURRENCY * 4,
            OBS_BATCH_SIZE,
            |batch| async {
                NormalizedImportRepository::new(db).bulk_create(batch).await.map_err(Into::into)
            },
            |total| info!(source = source_name, file = file_path, rows = total, "batch flushed"),
        )
        .await
    }
}
