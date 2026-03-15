use std::collections::HashMap;

use anyhow::Context;
use state_search_core::{
    config::SourceConfig,
    config::OnFailure,
    models::{
        location::NewLocation,
        observation::NewObservation,
        time::NewTimePeriod,
    },
    repositories::{
        location::LocationRepository,
        observation::ObservationRepository,
        time::TimeRepository,
    },
    Db,
};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::transforms::{
    chain::apply_chain,
    resolve::{build_resolved_field_map, ResolvedFieldMap},
    FieldValue,
};

const LOCATION_FIELDS: &[&str] = &[
    "county", "country", "zip_code", "fips_code", "latitude", "longitude",
];
const TIME_FIELDS: &[&str] = &["year", "quarter", "month", "day"];

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
        info!(
            source = source_name,
            file = file_path,
            %ingest_run_id,
            "starting ingest"
        );

        let resolved = build_resolved_field_map(source)
            .with_context(|| format!("failed to resolve field map for source '{source_name}'"))?;

        let mut reader = csv::Reader::from_path(file_path)
            .with_context(|| format!("cannot open {file_path}"))?;

        let headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_string()).collect();
        debug!(source = source_name, ?headers, "CSV headers detected");

        let default_country = infer_country_from_path(file_path);

        let mut total = 0u64;
        let mut row_num = 0u64;

        let result = self.process_rows(
            source,
            file_path,
            ingest_run_id,
            &resolved,
            &headers,
            default_country.as_deref(),
            &mut reader,
            &mut total,
            &mut row_num,
        ).await;

        match result {
            Ok(()) => {
                info!(
                    source = source_name,
                    file = file_path,
                    %ingest_run_id,
                    observations = total,
                    "ingest complete"
                );
                Ok(total)
            }
            Err(e) => {
                warn!(
                    source = source_name,
                    file = file_path,
                    %ingest_run_id,
                    error = %e,
                    "ingest failed — cleaning up partial observations"
                );
                match ObservationRepository::delete_by_ingest_run(self.db, ingest_run_id).await {
                    Ok(deleted) => {
                        info!(
                            %ingest_run_id,
                            deleted,
                            "cleanup complete"
                        );
                    }
                    Err(cleanup_err) => {
                        warn!(
                            %ingest_run_id,
                            error = %cleanup_err,
                            "cleanup failed — manual intervention required"
                        );
                    }
                }
                Err(e)
            }
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
        total: &mut u64,
        row_num: &mut u64,
    ) -> anyhow::Result<()> {
        let source_name = source.name.as_str();
        let db = self.db;

        for record in reader.records() {
            let record = record?;
            *row_num += 1;

            // 1. Build raw map: trim all strings, empty → Null
            let raw = row_to_canonical(headers, &record);
            debug!(row = row_num, "parsed and trimmed CSV row");

            // 2. Store raw row
            let raw_json = canonical_to_json(&raw);
            sqlx::query(
                "INSERT INTO raw_imports (source_id, source_file, raw_data) VALUES ($1, $2, $3)",
            )
            .bind(Option::<i64>::None)
            .bind(file_path)
            .bind(&raw_json)
            .execute(db)
            .await?;

            // 3. Apply field transforms per resolved field
            let mut transformed: HashMap<String, FieldValue> = HashMap::new();
            let mut skip_row = false;

            for (canonical, resolved_field) in resolved {
                let raw_val = raw
                    .get(resolved_field.source_col.as_str())
                    .cloned()
                    .unwrap_or(FieldValue::Null);

                match apply_chain(raw_val, &resolved_field.chain) {
                    Ok(v) => { transformed.insert(canonical.clone(), v); }
                    Err(OnFailure::SkipRow) => {
                        warn!(
                            row = row_num,
                            field = canonical,
                            source = source_name,
                            "SkipRow: discarding row"
                        );
                        skip_row = true;
                        break;
                    }
                    Err(OnFailure::SkipDataset) => {
                        return Err(IngestError::DatasetSkipped {
                            file: file_path.to_string(),
                            reason: format!(
                                "transform failure on field '{}' at row {}",
                                canonical, row_num
                            ),
                        }.into());
                    }
                    // Ignore is handled inside apply_chain (returns Ok(Null)) — this arm
                    // is unreachable but kept for exhaustive matching.
                    Err(OnFailure::Ignore) => unreachable!(),
                }
            }

            // Check SkipRow BEFORE unmapped-column passthrough to avoid unnecessary work.
            if skip_row { continue; }

            // Pass through unmapped columns (preserve existing metric-extraction behavior)
            for (col, val) in &raw {
                if !resolved.values().any(|r| r.source_col == *col) {
                    transformed.insert(col.clone(), val.clone());
                }
            }

            // 4. Resolve dimensions
            let location_id = match resolve_location(&transformed, default_country) {
                Ok(loc) => match LocationRepository::new(db).upsert(loc).await {
                    Ok(id) => { debug!(row = row_num, location_id = id, "resolved location"); Some(id) }
                    Err(e) => { warn!(row = row_num, error = %e, "could not resolve location"); None }
                },
                Err(_) => None,
            };

            let time_id = match resolve_time(&transformed) {
                Ok(t) => match TimeRepository::new(db).upsert(t).await {
                    Ok(id) => { debug!(row = row_num, time_id = id, "resolved time"); Some(id) }
                    Err(e) => { warn!(row = row_num, error = %e, "could not resolve time"); None }
                },
                Err(_) => None,
            };

            // 5. Extract metrics
            let metrics = extract_metrics(&transformed);
            if metrics.is_empty() {
                warn!(row = row_num, "no metric fields found");
            }

            // Note: raw_import_id is set to NULL — a future migration can link them if needed.
            let observations: Vec<NewObservation> = metrics
                .into_iter()
                .map(|(name, value)| NewObservation {
                    raw_import_id: None,
                    location_id,
                    time_id,
                    source_name: Some(source_name.to_string()),
                    metric_name: name,
                    metric_value: value,
                    attributes: None,
                    ingest_run_id,
                })
                .collect();

            let count = ObservationRepository::new(db).bulk_create(observations).await?;
            debug!(row = row_num, inserted = count, "row complete");
            *total += count;

            if *row_num % 1000 == 0 {
                info!(
                    source = source_name,
                    file = file_path,
                    rows = row_num,
                    observations = total,
                    "ingest progress"
                );
            }
        }

        Ok(())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Parse CSV row into canonical FieldValue map: trim all strings, empty → Null.
/// Header keys are lowercased so matching against `sources.toml` source values is case-insensitive.
fn row_to_canonical(headers: &[String], record: &csv::StringRecord) -> HashMap<String, FieldValue> {
    headers
        .iter()
        .zip(record.iter())
        .map(|(header, raw)| {
            let trimmed = raw.trim();
            let val = if trimmed.is_empty() {
                FieldValue::Null
            } else {
                FieldValue::Str(trimmed.to_string())
            };
            (header.to_lowercase(), val)
        })
        .collect()
}

/// Serialize the canonical FieldValue map to JSON for raw_imports storage.
fn canonical_to_json(map: &HashMap<String, FieldValue>) -> serde_json::Value {
    let obj: serde_json::Map<String, serde_json::Value> = map
        .iter()
        .map(|(k, v)| {
            let json_val = match v {
                FieldValue::Null        => serde_json::Value::Null,
                FieldValue::Str(s)      => serde_json::Value::String(s.clone()),
                FieldValue::I8(x)       => serde_json::json!(*x),
                FieldValue::I16(x)      => serde_json::json!(*x),
                FieldValue::I32(x)      => serde_json::json!(*x),
                FieldValue::I64(x)      => serde_json::json!(*x),
                FieldValue::U8(x)       => serde_json::json!(*x),
                FieldValue::U16(x)      => serde_json::json!(*x),
                FieldValue::U32(x)      => serde_json::json!(*x),
                FieldValue::U64(x)      => serde_json::json!(*x),
                FieldValue::F32(x)      => serde_json::json!(*x),
                FieldValue::F64(x)      => serde_json::json!(*x),
                FieldValue::Bool(x)     => serde_json::json!(*x),
                FieldValue::Date(x)     => serde_json::Value::String(x.to_string()),
                FieldValue::DateTime(x) => serde_json::Value::String(x.to_rfc3339()),
            };
            (k.clone(), json_val)
        })
        .collect();
    serde_json::Value::Object(obj)
}

fn str_from_field(map: &HashMap<String, FieldValue>, key: &str) -> Option<String> {
    match map.get(key)? {
        FieldValue::Str(s) => Some(s.clone()),
        _                  => None,
    }
}

fn f64_from_field(map: &HashMap<String, FieldValue>, key: &str) -> Option<f64> {
    match map.get(key)? {
        FieldValue::F64(v) => Some(*v),
        FieldValue::F32(v) => Some(*v as f64),
        FieldValue::Str(s) => s.parse().ok(),
        _                  => None,
    }
}

fn i16_from_field(map: &HashMap<String, FieldValue>, key: &str) -> Option<i16> {
    match map.get(key)? {
        FieldValue::I8(v)  => Some(*v as i16),
        FieldValue::I16(v) => Some(*v),
        FieldValue::I32(v) => i16::try_from(*v).ok(),
        FieldValue::I64(v) => i16::try_from(*v).ok(),
        FieldValue::U8(v)  => Some(*v as i16),
        FieldValue::U16(v) => i16::try_from(*v).ok(),
        FieldValue::Str(s) => s.parse().ok(),
        _                  => None,
    }
}

fn resolve_location(
    map: &HashMap<String, FieldValue>,
    default_country: Option<&str>,
) -> core::result::Result<NewLocation, ()> {
    let loc = NewLocation {
        county:    str_from_field(map, "county"),
        country:   str_from_field(map, "country").or_else(|| default_country.map(str::to_string)),
        zip_code:  str_from_field(map, "zip_code"),
        fips_code: str_from_field(map, "fips_code"),
        latitude:  f64_from_field(map, "latitude"),
        longitude: f64_from_field(map, "longitude"),
    };
    if loc.is_empty() { Err(()) } else { Ok(loc) }
}

fn resolve_time(map: &HashMap<String, FieldValue>) -> core::result::Result<NewTimePeriod, ()> {
    let year = i16_from_field(map, "year").ok_or(())?;
    Ok(NewTimePeriod {
        year,
        // Clamp to DB check-constraint ranges; out-of-range values become NULL.
        quarter: i16_from_field(map, "quarter").filter(|&q| (1..=4).contains(&q)),
        month:   i16_from_field(map, "month")  .filter(|&m| (1..=12).contains(&m)),
        day:     i16_from_field(map, "day")    .filter(|&d| (1..=31).contains(&d)),
    })
}

fn extract_metrics(map: &HashMap<String, FieldValue>) -> Vec<(String, Option<f64>)> {
    let skip: std::collections::HashSet<&str> = LOCATION_FIELDS
        .iter()
        .chain(TIME_FIELDS.iter())
        .copied()
        .collect();

    map.iter()
        .filter(|(k, _)| !skip.contains(k.as_str()))
        .map(|(k, v)| {
            let num = match v {
                FieldValue::F64(x) => Some(*x),
                FieldValue::F32(x) => Some(*x as f64),
                FieldValue::I8(x)  => Some(*x as f64),
                FieldValue::I16(x) => Some(*x as f64),
                FieldValue::I32(x) => Some(*x as f64),
                FieldValue::I64(x) => Some(*x as f64),
                FieldValue::U8(x)  => Some(*x as f64),
                FieldValue::U16(x) => Some(*x as f64),
                FieldValue::U32(x) => Some(*x as f64),
                FieldValue::U64(x) => Some(*x as f64),
                FieldValue::Str(s) => s.parse().ok(),
                _                  => None,
            };
            (k.clone(), num)
        })
        .collect()
}

fn infer_country_from_path(file_path: &str) -> Option<String> {
    if file_path.to_lowercase().split('/').any(|seg| seg == "usa") {
        Some("USA".to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::FieldValue;

    #[test]
    fn row_to_canonical_trims_strings() {
        let headers = vec!["col".to_string()];
        let record = csv::StringRecord::from(vec!["  hello  "]);
        let result = row_to_canonical(&headers, &record);
        assert!(matches!(result.get("col").unwrap(), FieldValue::Str(s) if s == "hello"));
    }

    #[test]
    fn row_to_canonical_whitespace_only_becomes_null() {
        let headers = vec!["col".to_string()];
        let record = csv::StringRecord::from(vec!["   "]);
        let result = row_to_canonical(&headers, &record);
        assert!(matches!(result.get("col").unwrap(), FieldValue::Null));
    }

    #[test]
    fn row_to_canonical_empty_string_becomes_null() {
        let headers = vec!["col".to_string()];
        let record = csv::StringRecord::from(vec![""]);
        let result = row_to_canonical(&headers, &record);
        assert!(matches!(result.get("col").unwrap(), FieldValue::Null));
    }

    #[test]
    fn row_to_canonical_lowercases_headers() {
        let headers = vec!["State".to_string(), "YEAR".to_string(), "My_Value".to_string()];
        let record = csv::StringRecord::from(vec!["Colorado", "2024", "42"]);
        let result = row_to_canonical(&headers, &record);
        assert!(result.contains_key("state"));
        assert!(result.contains_key("year"));
        assert!(result.contains_key("my_value"));
        assert!(!result.contains_key("State"));
        assert!(!result.contains_key("YEAR"));
    }

    #[test]
    fn infer_country_still_works_after_refactor() {
        // Smoke-check that the helper is intact after the pipeline changes.
        assert_eq!(
            infer_country_from_path("data/usa/co/file.csv"),
            Some("USA".to_string())
        );
        assert_eq!(infer_country_from_path("data/co/file.csv"), None);
    }
}
