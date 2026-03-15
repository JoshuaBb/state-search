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

use crate::transforms::{
    chain::apply_chain,
    resolve::build_resolved_field_map,
    FieldValue,
};

const LOCATION_FIELDS: &[&str] = &[
    "state_code", "state_name", "country", "zip_code", "fips_code", "latitude", "longitude",
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

    pub async fn run(&self, source: &SourceConfig, file_path: &str) -> anyhow::Result<u64> {
        let source_name = source.name.as_str();
        info!(source = source_name, file = file_path, "starting ingest");

        // Resolve field map once — hard errors here are config problems
        let resolved = build_resolved_field_map(source)
            .with_context(|| format!("failed to resolve field map for source '{source_name}'"))?;

        let mut reader = csv::Reader::from_path(file_path)
            .with_context(|| format!("cannot open {file_path}"))?;

        let headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_string()).collect();
        debug!(source = source_name, ?headers, "CSV headers detected");

        let default_country = infer_country_from_path(file_path);

        // Skip-check runs BEFORE the transaction (outside BEGIN)
        let already_ingested: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM raw_imports WHERE source_file = $1)",
        )
        .bind(file_path)
        .fetch_one(self.db)
        .await?;

        if already_ingested {
            info!(source = source_name, file = file_path, "skipping (already ingested)");
            return Ok(0);
        }

        // Open transaction for the entire file
        let mut tx = self.db.begin().await?;
        let mut total = 0u64;
        let mut row_num = 0u64;

        let result: anyhow::Result<u64> = async {
            for record in reader.records() {
                let record = record?;
                row_num += 1;

                // 1. Build raw map: trim all strings, empty → Null
                let raw = row_to_canonical(&headers, &record);
                debug!(row = row_num, "parsed and trimmed CSV row");

                // 2. Store raw row inside transaction
                let raw_json = canonical_to_json(&raw);
                let raw_id: i64 = sqlx::query_scalar(
                    "INSERT INTO raw_imports (source_id, source_file, raw_data) VALUES ($1, $2, $3) RETURNING id",
                )
                .bind(Option::<i64>::None)
                .bind(file_path)
                .bind(&raw_json)
                .fetch_one(&mut *tx)
                .await?;

                // 3. Apply field transforms per resolved field
                let mut transformed: HashMap<String, FieldValue> = HashMap::new();
                let mut skip_row = false;

                for (canonical, resolved_field) in &resolved {
                    let raw_val = raw
                        .get(resolved_field.source_col.as_str())
                        .map(field_value_clone)
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
                            }
                            .into());
                        }
                        // Ignore is handled inside apply_chain (returns Ok(Null)) — this arm
                        // is unreachable but kept for exhaustive matching.
                        Err(OnFailure::Ignore) => unreachable!(),
                    }
                }

                // Check SkipRow BEFORE unmapped-column passthrough to avoid unnecessary work.
                if skip_row {
                    continue;
                }

                // Pass through unmapped columns (preserve existing metric-extraction behavior)
                for (col, val) in &raw {
                    if !resolved.values().any(|r| r.source_col == *col) {
                        transformed.insert(col.clone(), field_value_clone(val));
                    }
                }

                // 4. Resolve dimensions — all DB calls use &mut tx to stay in the file transaction
                let location_id = match resolve_location(&transformed, default_country.as_deref()) {
                    Ok(loc) => {
                        match LocationRepository::upsert_with_tx(&mut tx, loc).await {
                            Ok(id) => { debug!(row = row_num, location_id = id, "resolved location"); Some(id) }
                            Err(e) => { warn!(row = row_num, error = %e, "could not resolve location"); None }
                        }
                    }
                    Err(_) => None,
                };

                let time_id = match resolve_time(&transformed) {
                    Ok(t) => {
                        match TimeRepository::upsert_with_tx(&mut tx, t).await {
                            Ok(id) => { debug!(row = row_num, time_id = id, "resolved time"); Some(id) }
                            Err(e) => { warn!(row = row_num, error = %e, "could not resolve time"); None }
                        }
                    }
                    Err(_) => None,
                };

                // 5. Extract metrics
                let metrics = extract_metrics(&transformed);
                if metrics.is_empty() {
                    warn!(row = row_num, "no metric fields found");
                }

                let observations: Vec<NewObservation> = metrics
                    .into_iter()
                    .map(|(name, value)| NewObservation {
                        raw_import_id: Some(raw_id),
                        location_id,
                        time_id,
                        source_name: Some(source_name.to_string()),
                        metric_name: name,
                        metric_value: value,
                        attributes: None,
                    })
                    .collect();

                // Use bulk_create_with_tx so observations are part of the file transaction.
                let count = ObservationRepository::bulk_create_with_tx(&mut tx, observations).await?;
                debug!(row = row_num, inserted = count, "row complete");
                total += count;
            }
            Ok(total)
        }
        .await;

        match result {
            Ok(n) => {
                tx.commit().await?;
                info!(source = source_name, file = file_path, observations = n, "ingest complete");
                Ok(n)
            }
            Err(e) => {
                tx.rollback().await?;
                Err(e)
            }
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Parse CSV row into canonical FieldValue map: trim all strings, empty → Null.
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
            (header.clone(), val)
        })
        .collect()
}

/// Shallow clone of a FieldValue for passing through unmapped columns.
fn field_value_clone(v: &FieldValue) -> FieldValue {
    match v {
        FieldValue::Null        => FieldValue::Null,
        FieldValue::Str(s)      => FieldValue::Str(s.clone()),
        FieldValue::I8(x)       => FieldValue::I8(*x),
        FieldValue::I16(x)      => FieldValue::I16(*x),
        FieldValue::I32(x)      => FieldValue::I32(*x),
        FieldValue::I64(x)      => FieldValue::I64(*x),
        FieldValue::U8(x)       => FieldValue::U8(*x),
        FieldValue::U16(x)      => FieldValue::U16(*x),
        FieldValue::U32(x)      => FieldValue::U32(*x),
        FieldValue::U64(x)      => FieldValue::U64(*x),
        FieldValue::F32(x)      => FieldValue::F32(*x),
        FieldValue::F64(x)      => FieldValue::F64(*x),
        FieldValue::Bool(x)     => FieldValue::Bool(*x),
        FieldValue::Date(x)     => FieldValue::Date(*x),
        FieldValue::DateTime(x) => FieldValue::DateTime(*x),
    }
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
        state_code: str_from_field(map, "state_code"),
        state_name: str_from_field(map, "state_name"),
        country:    str_from_field(map, "country").or_else(|| default_country.map(str::to_string)),
        zip_code:   str_from_field(map, "zip_code"),
        fips_code:  str_from_field(map, "fips_code"),
        latitude:   f64_from_field(map, "latitude"),
        longitude:  f64_from_field(map, "longitude"),
    };
    if loc.is_empty() { Err(()) } else { Ok(loc) }
}

fn resolve_time(map: &HashMap<String, FieldValue>) -> core::result::Result<NewTimePeriod, ()> {
    let year = i16_from_field(map, "year").ok_or(())?;
    Ok(NewTimePeriod {
        year,
        quarter: i16_from_field(map, "quarter"),
        month:   i16_from_field(map, "month"),
        day:     i16_from_field(map, "day"),
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
}
