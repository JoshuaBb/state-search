use std::collections::HashSet;

use anyhow::Context;
use futures::{stream, StreamExt};
use state_search_core::{
    config::{DimTarget, SourceConfig},
    models::{
        dim_file_log::NewDimFileLog,
        location::NewLocation,
        time::NewTimePeriod,
    },
    repositories::{
        dim_file_log::DimFileLogRepository,
        location::LocationRepository,
        time::TimeRepository,
    },
    Db,
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::transforms::{chain::apply_chain, resolve::build_resolved_field_map, FieldValue};
use super::{
    row::{row_to_canonical, strip_excluded_columns},
    uuid::derive_uuid,
    ROW_CONCURRENCY, OBS_BATCH_SIZE,
};

// ── Startup validation ────────────────────────────────────────────────────────

/// Validate that every entry in `exclude_columns` exists in the CSV headers
/// (case-insensitive). Returns Err with a descriptive message on failure.
pub(super) fn validate_exclude_columns(
    source_name: &str,
    exclude_columns: &[String],
    headers: &[String],
) -> anyhow::Result<()> {
    let header_set: HashSet<String> = headers.iter().map(|h| h.to_lowercase()).collect();
    for col in exclude_columns {
        if !header_set.contains(&col.to_lowercase()) {
            anyhow::bail!(
                "exclude_columns: '{}' not found in CSV headers for source '{}'",
                col, source_name
            );
        }
    }
    Ok(())
}

/// `location_id` and `time_id` are always valid unique_key entries — they are FK
/// columns resolved from dim tables during ingestion and injected into the key map.
const FK_KEY_COLUMNS: &[&str] = &["location_id", "time_id"];

/// Validate that every entry in `unique_key` is either a canonical name declared in
/// `fields` or `derived`, or one of the FK key columns (`location_id`, `time_id`).
/// Returns Err on the first invalid entry.
pub(super) fn validate_unique_key(source: &SourceConfig) -> anyhow::Result<()> {
    let canonical_names: HashSet<&str> = source.fields.values()
        .map(|f| f.canonical.as_str())
        .chain(source.derived.keys().map(String::as_str))
        .chain(FK_KEY_COLUMNS.iter().copied())
        .collect();
    for col in &source.unique_key {
        if !canonical_names.contains(col.as_str()) {
            anyhow::bail!(
                "unique_key: '{}' is not a declared canonical field in source '{}'",
                col, source.name
            );
        }
    }
    Ok(())
}

// ── Dim-only ingest run ───────────────────────────────────────────────────────

pub async fn run_dim(source: &SourceConfig, file_path: &str, db: &Db) -> anyhow::Result<u64> {
    let target = source.target.expect("run_dim called without a target");
    let source_name = source.name.as_str();

    if source.unique_key.is_empty() {
        warn!(source = source_name, "unique_key is empty — dim rows will get random UUIDs; re-runs will not be idempotent");
    }

    // Check for prior ingest of this file
    if DimFileLogRepository::exists_for_file(db, source_name, file_path).await? {
        warn!(source = source_name, file = file_path, "file has been previously ingested into {}; re-ingesting (upserts are idempotent)", target_str(target));
    }

    let resolved = build_resolved_field_map(source)
        .with_context(|| format!("failed to resolve field map for source '{source_name}'"))?;

    let mut reader = csv::Reader::from_path(file_path)
        .with_context(|| format!("cannot open {file_path}"))?;

    let headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_string()).collect();

    // Startup validations
    validate_exclude_columns(source_name, &source.exclude_columns, &headers)?;
    validate_unique_key(source)?;

    let excluded: HashSet<String> = source.exclude_columns.iter()
        .map(|c| c.to_lowercase())
        .collect();

    let total = match target {
        DimTarget::DimLocation => run_dim_location(source, file_path, &resolved, &headers, &excluded, &mut reader, db).await?,
        DimTarget::DimTime     => run_dim_time(source, file_path, &resolved, &headers, &excluded, &mut reader, db).await?,
    };

    // Record successful run
    DimFileLogRepository::insert(db, &NewDimFileLog {
        source_name: source_name.to_string(),
        target:      target_str(target).to_string(),
        file_path:   file_path.to_string(),
        row_count:   total as i64,
    }).await?;

    info!(source = source_name, file = file_path, target = target_str(target), rows = total, "dim ingest complete");
    Ok(total)
}

async fn run_dim_location(
    source: &SourceConfig,
    file_path: &str,
    resolved: &crate::transforms::resolve::ResolvedFieldMap,
    headers: &[String],
    excluded: &HashSet<String>,
    reader: &mut csv::Reader<std::fs::File>,
    db: &Db,
) -> anyhow::Result<u64> {
    let source_name = source.name.as_str();
    let key_cols: Vec<String> = source.unique_key.clone();

    let work = stream::iter(reader.records().enumerate()).map(|(i, result): (usize, _)| {
        let rn = i as u64 + 1;
        let key_cols = key_cols.clone();
        async move {
            let record = result.map_err(anyhow::Error::from)?;
            let (h, r) = if excluded.is_empty() {
                (headers.to_vec(), record.clone())
            } else {
                strip_excluded_columns(headers, &record, excluded)
            };
            let raw = row_to_canonical(&h, &r);
            let transformed = match apply_transforms_dim(&raw, resolved, rn, source_name)? {
                Some(t) => t,
                None    => return Ok(vec![]),
            };

            let id = if key_cols.is_empty() {
                Uuid::new_v4()
            } else {
                let cols: Vec<&str> = key_cols.iter().map(String::as_str).collect();
                derive_uuid(&cols, &transformed)
            };

            let loc = NewLocation {
                id,
                county:           field_as_str(&transformed, "county"),
                country:          field_as_str(&transformed, "country"),
                zip_code:         field_as_str(&transformed, "zip_code"),
                fips_code:        field_as_str(&transformed, "fips_code"),
                latitude:         field_as_f64(&transformed, "latitude"),
                longitude:        field_as_f64(&transformed, "longitude"),
                city:             field_as_str(&transformed, "city"),
                state_code:       field_as_str(&transformed, "state_code"),
                state_name:       field_as_str(&transformed, "state_name"),
                zcta:             field_as_str(&transformed, "zcta"),
                parent_zcta:      field_as_str(&transformed, "parent_zcta"),
                population:       field_as_f64(&transformed, "population"),
                density:          field_as_f64(&transformed, "density"),
                county_weights:   field_as_json(&transformed, "county_weights"),
                county_names_all: field_as_str(&transformed, "county_names_all"),
                county_fips_all:  field_as_str(&transformed, "county_fips_all"),
                imprecise:        field_as_bool(&transformed, "imprecise"),
                military:         field_as_bool(&transformed, "military"),
                timezone:         field_as_str(&transformed, "timezone"),
            };
            Ok::<Vec<NewLocation>, anyhow::Error>(vec![loc])
        }
    });

    crate::stream::batched_channel_sink(
        work,
        ROW_CONCURRENCY,
        ROW_CONCURRENCY * 4,
        OBS_BATCH_SIZE,
        |batch: Vec<NewLocation>| async move {
            bulk_upsert_locations(batch, db).await.map_err(Into::into)
        },
        |total| info!(source = source_name, file = file_path, rows = total, "dim_location batch flushed"),
    )
    .await
}

async fn run_dim_time(
    source: &SourceConfig,
    file_path: &str,
    resolved: &crate::transforms::resolve::ResolvedFieldMap,
    headers: &[String],
    excluded: &HashSet<String>,
    reader: &mut csv::Reader<std::fs::File>,
    db: &Db,
) -> anyhow::Result<u64> {
    let source_name = source.name.as_str();
    let key_cols: Vec<String> = source.unique_key.clone();

    let work = stream::iter(reader.records().enumerate()).map(|(i, result): (usize, _)| {
        let rn = i as u64 + 1;
        let key_cols = key_cols.clone();
        async move {
            let record = result.map_err(anyhow::Error::from)?;
            let (h, r) = if excluded.is_empty() {
                (headers.to_vec(), record.clone())
            } else {
                strip_excluded_columns(headers, &record, excluded)
            };
            let raw = row_to_canonical(&h, &r);
            let transformed = match apply_transforms_dim(&raw, resolved, rn, source_name)? {
                Some(t) => t,
                None    => return Ok(vec![]),
            };

            let year = super::row::i16_from_field(&transformed, "year")
                .ok_or_else(|| anyhow::anyhow!("row {rn}: 'year' field missing or invalid for dim_time"))?;

            let id = if key_cols.is_empty() {
                Uuid::new_v4()
            } else {
                let cols: Vec<&str> = key_cols.iter().map(String::as_str).collect();
                derive_uuid(&cols, &transformed)
            };

            let t = NewTimePeriod {
                id,
                year,
                quarter: super::row::i16_from_field(&transformed, "quarter").filter(|&q| (1..=4).contains(&q)),
                month:   super::row::i16_from_field(&transformed, "month")  .filter(|&m| (1..=12).contains(&m)),
                day:     super::row::i16_from_field(&transformed, "day")    .filter(|&d| (1..=31).contains(&d)),
            };
            Ok::<Vec<NewTimePeriod>, anyhow::Error>(vec![t])
        }
    });

    crate::stream::batched_channel_sink(
        work,
        ROW_CONCURRENCY,
        ROW_CONCURRENCY * 4,
        OBS_BATCH_SIZE,
        |batch: Vec<NewTimePeriod>| async move {
            bulk_upsert_times(batch, db).await.map_err(Into::into)
        },
        |total| info!(source = source_name, file = file_path, rows = total, "dim_time batch flushed"),
    )
    .await
}

// ── Flush helpers ─────────────────────────────────────────────────────────────

async fn bulk_upsert_locations(locs: Vec<NewLocation>, db: &Db) -> state_search_core::error::Result<u64> {
    let n = locs.len() as u64;
    for loc in locs {
        LocationRepository::new(db).upsert(loc).await?;
    }
    Ok(n)
}

async fn bulk_upsert_times(times: Vec<NewTimePeriod>, db: &Db) -> state_search_core::error::Result<u64> {
    let n = times.len() as u64;
    for t in times {
        TimeRepository::new(db).upsert(t).await?;
    }
    Ok(n)
}

// ── Row processing helpers ────────────────────────────────────────────────────

/// Returns `Ok(None)` for `SkipRow` (caller should return `Ok(vec![])` to skip the row),
/// `Err` for `SkipDataset` (aborts the whole file), `Ok(Some(_))` for success.
fn apply_transforms_dim(
    raw: &std::collections::HashMap<String, FieldValue>,
    resolved: &crate::transforms::resolve::ResolvedFieldMap,
    row_num: u64,
    source_name: &str,
) -> anyhow::Result<Option<std::collections::HashMap<String, FieldValue>>> {
    let mut out = std::collections::HashMap::new();
    for (canonical, rf) in resolved.iter() {
        let raw_val = raw.get(rf.source_col.as_str()).cloned().unwrap_or(FieldValue::Null);
        match apply_chain(raw_val, &rf.chain) {
            Ok(v) => { out.insert(canonical.clone(), v); }
            Err(state_search_core::config::OnFailure::SkipRow) => {
                warn!(row = row_num, field = canonical, source = source_name, "SkipRow in dim pipeline — discarding row");
                return Ok(None);
            }
            Err(state_search_core::config::OnFailure::SkipDataset) => {
                anyhow::bail!("row {row_num}: SkipDataset on field '{canonical}' in source '{source_name}'");
            }
            Err(state_search_core::config::OnFailure::Ignore) => unreachable!(),
        }
    }
    Ok(Some(out))
}

fn field_as_str(map: &std::collections::HashMap<String, FieldValue>, key: &str) -> Option<String> {
    match map.get(key)? {
        FieldValue::Str(s) => Some(s.clone()),
        _ => None,
    }
}

fn field_as_f64(map: &std::collections::HashMap<String, FieldValue>, key: &str) -> Option<f64> {
    super::row::f64_from_field(map, key)
}

fn field_as_bool(map: &std::collections::HashMap<String, FieldValue>, key: &str) -> Option<bool> {
    match map.get(key)? {
        FieldValue::Bool(b) => Some(*b),
        FieldValue::Str(s) => match s.to_lowercase().as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn field_as_json(
    map: &std::collections::HashMap<String, FieldValue>,
    key: &str,
) -> Option<serde_json::Value> {
    match map.get(key)? {
        FieldValue::Json(v)                  => Some(v.clone()),
        FieldValue::Str(s) if !s.is_empty()  => serde_json::from_str(s).ok(),
        _ => None,
    }
}

fn target_str(t: DimTarget) -> &'static str {
    match t {
        DimTarget::DimLocation => "dim_location",
        DimTarget::DimTime     => "dim_time",
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use state_search_core::config::{FieldDef, SourceConfig};
    use std::collections::HashMap;

    fn make_source(name: &str, unique_key: Vec<&str>, exclude_cols: Vec<&str>) -> SourceConfig {
        SourceConfig {
            name:            name.to_string(),
            description:     None,
            files:           vec![],
            fields:          HashMap::new(),
            derived:         HashMap::new(),
            target:          Some(DimTarget::DimLocation),
            unique_key:      unique_key.into_iter().map(str::to_string).collect(),
            exclude_columns: exclude_cols.into_iter().map(str::to_string).collect(),
        }
    }

    fn make_source_with_field(name: &str, canonical: &str) -> SourceConfig {
        let mut fields = HashMap::new();
        fields.insert("col".to_string(), FieldDef {
            canonical: canonical.to_string(),
            field_type: "text".to_string(),
            format: None,
            on_failure: None,
            rules: vec![],
        });
        SourceConfig {
            name:            name.to_string(),
            description:     None,
            files:           vec![],
            fields,
            derived:         HashMap::new(),
            target:          Some(DimTarget::DimLocation),
            unique_key:      vec![canonical.to_string()],
            exclude_columns: vec![],
        }
    }

    #[test]
    fn validate_exclude_columns_passes_when_present() {
        let headers = vec!["State".to_string(), "Year".to_string(), "Notes".to_string()];
        assert!(validate_exclude_columns("src", &["Notes".to_string()], &headers).is_ok());
    }

    #[test]
    fn validate_exclude_columns_case_insensitive() {
        let headers = vec!["State".to_string(), "NOTES".to_string()];
        assert!(validate_exclude_columns("src", &["notes".to_string()], &headers).is_ok());
    }

    #[test]
    fn validate_exclude_columns_fails_when_absent() {
        let headers = vec!["State".to_string()];
        let result = validate_exclude_columns("src", &["missing_col".to_string()], &headers);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing_col"));
    }

    #[test]
    fn validate_unique_key_passes_for_declared_canonical() {
        let source = make_source_with_field("src", "fips_code");
        assert!(validate_unique_key(&source).is_ok());
    }

    #[test]
    fn validate_unique_key_fails_for_undeclared_canonical() {
        let source = make_source("src", vec!["nonexistent_field"], vec![]);
        let result = validate_unique_key(&source);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("nonexistent_field"));
    }

    #[test]
    fn validate_unique_key_empty_passes() {
        let source = make_source("src", vec![], vec![]);
        assert!(validate_unique_key(&source).is_ok());
    }

    #[test]
    fn validate_unique_key_allows_fk_columns() {
        // location_id and time_id are always valid regardless of declared fields
        let source = make_source("src", vec!["location_id", "time_id"], vec![]);
        assert!(validate_unique_key(&source).is_ok());
    }
}
