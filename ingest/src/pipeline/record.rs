use std::collections::HashMap;

use state_search_core::{
    config::OnFailure,
    Db,
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::transforms::{chain::apply_chain, resolve::ResolvedFieldMap, FieldValue};
use super::{
    IngestError,
    context::extract_context,
    dimensions::{resolve_location_id, resolve_time_id, LocationCache},
    observations::{build_observations, extract_metrics, NewObservation},
    row::{canonical_to_json, row_to_canonical},
};

/// Outcome of applying all field transforms to a single CSV row.
pub(super) enum RowOutcome {
    /// Transforms succeeded — carry forward the result map.
    Proceed(HashMap<String, FieldValue>),
    /// A field failed with SkipRow — discard this row and continue.
    Skip,
    /// A field failed with SkipDataset — abort the entire file.
    Abort(anyhow::Error),
}

/// Process a single CSV record: store raw import, apply transforms, resolve dimensions.
/// Returns the observations to insert — callers batch these before writing to the DB.
pub(super) async fn process_record(
    record: csv::StringRecord,
    row_num: u64,
    source_name: &str,
    file_path: &str,
    ingest_run_id: Uuid,
    resolved: &ResolvedFieldMap,
    headers: &[String],
    default_country: Option<&str>,
    db: &Db,
    location_cache: &LocationCache,
) -> anyhow::Result<Vec<NewObservation>> {
    let raw = row_to_canonical(headers, &record);
    debug!(row = row_num, "parsed and trimmed CSV row");

    sqlx::query(
        "INSERT INTO raw_imports (source_id, source_file, raw_data) VALUES ($1, $2, $3)",
    )
    .bind(Option::<i64>::None)
    .bind(file_path)
    .bind(&canonical_to_json(&raw))
    .execute(db)
    .await?;

    let transformed = match apply_all_transforms(&raw, resolved, row_num, source_name, file_path) {
        RowOutcome::Proceed(t) => with_passthrough(&raw, resolved, t),
        RowOutcome::Skip       => return Ok(vec![]),
        RowOutcome::Abort(e)   => return Err(e),
    };

    let context_attrs = extract_context(&transformed);

    let (location_id, time_id, context_id) = tokio::join!(
        resolve_location_id(&transformed, default_country, db, row_num, location_cache),
        resolve_time_id(&transformed, db, row_num),
        insert_context(source_name, context_attrs, db, row_num),
    );

    let metrics = extract_metrics(&transformed);
    if metrics.is_empty() {
        warn!(row = row_num, "no metric fields found");
    }

    let obs = build_observations(metrics, location_id, time_id, context_id, source_name, ingest_run_id);
    debug!(row = row_num, produced = obs.len(), "row ready");
    Ok(obs)
}

// ── Context helpers ────────────────────────────────────────────────────────────

/// Stub — row context insertion removed in pipeline rewrite (Task 10).
async fn insert_context(
    _source_name: &str,
    _attributes: serde_json::Value,
    _db: &Db,
    _row_num: u64,
) -> Option<i64> {
    None
}

// ── Transform helpers ─────────────────────────────────────────────────────────

/// Apply all resolved field transforms to a raw row using `fold`.
///
/// Returns:
/// - `Proceed(map)` — all transforms succeeded
/// - `Skip`         — a field returned `SkipRow`
/// - `Abort(err)`   — a field returned `SkipDataset`
///
/// Once a terminal outcome (`Skip` or `Abort`) is reached the fold short-circuits
/// by passing it through unchanged for all remaining fields.
fn apply_all_transforms(
    raw: &HashMap<String, FieldValue>,
    resolved: &ResolvedFieldMap,
    row_num: u64,
    source_name: &str,
    file_path: &str,
) -> RowOutcome {
    resolved
        .iter()
        .fold(RowOutcome::Proceed(HashMap::new()), |outcome, (canonical, resolved_field)| {
            let RowOutcome::Proceed(mut acc) = outcome else {
                return outcome;
            };

            let raw_val = raw
                .get(resolved_field.source_col.as_str())
                .cloned()
                .unwrap_or(FieldValue::Null);

            match apply_chain(raw_val, &resolved_field.chain) {
                Ok(v) => {
                    acc.insert(canonical.clone(), v);
                    RowOutcome::Proceed(acc)
                }
                Err(OnFailure::SkipRow) => {
                    warn!(
                        row = row_num, field = canonical, source = source_name,
                        "SkipRow: discarding row"
                    );
                    RowOutcome::Skip
                }
                Err(OnFailure::SkipDataset) => RowOutcome::Abort(
                    IngestError::DatasetSkipped {
                        file: file_path.to_string(),
                        reason: format!(
                            "transform failure on field '{}' at row {}",
                            canonical, row_num
                        ),
                    }
                    .into(),
                ),
                Err(OnFailure::Ignore) => unreachable!(),
            }
        })
}

/// Extend `transformed` with any raw columns not covered by the resolved field map.
fn with_passthrough(
    raw: &HashMap<String, FieldValue>,
    resolved: &ResolvedFieldMap,
    mut transformed: HashMap<String, FieldValue>,
) -> HashMap<String, FieldValue> {
    transformed.extend(
        raw.iter()
            .filter(|(col, _)| !resolved.values().any(|r| &r.source_col == *col))
            .map(|(col, val)| (col.clone(), val.clone())),
    );
    transformed
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::FieldValue;

    #[test]
    fn apply_all_transforms_proceed_on_empty_resolved() {
        let raw = HashMap::new();
        let resolved = ResolvedFieldMap::new();
        match apply_all_transforms(&raw, &resolved, 1, "src", "f.csv") {
            RowOutcome::Proceed(m) => assert!(m.is_empty()),
            _ => panic!("expected Proceed"),
        }
    }

    #[test]
    fn with_passthrough_adds_unmapped_columns() {
        let mut raw = HashMap::new();
        raw.insert("unmapped".to_string(), FieldValue::Str("v".to_string()));
        let resolved = ResolvedFieldMap::new();
        let result = with_passthrough(&raw, &resolved, HashMap::new());
        assert!(result.contains_key("unmapped"));
    }
}
