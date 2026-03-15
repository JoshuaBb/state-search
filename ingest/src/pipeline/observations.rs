use std::collections::HashMap;

use state_search_core::models::observation::NewObservation;
use uuid::Uuid;

use crate::transforms::FieldValue;

const LOCATION_FIELDS: &[&str] = &[
    "county", "country", "zip_code", "fips_code", "latitude", "longitude",
];
const TIME_FIELDS: &[&str] = &["year", "quarter", "month", "day"];

/// Extract all non-dimension fields as (metric_name, numeric_value) pairs.
pub(super) fn extract_metrics(map: &HashMap<String, FieldValue>) -> Vec<(String, Option<f64>)> {
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

/// Build `NewObservation` values from extracted metrics.
pub(super) fn build_observations(
    metrics: Vec<(String, Option<f64>)>,
    location_id: Option<i64>,
    time_id: Option<i64>,
    context_id: Option<i64>,
    source_name: &str,
    ingest_run_id: Uuid,
) -> Vec<NewObservation> {
    metrics
        .into_iter()
        .map(|(name, value)| NewObservation {
            raw_import_id: None,
            location_id,
            time_id,
            source_name: Some(source_name.to_string()),
            metric_name: name,
            metric_value: value,
            attributes: None,
            context_id,
            ingest_run_id,
        })
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_observations_maps_metrics_to_new_observations() {
        let id = Uuid::new_v4();
        let obs = build_observations(
            vec![("metric_a".to_string(), Some(1.0)), ("metric_b".to_string(), None)],
            Some(10),
            Some(20),
            Some(99),
            "src",
            id,
        );
        assert_eq!(obs.len(), 2);
        assert!(obs.iter().all(|o| o.ingest_run_id == id));
        assert!(obs.iter().all(|o| o.location_id == Some(10)));
    }
}
