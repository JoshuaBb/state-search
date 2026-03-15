use std::collections::HashMap;

use uuid::Uuid;

/// Temporary stub — NewObservation removed in pipeline rewrite (Task 10).
#[derive(Debug)]
pub(super) struct NewObservation {
    pub raw_import_id: Option<i64>,
    pub location_id: Option<i64>,
    pub time_id: Option<i64>,
    pub source_name: Option<String>,
    pub metric_name: String,
    pub metric_value: Option<f64>,
    pub attributes: Option<serde_json::Value>,
    pub context_id: Option<i64>,
    pub ingest_run_id: Uuid,
}

use crate::transforms::FieldValue;

const LOCATION_FIELDS: &[&str] = &[
    "county", "country", "zip_code", "fips_code", "latitude", "longitude",
];
const TIME_FIELDS: &[&str] = &["year", "quarter", "month", "day"];

/// Extract non-dimension fields that carry a parseable numeric value.
///
/// String-valued columns (e.g. analyte name, units, IDs) and non-numeric sentinel
/// values (e.g. "NA", "N/A", "-") are silently skipped — an observation with a null
/// metric_value has no information to contribute to the fact table.
pub(super) fn extract_metrics(map: &HashMap<String, FieldValue>) -> Vec<(String, f64)> {
    let skip: std::collections::HashSet<&str> = LOCATION_FIELDS
        .iter()
        .chain(TIME_FIELDS.iter())
        .copied()
        .collect();

    map.iter()
        .filter(|(k, _)| !skip.contains(k.as_str()))
        .filter_map(|(k, v)| {
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
                FieldValue::Str(s) => parse_numeric(s),
                _                  => None,
            };
            num.map(|n| (k.clone(), n))
        })
        .collect()
}

/// Parse a numeric string, stripping common formatting characters.
/// Handles values like "1,234", "$1,234.56", "12.5%".
/// Returns `None` for non-numeric sentinels like "NA", "N/A", "-", "ND".
fn parse_numeric(s: &str) -> Option<f64> {
    let cleaned: String = s.chars().filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-').collect();
    cleaned.parse().ok()
}

/// Build `NewObservation` values from extracted metrics.
pub(super) fn build_observations(
    metrics: Vec<(String, f64)>,
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
            metric_value: Some(value),
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
            vec![("metric_a".to_string(), 1.0), ("metric_b".to_string(), 2.5)],
            Some(10),
            Some(20),
            Some(99),
            "src",
            id,
        );
        assert_eq!(obs.len(), 2);
        assert!(obs.iter().all(|o| o.ingest_run_id == id));
        assert!(obs.iter().all(|o| o.location_id == Some(10)));
        assert!(obs.iter().all(|o| o.metric_value.is_some()));
    }

    #[test]
    fn extract_metrics_skips_non_numeric_strings() {
        let mut map = HashMap::new();
        map.insert("analyte_name".to_string(), FieldValue::Str("TTHM".to_string()));
        map.insert("units".to_string(),        FieldValue::Str("ug/L".to_string()));
        map.insert("average_concentration".to_string(), FieldValue::Str("3.8".to_string()));
        map.insert("maximum_concentration".to_string(), FieldValue::Str("NA".to_string()));
        let metrics = extract_metrics(&map);
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].0, "average_concentration");
        assert!((metrics[0].1 - 3.8).abs() < f64::EPSILON);
    }

    #[test]
    fn parse_numeric_strips_formatting() {
        assert_eq!(parse_numeric("1,234"),    Some(1234.0));
        assert_eq!(parse_numeric("1,234.56"), Some(1234.56));
        assert_eq!(parse_numeric("12.5%"),    Some(12.5));
        assert_eq!(parse_numeric("$1,234"),   Some(1234.0));
        assert_eq!(parse_numeric("NA"),       None);
        assert_eq!(parse_numeric("N/A"),      None);
        assert_eq!(parse_numeric("-"),        None);
        assert_eq!(parse_numeric("ND"),       None);
    }
}
