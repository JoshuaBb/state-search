use std::collections::HashMap;

use crate::transforms::FieldValue;

const LOCATION_FIELDS: &[&str] = &[
    "county", "country", "zip_code", "fips_code", "latitude", "longitude",
    "state_code", "state_name",
];
const TIME_FIELDS: &[&str] = &["year", "quarter", "month", "day"];

/// Collect all non-numeric, non-dimension fields from the transformed row into
/// a JSONB-compatible `serde_json::Value` for `fact_row_context.attributes`.
///
/// Numeric fields (any `FieldValue` integer or float variant) and recognised
/// dimension fields are excluded. Only string values are collected.
pub(super) fn extract_context(map: &HashMap<String, FieldValue>) -> serde_json::Value {
    let skip: std::collections::HashSet<&str> = LOCATION_FIELDS
        .iter()
        .chain(TIME_FIELDS.iter())
        .copied()
        .collect();

    let attrs: serde_json::Map<String, serde_json::Value> = map
        .iter()
        .filter(|(k, _)| !skip.contains(k.as_str()))
        .filter_map(|(k, v)| match v {
            FieldValue::Str(s) => Some((k.clone(), serde_json::Value::String(s.clone()))),
            _ => None,
        })
        .collect();

    serde_json::Value::Object(attrs)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_context_collects_string_fields() {
        let mut map = HashMap::new();
        map.insert("analyte_name".to_string(), FieldValue::Str("TTHM".to_string()));
        map.insert("units".to_string(),        FieldValue::Str("ug/L".to_string()));
        map.insert("average_concentration".to_string(), FieldValue::F64(3.8));
        map.insert("county".to_string(),       FieldValue::Str("Denver".to_string()));
        map.insert("year".to_string(),         FieldValue::I32(2022));

        let ctx = extract_context(&map);
        let obj = ctx.as_object().unwrap();

        // String non-dimension fields captured
        assert_eq!(obj.get("analyte_name").and_then(|v| v.as_str()), Some("TTHM"));
        assert_eq!(obj.get("units").and_then(|v| v.as_str()), Some("ug/L"));

        // Numeric field excluded
        assert!(!obj.contains_key("average_concentration"));

        // Dimension fields excluded
        assert!(!obj.contains_key("county"));
        assert!(!obj.contains_key("year"));
    }

    #[test]
    fn extract_context_returns_empty_object_when_no_string_fields() {
        let mut map = HashMap::new();
        map.insert("average_concentration".to_string(), FieldValue::F64(3.8));
        map.insert("year".to_string(), FieldValue::I32(2022));

        let ctx = extract_context(&map);
        assert!(ctx.as_object().unwrap().is_empty());
    }
}
