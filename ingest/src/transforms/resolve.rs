use std::collections::HashMap;
use state_search_core::config::{FieldDef, OnFailure, SourceConfig};
use super::{FieldRule, coerce::{
    CoerceToBool, CoerceToDate, CoerceToDateTime, CoerceToF32, CoerceToF64,
    CoerceToI8, CoerceToI16, CoerceToI32, CoerceToI64,
    CoerceToU8, CoerceToU16, CoerceToU32, CoerceToU64,
    DateFormat, DateTimeFormat,
}, rules::state_name_to_code::StateNameToCode};

pub struct ResolvedField {
    pub source_col: String,
    pub chain: Vec<(Box<dyn FieldRule>, OnFailure)>,
    pub field_on_failure: OnFailure,
}

impl std::fmt::Debug for ResolvedField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedField")
            .field("source_col", &self.source_col)
            .field("chain_len", &self.chain.len())
            .field("field_on_failure", &self.field_on_failure)
            .finish()
    }
}

pub type ResolvedFieldMap = HashMap<String, ResolvedField>;

/// Map a postgres type string + optional format to the implicit coerce rule.
/// Returns None for "text" (no coercion needed).
fn field_type_str_to_coerce(
    field_type: &str,
    format: Option<&str>,
    field_on_failure: OnFailure,
) -> anyhow::Result<Option<(Box<dyn FieldRule>, OnFailure)>> {
    if let Some(fmt_str) = format {
        match field_type {
            "date" => {
                let fmt = DateFormat::from_str(fmt_str)
                    .ok_or_else(|| anyhow::anyhow!("unknown date format: '{}'", fmt_str))?;
                return Ok(Some((Box::new(CoerceToDate(fmt)), field_on_failure)));
            }
            "timestamptz" | "timestamp with time zone" => {
                let fmt = DateTimeFormat::from_str(fmt_str)
                    .ok_or_else(|| anyhow::anyhow!("unknown datetime format: '{}'", fmt_str))?;
                return Ok(Some((Box::new(CoerceToDateTime(fmt)), field_on_failure)));
            }
            _ => anyhow::bail!("'format' is only valid for date/timestamptz types, got '{}'", field_type),
        }
    }

    let coerce: Option<Box<dyn FieldRule>> = match field_type {
        "text" | "varchar" | "char"           => None,
        "smallint" | "int2"                   => Some(Box::new(CoerceToI16)),
        "integer"  | "int"  | "int4"          => Some(Box::new(CoerceToI32)),
        "bigint"   | "int8"                   => Some(Box::new(CoerceToI64)),
        "real"     | "float4"                 => Some(Box::new(CoerceToF32)),
        "float8"   | "double precision"       => Some(Box::new(CoerceToF64)),
        "numeric"  | "decimal"                => Some(Box::new(CoerceToF64)),
        "boolean"  | "bool"                   => Some(Box::new(CoerceToBool)),
        "date"                                => Some(Box::new(CoerceToDate(DateFormat::Iso8601))),
        "timestamptz" | "timestamp with time zone" => Some(Box::new(CoerceToDateTime(DateTimeFormat::Rfc3339))),
        // Legacy short names kept for compatibility
        "i8"  => Some(Box::new(CoerceToI8)),
        "i16" => Some(Box::new(CoerceToI16)),
        "i32" => Some(Box::new(CoerceToI32)),
        "i64" => Some(Box::new(CoerceToI64)),
        "u8"  => Some(Box::new(CoerceToU8)),
        "u16" => Some(Box::new(CoerceToU16)),
        "u32" => Some(Box::new(CoerceToU32)),
        "u64" => Some(Box::new(CoerceToU64)),
        "f32" => Some(Box::new(CoerceToF32)),
        "f64" => Some(Box::new(CoerceToF64)),
        other => anyhow::bail!("unknown field type: '{}'", other),
    };
    Ok(coerce.map(|r| (r, field_on_failure)))
}

fn lookup_rule(kind: &str) -> Option<Box<dyn FieldRule>> {
    match kind {
        "state_name_to_code" => Some(Box::new(StateNameToCode)),
        _                    => None,
    }
}

/// Resolve a single FieldDef into a ResolvedField.
/// `csv_col` is the source CSV column name (the YAML key in `fields`).
pub fn build_resolved_field(csv_col: &str, def: &FieldDef) -> anyhow::Result<ResolvedField> {
    let field_on_failure = def.on_failure.unwrap_or(OnFailure::Ignore);
    let mut chain: Vec<(Box<dyn FieldRule>, OnFailure)> = Vec::new();

    for rule_def in &def.rules {
        let rule = lookup_rule(&rule_def.kind)
            .ok_or_else(|| anyhow::anyhow!(
                "unknown rule kind: '{}' (in field canonical='{}')",
                rule_def.kind, def.canonical
            ))?;
        let rule_on_failure = rule_def.on_failure.unwrap_or(OnFailure::Ignore);
        let effective = field_on_failure.max(rule_on_failure);
        chain.push((rule, effective));
    }

    if let Some(coerce_entry) = field_type_str_to_coerce(
        &def.field_type,
        def.format.as_deref(),
        field_on_failure,
    )? {
        chain.push(coerce_entry);
    }

    Ok(ResolvedField {
        source_col: csv_col.to_lowercase(),
        chain,
        field_on_failure,
    })
}

/// Resolve all fields in a source config into a ResolvedFieldMap.
/// HashMap key = canonical name; source_col = CSV column (lowercased).
pub fn build_resolved_field_map(source: &SourceConfig) -> anyhow::Result<ResolvedFieldMap> {
    source
        .fields
        .iter()
        .map(|(csv_col, def)| {
            build_resolved_field(csv_col, def)
                .map(|resolved| (def.canonical.clone(), resolved))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use state_search_core::config::{FieldDef, OnFailure, RuleDef, SourceConfig};
    use std::collections::HashMap;

    #[test]
    fn build_resolved_field_map_uses_csv_col_as_key_and_canonical_from_def() {
        let mut fields = HashMap::new();
        fields.insert("Year".to_string(), FieldDef {
            canonical: "year".to_string(),
            field_type: "smallint".to_string(),
            format: None,
            on_failure: None,
            rules: vec![],
        });

        let source = SourceConfig {
            name: "s".to_string(),
            description: None,
            files: vec![],
            fields,
            derived: HashMap::new(),
            target: None,
            exclude_columns: vec![],
            unique_key: vec![],
        };

        let resolved = build_resolved_field_map(&source).unwrap();
        // Key in ResolvedFieldMap is the canonical name
        assert!(resolved.contains_key("year"));
        // source_col is the lowercased CSV column
        assert_eq!(resolved["year"].source_col, "year");
    }

    #[test]
    fn field_type_smallint_produces_coerce_i16() {
        let def = FieldDef {
            canonical: "year".to_string(),
            field_type: "smallint".to_string(),
            format: None,
            on_failure: None,
            rules: vec![],
        };
        let resolved = build_resolved_field("Year", &def).unwrap();
        // smallint → CoerceToI16 appended as implicit rule
        assert_eq!(resolved.chain.len(), 1);
    }

    #[test]
    fn field_type_text_appends_no_coerce() {
        let def = FieldDef {
            canonical: "state_code".to_string(),
            field_type: "text".to_string(),
            format: None,
            on_failure: None,
            rules: vec![],
        };
        let resolved = build_resolved_field("state", &def).unwrap();
        assert!(resolved.chain.is_empty());
    }

    #[test]
    fn unknown_rule_kind_is_error() {
        let def = FieldDef {
            canonical: "col".to_string(),
            field_type: "text".to_string(),
            format: None,
            on_failure: None,
            rules: vec![RuleDef { kind: "does_not_exist".to_string(), on_failure: None }],
        };
        let result = build_resolved_field("col", &def);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown rule kind"));
    }

    #[test]
    fn effective_on_failure_takes_max() {
        let def = FieldDef {
            canonical: "state_code".to_string(),
            field_type: "text".to_string(),
            format: None,
            on_failure: Some(OnFailure::SkipRow),
            rules: vec![RuleDef { kind: "state_name_to_code".to_string(), on_failure: Some(OnFailure::SkipDataset) }],
        };
        let resolved = build_resolved_field("state", &def).unwrap();
        let (_, eff) = &resolved.chain[0];
        assert_eq!(*eff, OnFailure::SkipDataset);
    }
}
