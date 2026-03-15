use std::collections::HashMap;
use state_search_core::config::{FieldDef, FieldType, OnFailure, SourceConfig};
use super::{FieldRule, coerce::{
    CoerceToBool, CoerceToDate, CoerceToDateTime, CoerceToF32, CoerceToF64,
    CoerceToI8, CoerceToI16, CoerceToI32, CoerceToI64,
    CoerceToU8, CoerceToU16, CoerceToU32, CoerceToU64,
    DateFormat, DateTimeFormat,
}, rules::state_name_to_code::StateNameToCode};

/// Resolved runtime representation for a single canonical field.
pub struct ResolvedField {
    /// CSV column name (source of the canonical value).
    pub source_col: String,
    /// Ordered rule chain with pre-computed effective OnFailure per entry.
    /// The implicit coerce rule (if any) is always last.
    pub chain: Vec<(Box<dyn FieldRule>, OnFailure)>,
    /// Field-level OnFailure; used as effective_on_failure for the implicit coerce rule.
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

/// Parse and validate a format string given the field type.
/// Returns the coerce rule to use (None = string, no coerce needed).
fn parse_format_and_make_coerce(
    format: Option<&str>,
    field_type: &FieldType,
    field_on_failure: OnFailure,
) -> anyhow::Result<Option<(Box<dyn FieldRule>, OnFailure)>> {
    // format is only valid for Date/DateTime
    if let Some(fmt_str) = format {
        match field_type {
            FieldType::Date => {
                let fmt = DateFormat::from_str(fmt_str)
                    .ok_or_else(|| anyhow::anyhow!("unknown date format: '{}'", fmt_str))?;
                return Ok(Some((Box::new(CoerceToDate(fmt)), field_on_failure)));
            }
            FieldType::DateTime => {
                let fmt = DateTimeFormat::from_str(fmt_str)
                    .ok_or_else(|| anyhow::anyhow!("unknown datetime format: '{}'", fmt_str))?;
                return Ok(Some((Box::new(CoerceToDateTime(fmt)), field_on_failure)));
            }
            _ => {
                anyhow::bail!(
                    "'format' is only valid for date/datetime types, got {:?}",
                    field_type
                );
            }
        }
    }

    // No format string — use defaults or no coerce
    let coerce: Option<Box<dyn FieldRule>> = match field_type {
        FieldType::String   => None,
        FieldType::I8       => Some(Box::new(CoerceToI8)),
        FieldType::I16      => Some(Box::new(CoerceToI16)),
        FieldType::I32      => Some(Box::new(CoerceToI32)),
        FieldType::I64      => Some(Box::new(CoerceToI64)),
        FieldType::U8       => Some(Box::new(CoerceToU8)),
        FieldType::U16      => Some(Box::new(CoerceToU16)),
        FieldType::U32      => Some(Box::new(CoerceToU32)),
        FieldType::U64      => Some(Box::new(CoerceToU64)),
        FieldType::F32      => Some(Box::new(CoerceToF32)),
        FieldType::F64      => Some(Box::new(CoerceToF64)),
        FieldType::Bool     => Some(Box::new(CoerceToBool)),
        FieldType::Date     => Some(Box::new(CoerceToDate(DateFormat::Iso8601))),
        FieldType::DateTime => Some(Box::new(CoerceToDateTime(DateTimeFormat::Rfc3339))),
    };
    Ok(coerce.map(|r| (r, field_on_failure)))
}

/// Look up a rule kind string and return the corresponding boxed rule.
fn lookup_rule(kind: &str) -> Option<Box<dyn FieldRule>> {
    match kind {
        "state_name_to_code" => Some(Box::new(StateNameToCode)),
        _                    => None,
    }
}

/// Resolve a single `FieldDef` into a `ResolvedField`.
pub fn build_resolved_field(
    canonical: &String,
    def: &FieldDef,
) -> anyhow::Result<ResolvedField> {
    let field_on_failure = def.on_failure.unwrap_or(OnFailure::Ignore);
    let mut chain: Vec<(Box<dyn FieldRule>, OnFailure)> = Vec::new();

    // User rules
    for rule_def in &def.rules {
        let rule = lookup_rule(&rule_def.kind)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "unknown rule kind: '{}' (in field '{}')",
                    rule_def.kind,
                    canonical
                )
            })?;
        let rule_on_failure = rule_def.on_failure.unwrap_or(OnFailure::Ignore);
        let effective = field_on_failure.max(rule_on_failure);
        chain.push((rule, effective));
    }

    // Implicit coerce rule (appended last)
    if let Some(coerce_entry) = parse_format_and_make_coerce(
        def.format.as_deref(),
        &def.field_type,
        field_on_failure,
    )? {
        chain.push(coerce_entry);
    }

    Ok(ResolvedField {
        source_col: def.source.to_lowercase(),
        chain,
        field_on_failure,
    })
}

/// Resolve all fields in a source config into a `ResolvedFieldMap`.
pub fn build_resolved_field_map(source: &SourceConfig) -> anyhow::Result<ResolvedFieldMap> {
    source
        .field_map
        .iter()
        .map(|(canonical, def)| {
            build_resolved_field(canonical, def)
                .map(|resolved| (canonical.clone(), resolved))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use state_search_core::config::{FieldDef, FieldType, OnFailure, RuleDef};

    #[test]
    fn unknown_rule_kind_is_error() {
        let def: FieldDef = toml::from_str(r#"
            source = "col"
            [[rules]]
            kind = "does_not_exist"
        "#).unwrap();
        let result = build_resolved_field(&"col".to_string(), &def);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown rule kind"));
    }

    #[test]
    fn format_on_non_date_type_is_error() {
        let def: FieldDef = toml::from_str(r#"
            source = "col"
            type   = "string"
            format = "iso8601"
        "#).unwrap();
        let result = build_resolved_field(&"col".to_string(), &def);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("format"));
    }

    #[test]
    fn unknown_date_format_is_error() {
        let def: FieldDef = toml::from_str(r#"
            source = "col"
            type   = "date"
            format = "not_a_real_format"
        "#).unwrap();
        let result = build_resolved_field(&"col".to_string(), &def);
        assert!(result.is_err());
    }

    #[test]
    fn effective_on_failure_takes_max() {
        // field = skip_row (1), rule = skip_dataset (2) → effective = skip_dataset
        let def: FieldDef = toml::from_str(r#"
            source     = "state"
            on_failure = "skip_row"
            [[rules]]
            kind       = "state_name_to_code"
            on_failure = "skip_dataset"
        "#).unwrap();
        let resolved = build_resolved_field(&"state_name".to_string(), &def).unwrap();
        // chain[0] is state_name_to_code with effective = skip_dataset
        let (_, eff) = &resolved.chain[0];
        assert_eq!(*eff, OnFailure::SkipDataset);
    }

    #[test]
    fn string_type_appends_no_coerce_rule() {
        let def: FieldDef = toml::from_str(r#"source = "col""#).unwrap();
        let resolved = build_resolved_field(&"f".to_string(), &def).unwrap();
        assert!(resolved.chain.is_empty()); // no user rules, no coerce
    }

    #[test]
    fn integer_type_appends_coerce_rule() {
        let def: FieldDef = toml::from_str(r#"
            source = "col"
            type   = "i32"
        "#).unwrap();
        let resolved = build_resolved_field(&"f".to_string(), &def).unwrap();
        assert_eq!(resolved.chain.len(), 1); // just the implicit coerce
    }

    #[test]
    fn both_on_failure_none_defaults_to_ignore() {
        // When neither field nor rule specifies on_failure, effective = Ignore
        let def: FieldDef = toml::from_str(r#"
            source = "state"
            [[rules]]
            kind = "state_name_to_code"
        "#).unwrap();
        let resolved = build_resolved_field(&"state_name".to_string(), &def).unwrap();
        let (_, eff) = &resolved.chain[0];
        assert_eq!(*eff, OnFailure::Ignore);
        assert_eq!(resolved.field_on_failure, OnFailure::Ignore);
    }
}
