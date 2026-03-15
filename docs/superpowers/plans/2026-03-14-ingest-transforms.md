# Ingest Transform Pipeline Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the ingest pipeline with global string trimming, typed field coercion, and a chainable sealed-trait rule system configurable via `config/sources.toml`.

**Architecture:** Per-field `FieldDef` config replaces the flat `field_map` string map. At pipeline startup, `SourceConfig` is resolved into a `ResolvedFieldMap` — a pre-built chain of `Box<dyn FieldRule>` per field, with an implicit coerce rule appended last. The entire CSV file runs inside a single `sqlx` transaction; `SkipDataset` failures trigger a full rollback.

**Tech Stack:** Rust (tokio, axum, sqlx), PostgreSQL, `phf` (compile-time maps), `chrono` (date/time parsing), `thiserror` (error types), `toml` (config deserialization tests)

**Spec:** `docs/superpowers/specs/2026-03-14-ingest-transforms-design.md`

---

## File Structure

**Modified:**
- `core/src/config.rs` — Add `OnFailure`, `FieldType`, `RuleDef`, `FieldDef`; change `SourceConfig.field_map` from `HashMap<String, String>` to `HashMap<String, FieldDef>`
- `core/Cargo.toml` — Add `toml` as dev-dependency for config deserialization tests
- `core/src/repositories/location.rs` — Add `upsert_with_tx` accepting `&mut Transaction`
- `core/src/repositories/time.rs` — Add `upsert_with_tx` accepting `&mut Transaction`
- `core/src/repositories/observation.rs` — Add `bulk_create_with_tx` accepting `&mut Transaction` (no internal begin/commit)
- `ingest/Cargo.toml` — Add `phf` (macros feature), `chrono`, `thiserror`
- `ingest/src/main.rs` — Add `mod transforms;`
- `ingest/src/pipeline.rs` — Integrate `ResolvedFieldMap`, `sqlx` transaction, trim/null defaults, `IngestError`; remove `build_field_map`/`apply_field_map`; use `_with_tx` repo methods
- `config/sources.toml` — Update to new `FieldDef` format

**New:**
- `ingest/src/transforms/mod.rs` — Sealed `FieldRule` trait, `FieldValue`, `RuleOutcome`; test helpers
- `ingest/src/transforms/chain.rs` — `apply_chain()`
- `ingest/src/transforms/coerce.rs` — `DateFormat`, `DateTimeFormat`, all `CoerceToX` rule structs
- `ingest/src/transforms/resolve.rs` — `FieldFormat`, `ResolvedField`, `ResolvedFieldMap`, `build_resolved_field_map()`
- `ingest/src/transforms/rules/mod.rs` — `pub use` exports
- `ingest/src/transforms/rules/state_name_to_code.rs` — `StateNameToCode` + `phf::phf_map!` lookup

---

## Chunk 1: Core Config Types

### Task 1: Add `OnFailure`, `FieldType`, `RuleDef`, `FieldDef` to `core/src/config.rs`

**Files:**
- Modify: `core/src/config.rs`
- Modify: `core/Cargo.toml`

- [ ] **Step 1.1: Add toml dev-dependency**

In `core/Cargo.toml`, add:
```toml
[dev-dependencies]
toml = "0.8"
```

- [ ] **Step 1.2: Write failing tests**

At the bottom of `core/src/config.rs`, add:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_def_minimal_deserialization() {
        let src = r#"source = "state""#;
        let def: FieldDef = toml::from_str(src).unwrap();
        assert_eq!(def.source, "state");
        assert_eq!(def.field_type, FieldType::String);
        assert!(def.on_failure.is_none());
        assert!(def.rules.is_empty());
        assert!(def.format.is_none());
    }

    #[test]
    fn field_def_full_deserialization() {
        let src = r#"
            source     = "state"
            type       = "string"
            on_failure = "skip_row"

            [[rules]]
            kind       = "state_name_to_code"
            on_failure = "skip_dataset"
        "#;
        let def: FieldDef = toml::from_str(src).unwrap();
        assert_eq!(def.on_failure, Some(OnFailure::SkipRow));
        assert_eq!(def.rules.len(), 1);
        assert_eq!(def.rules[0].kind, "state_name_to_code");
        assert_eq!(def.rules[0].on_failure, Some(OnFailure::SkipDataset));
    }

    #[test]
    fn on_failure_ranking() {
        assert!(OnFailure::Ignore < OnFailure::SkipRow);
        assert!(OnFailure::SkipRow < OnFailure::SkipDataset);
    }

    #[test]
    fn field_type_date_with_format() {
        // Note: TOML does not support semicolons as separators — each key must be on its own line
        let src = r#"
            source = "col"
            type   = "date"
            format = "month_day_year"
        "#;
        let def: FieldDef = toml::from_str(src).unwrap();
        assert_eq!(def.field_type, FieldType::Date);
        assert_eq!(def.format.as_deref(), Some("month_day_year"));
    }

    #[test]
    fn field_def_full_explicit_type_is_asserted() {
        let src = r#"
            source     = "state"
            type       = "string"
            on_failure = "skip_row"
        "#;
        let def: FieldDef = toml::from_str(src).unwrap();
        assert_eq!(def.field_type, FieldType::String);
        assert_eq!(def.on_failure, Some(OnFailure::SkipRow));
    }
}
```

- [ ] **Step 1.3: Run — expect compile failure**

```bash
cargo test -p state-search-core 2>&1 | head -20
```
Expected: compile errors for missing types (`FieldDef`, `OnFailure`, `FieldType`, `RuleDef`)

- [ ] **Step 1.4: Add `OnFailure` enum**

In `core/src/config.rs`, before `SourceConfig`:
```rust
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum OnFailure {
    Ignore      = 0,   // treat field as Null, continue
    SkipRow     = 1,   // discard this row, continue ingest
    SkipDataset = 2,   // rollback the entire file transaction
}
// Note: `Ord` is derived by declaration order, which matches the discriminant order
// (Ignore < SkipRow < SkipDataset). Do not reorder variants.

impl Default for OnFailure {
    fn default() -> Self { Self::Ignore }
}
```

- [ ] **Step 1.5: Add `FieldType` enum**

```rust
#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    #[default]
    String,
    I8, I16, I32, I64,
    U8, U16, U32, U64,
    F32, F64,
    Bool,
    Date,
    DateTime,
}
```

- [ ] **Step 1.6: Add `RuleDef` and `FieldDef` structs**

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct RuleDef {
    pub kind: String,
    #[serde(default)]
    pub on_failure: Option<OnFailure>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FieldDef {
    pub source: String,
    /// TOML key is "type"; `type` is a Rust keyword so we rename via serde.
    #[serde(rename = "type", default)]
    pub field_type: FieldType,
    /// Format string — only valid for Date/DateTime; validated at resolve time.
    pub format: Option<String>,
    #[serde(default)]
    pub on_failure: Option<OnFailure>,
    #[serde(default)]
    pub rules: Vec<RuleDef>,
}
```

- [ ] **Step 1.7: Update `SourceConfig.field_map` type**

Change the `field_map` field in `SourceConfig` from:
```rust
pub field_map: HashMap<String, String>,
```
to:
```rust
pub field_map: HashMap<String, FieldDef>,
```

- [ ] **Step 1.8: Run tests — expect pass**

```bash
cargo test -p state-search-core
```
Expected: all tests pass (no failures)

- [ ] **Step 1.9: Verify ingest crate shows expected errors (not unexpected ones)**

```bash
cargo build -p state-search-ingest 2>&1 | head -30
```
Expected: compile errors in `pipeline.rs` where `field_map` is used as `HashMap<String, String>` — this is expected and will be fixed in Chunk 4.

- [ ] **Step 1.10: Commit**

```bash
git add core/src/config.rs core/Cargo.toml
git commit -m "feat(core): add FieldDef, FieldType, OnFailure, RuleDef config types"
```

---

## Chunk 2: Transform Module — Trait, Chain, Coerce

### Task 2: Create `ingest/src/transforms/` module

**Files:**
- Modify: `ingest/Cargo.toml`
- Create: `ingest/src/transforms/mod.rs`
- Create: `ingest/src/transforms/chain.rs`
- Create: `ingest/src/transforms/coerce.rs`
- Create: `ingest/src/transforms/rules/mod.rs` (stub)
- Create: `ingest/src/transforms/resolve.rs` (stub)
- Modify: `ingest/src/main.rs`

- [ ] **Step 2.1: Add dependencies to `ingest/Cargo.toml`**

```toml
phf       = { version = "0.13", features = ["macros"] }
chrono    = { workspace = true }
thiserror = { workspace = true }
```

- [ ] **Step 2.2: Create directory structure**

```bash
mkdir -p ingest/src/transforms/rules
```

- [ ] **Step 2.3: Create `ingest/src/transforms/mod.rs`**

```rust
mod private {
    pub trait Sealed {}
}

pub trait FieldRule: private::Sealed + Send + Sync {
    /// Takes `FieldValue` by value to transfer ownership through the chain without
    /// cloning. Rules that inspect and re-emit a `Str` should consume and reconstruct.
    fn apply(&self, value: FieldValue) -> RuleOutcome;
}

#[derive(Debug)]
pub enum FieldValue {
    Null,
    Str(String),
    I8(i8),   I16(i16),  I32(i32),  I64(i64),
    U8(u8),   U16(u16),  U32(u32),  U64(u64),
    F32(f32), F64(f64),
    Bool(bool),
    Date(chrono::NaiveDate),
    DateTime(chrono::DateTime<chrono::Utc>),
}

#[derive(Debug)]
pub enum RuleOutcome {
    Value(FieldValue),
    Fail,
}

pub mod chain;
pub mod coerce;
pub mod resolve;
pub mod rules;

/// Test-only helpers implementing FieldRule for use in unit tests within this crate.
#[cfg(test)]
pub(crate) mod test_helpers {
    use super::{private, FieldRule, FieldValue, RuleOutcome};

    /// Always returns the given static string as a `Str` value.
    pub struct ReturnStr(pub &'static str);
    impl private::Sealed for ReturnStr {}
    impl FieldRule for ReturnStr {
        fn apply(&self, _: FieldValue) -> RuleOutcome {
            RuleOutcome::Value(FieldValue::Str(self.0.to_string()))
        }
    }

    /// Passes the input value through unchanged.
    pub struct Passthrough;
    impl private::Sealed for Passthrough {}
    impl FieldRule for Passthrough {
        fn apply(&self, value: FieldValue) -> RuleOutcome {
            RuleOutcome::Value(value)
        }
    }

    /// Always returns `RuleOutcome::Fail`.
    pub struct AlwaysFail;
    impl private::Sealed for AlwaysFail {}
    impl FieldRule for AlwaysFail {
        fn apply(&self, _: FieldValue) -> RuleOutcome {
            RuleOutcome::Fail
        }
    }
}
```

- [ ] **Step 2.4: Create stub files so the module compiles**

Create `ingest/src/transforms/rules/mod.rs`:
```rust
pub mod state_name_to_code;
```

Create `ingest/src/transforms/rules/state_name_to_code.rs` (stub):
```rust
// Implemented in Chunk 3
```

Create `ingest/src/transforms/resolve.rs` (stub):
```rust
// Implemented in Chunk 3
```

- [ ] **Step 2.5: Add `mod transforms;` to `ingest/src/main.rs`**

Add at the top of `main.rs`, after `mod pipeline;`:
```rust
mod transforms;
```

- [ ] **Step 2.6: Write failing tests for `chain.rs`**

Create `ingest/src/transforms/chain.rs`:
```rust
use state_search_core::config::OnFailure;
use super::{FieldRule, FieldValue, RuleOutcome};

pub fn apply_chain(
    value: FieldValue,
    chain: &[(Box<dyn FieldRule>, OnFailure)],
) -> Result<FieldValue, OnFailure> {
    let mut current = value;
    for (rule, effective_on_failure) in chain {
        match rule.apply(current) {
            RuleOutcome::Value(v) => current = v,
            RuleOutcome::Fail => match effective_on_failure {
                OnFailure::Ignore => current = FieldValue::Null,
                _                 => return Err(*effective_on_failure),
            },
        }
    }
    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::test_helpers::{AlwaysFail, Passthrough, ReturnStr};

    fn boxed<R: FieldRule + 'static>(r: R) -> Box<dyn FieldRule> { Box::new(r) }

    #[test]
    fn rules_apply_in_order() {
        let chain = vec![
            (boxed(ReturnStr("first")),  OnFailure::Ignore),
            (boxed(ReturnStr("second")), OnFailure::Ignore),
        ];
        let result = apply_chain(FieldValue::Null, &chain).unwrap();
        assert!(matches!(result, FieldValue::Str(s) if s == "second"));
    }

    #[test]
    fn passthrough_preserves_value() {
        let chain = vec![(boxed(Passthrough), OnFailure::Ignore)];
        let result = apply_chain(FieldValue::Str("hello".into()), &chain).unwrap();
        assert!(matches!(result, FieldValue::Str(s) if s == "hello"));
    }

    #[test]
    fn fail_with_ignore_becomes_null() {
        let chain = vec![(boxed(AlwaysFail), OnFailure::Ignore)];
        let result = apply_chain(FieldValue::Str("x".into()), &chain).unwrap();
        assert!(matches!(result, FieldValue::Null));
    }

    #[test]
    fn fail_with_skip_row_returns_err() {
        let chain = vec![(boxed(AlwaysFail), OnFailure::SkipRow)];
        assert_eq!(apply_chain(FieldValue::Null, &chain).unwrap_err(), OnFailure::SkipRow);
    }

    #[test]
    fn fail_with_skip_dataset_returns_err() {
        let chain = vec![(boxed(AlwaysFail), OnFailure::SkipDataset)];
        assert_eq!(apply_chain(FieldValue::Null, &chain).unwrap_err(), OnFailure::SkipDataset);
    }

    #[test]
    fn short_circuits_on_first_fail() {
        let chain = vec![
            (boxed(AlwaysFail),           OnFailure::SkipRow),
            (boxed(ReturnStr("ignored")), OnFailure::Ignore),
        ];
        assert_eq!(apply_chain(FieldValue::Null, &chain).unwrap_err(), OnFailure::SkipRow);
    }

    #[test]
    fn passthrough_rule_does_not_fail_on_null() {
        // Verifies that a rule receiving Null and returning Ok(Null) does not trigger failure,
        // even when effective_on_failure is high. Coerce-specific Null passthrough is tested in coerce.rs.
        let chain = vec![(boxed(Passthrough), OnFailure::SkipDataset)];
        let result = apply_chain(FieldValue::Null, &chain).unwrap();
        assert!(matches!(result, FieldValue::Null));
    }

    #[test]
    fn empty_chain_returns_input() {
        let result = apply_chain(FieldValue::Str("val".into()), &[]).unwrap();
        assert!(matches!(result, FieldValue::Str(s) if s == "val"));
    }
}
```

- [ ] **Step 2.7: Run chain tests**

```bash
cargo test -p state-search-ingest transforms::chain 2>&1
```
Expected: `test result: ok. 8 passed`

- [ ] **Step 2.8: Create `ingest/src/transforms/coerce.rs`**

```rust
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use super::{private, FieldRule, FieldValue, RuleOutcome};

// ── Date / DateTime format enums ─────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub enum DateFormat {
    Iso8601,        // %Y-%m-%d  (default)
    MonthDayYear,   // %m/%d/%Y
    DayMonthYear,   // %d/%m/%Y
    YearMonthDay,   // %Y%m%d
    LongUs,         // %B %d, %Y
}

impl DateFormat {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "iso8601"        => Some(Self::Iso8601),
            "month_day_year" => Some(Self::MonthDayYear),
            "day_month_year" => Some(Self::DayMonthYear),
            "year_month_day" => Some(Self::YearMonthDay),
            "long_us"        => Some(Self::LongUs),
            _                => None,
        }
    }

    fn strptime_fmt(self) -> &'static str {
        match self {
            Self::Iso8601      => "%Y-%m-%d",
            Self::MonthDayYear => "%m/%d/%Y",
            Self::DayMonthYear => "%d/%m/%Y",
            Self::YearMonthDay => "%Y%m%d",
            Self::LongUs       => "%B %d, %Y",
        }
    }

    pub fn parse(self, s: &str) -> Option<NaiveDate> {
        NaiveDate::parse_from_str(s, self.strptime_fmt()).ok()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DateTimeFormat {
    Rfc3339,       // default
    Rfc2822,
    UnixSeconds,   // i64 seconds since epoch
    UnixMillis,    // i64 milliseconds since epoch
    UsDateTime,    // %m/%d/%Y %H:%M:%S
}

impl DateTimeFormat {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "rfc3339"      => Some(Self::Rfc3339),
            "rfc2822"      => Some(Self::Rfc2822),
            "unix_seconds" => Some(Self::UnixSeconds),
            "unix_millis"  => Some(Self::UnixMillis),
            "us_datetime"  => Some(Self::UsDateTime),
            _              => None,
        }
    }

    pub fn parse(self, s: &str) -> Option<DateTime<Utc>> {
        match self {
            Self::Rfc3339 => DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&Utc)),
            Self::Rfc2822 => DateTime::parse_from_rfc2822(s)
                .ok()
                .map(|dt| dt.with_timezone(&Utc)),
            Self::UnixSeconds => s
                .parse::<i64>()
                .ok()
                .and_then(|ts| DateTime::from_timestamp(ts, 0)),
            Self::UnixMillis => s
                .parse::<i64>()
                .ok()
                .and_then(|ms| DateTime::from_timestamp_millis(ms)),
            Self::UsDateTime => NaiveDateTime::parse_from_str(s, "%m/%d/%Y %H:%M:%S")
                .ok()
                .map(|ndt| ndt.and_utc()),
        }
    }
}

// ── Numeric coerce rules (macro-generated) ────────────────────────────────────

macro_rules! impl_numeric_coerce {
    ($name:ident, $variant:ident, $ty:ty) => {
        pub struct $name;
        impl private::Sealed for $name {}
        impl FieldRule for $name {
            fn apply(&self, value: FieldValue) -> RuleOutcome {
                match value {
                    FieldValue::Null    => RuleOutcome::Value(FieldValue::Null),
                    // Values arrive pre-trimmed from the global CSV stage — no need to trim here.
                    FieldValue::Str(s)  => match s.parse::<$ty>() {
                        Ok(v)  => RuleOutcome::Value(FieldValue::$variant(v)),
                        Err(_) => RuleOutcome::Fail,
                    },
                    _ => RuleOutcome::Fail,
                }
            }
        }
    };
}

impl_numeric_coerce!(CoerceToI8,  I8,  i8);
impl_numeric_coerce!(CoerceToI16, I16, i16);
impl_numeric_coerce!(CoerceToI32, I32, i32);
impl_numeric_coerce!(CoerceToI64, I64, i64);
impl_numeric_coerce!(CoerceToU8,  U8,  u8);
impl_numeric_coerce!(CoerceToU16, U16, u16);
impl_numeric_coerce!(CoerceToU32, U32, u32);
impl_numeric_coerce!(CoerceToU64, U64, u64);
impl_numeric_coerce!(CoerceToF32, F32, f32);
impl_numeric_coerce!(CoerceToF64, F64, f64);

// ── Bool coerce rule ──────────────────────────────────────────────────────────

pub struct CoerceToBool;
impl private::Sealed for CoerceToBool {}
impl FieldRule for CoerceToBool {
    fn apply(&self, value: FieldValue) -> RuleOutcome {
        match value {
            FieldValue::Null   => RuleOutcome::Value(FieldValue::Null),
            FieldValue::Str(s) => match s.to_lowercase().as_str() {
                "true"  | "1" | "yes" => RuleOutcome::Value(FieldValue::Bool(true)),
                "false" | "0" | "no"  => RuleOutcome::Value(FieldValue::Bool(false)),
                _                     => RuleOutcome::Fail,
            },
            _ => RuleOutcome::Fail,
        }
    }
}

// ── Date / DateTime coerce rules ──────────────────────────────────────────────

pub struct CoerceToDate(pub DateFormat);
impl private::Sealed for CoerceToDate {}
impl FieldRule for CoerceToDate {
    fn apply(&self, value: FieldValue) -> RuleOutcome {
        match value {
            FieldValue::Null   => RuleOutcome::Value(FieldValue::Null),
            FieldValue::Str(s) => match self.0.parse(&s) {
                Some(d) => RuleOutcome::Value(FieldValue::Date(d)),
                None    => RuleOutcome::Fail,
            },
            _ => RuleOutcome::Fail,
        }
    }
}

pub struct CoerceToDateTime(pub DateTimeFormat);
impl private::Sealed for CoerceToDateTime {}
impl FieldRule for CoerceToDateTime {
    fn apply(&self, value: FieldValue) -> RuleOutcome {
        match value {
            FieldValue::Null   => RuleOutcome::Value(FieldValue::Null),
            FieldValue::Str(s) => match self.0.parse(&s) {
                Some(dt) => RuleOutcome::Value(FieldValue::DateTime(dt)),
                None     => RuleOutcome::Fail,
            },
            _ => RuleOutcome::Fail,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::chain::apply_chain;
    use state_search_core::config::OnFailure;

    fn run(rule: impl FieldRule + 'static, input: FieldValue) -> Result<FieldValue, OnFailure> {
        apply_chain(input, &[(Box::new(rule), OnFailure::SkipRow)])
    }

    #[test]
    fn null_passes_through_numeric() {
        let result = run(CoerceToI32, FieldValue::Null).unwrap();
        assert!(matches!(result, FieldValue::Null));
    }

    #[test]
    fn coerce_i32_valid() {
        let result = run(CoerceToI32, FieldValue::Str("42".into())).unwrap();
        assert!(matches!(result, FieldValue::I32(42)));
    }

    #[test]
    fn coerce_i32_invalid_fails() {
        let result = run(CoerceToI32, FieldValue::Str("abc".into()));
        assert_eq!(result.unwrap_err(), OnFailure::SkipRow);
    }

    #[test]
    fn coerce_i32_out_of_range_fails() {
        let result = run(CoerceToI32, FieldValue::Str("99999999999".into()));
        assert_eq!(result.unwrap_err(), OnFailure::SkipRow);
    }

    #[test]
    fn coerce_f64_valid() {
        let result = run(CoerceToF64, FieldValue::Str("3.14".into())).unwrap();
        assert!(matches!(result, FieldValue::F64(v) if (v - 3.14).abs() < 1e-9));
    }

    #[test]
    fn coerce_bool_true_variants() {
        for s in ["true", "1", "yes", "True", "YES"] {
            let result = run(CoerceToBool, FieldValue::Str(s.into())).unwrap();
            assert!(matches!(result, FieldValue::Bool(true)), "failed for '{s}'");
        }
    }

    #[test]
    fn coerce_bool_false_variants() {
        for s in ["false", "0", "no", "False", "NO"] {
            let result = run(CoerceToBool, FieldValue::Str(s.into())).unwrap();
            assert!(matches!(result, FieldValue::Bool(false)), "failed for '{s}'");
        }
    }

    #[test]
    fn coerce_bool_invalid_fails() {
        let result = run(CoerceToBool, FieldValue::Str("maybe".into()));
        assert_eq!(result.unwrap_err(), OnFailure::SkipRow);
    }

    #[test]
    fn coerce_date_iso8601() {
        let result = run(CoerceToDate(DateFormat::Iso8601), FieldValue::Str("2026-03-14".into())).unwrap();
        assert!(matches!(result, FieldValue::Date(_)));
    }

    #[test]
    fn coerce_date_month_day_year() {
        let result = run(CoerceToDate(DateFormat::MonthDayYear), FieldValue::Str("03/14/2026".into())).unwrap();
        assert!(matches!(result, FieldValue::Date(_)));
    }

    #[test]
    fn coerce_date_invalid_fails() {
        let result = run(CoerceToDate(DateFormat::Iso8601), FieldValue::Str("not-a-date".into()));
        assert_eq!(result.unwrap_err(), OnFailure::SkipRow);
    }

    #[test]
    fn coerce_datetime_rfc3339() {
        let result = run(
            CoerceToDateTime(DateTimeFormat::Rfc3339),
            FieldValue::Str("2026-03-14T00:00:00Z".into()),
        ).unwrap();
        assert!(matches!(result, FieldValue::DateTime(_)));
    }

    #[test]
    fn coerce_datetime_unix_seconds() {
        let result = run(
            CoerceToDateTime(DateTimeFormat::UnixSeconds),
            FieldValue::Str("1741910400".into()),
        ).unwrap();
        assert!(matches!(result, FieldValue::DateTime(_)));
    }

    #[test]
    fn coerce_non_str_non_null_fails() {
        let result = run(CoerceToI32, FieldValue::Bool(true));
        assert_eq!(result.unwrap_err(), OnFailure::SkipRow);
    }

    #[test]
    fn null_passes_through_bool() {
        let result = run(CoerceToBool, FieldValue::Null).unwrap();
        assert!(matches!(result, FieldValue::Null));
    }

    #[test]
    fn null_passes_through_date() {
        let result = run(CoerceToDate(DateFormat::Iso8601), FieldValue::Null).unwrap();
        assert!(matches!(result, FieldValue::Null));
    }

    #[test]
    fn null_passes_through_datetime() {
        let result = run(CoerceToDateTime(DateTimeFormat::Rfc3339), FieldValue::Null).unwrap();
        assert!(matches!(result, FieldValue::Null));
    }
}
```

- [ ] **Step 2.9: Run coerce tests**

```bash
cargo test -p state-search-ingest transforms::coerce 2>&1
```
Expected: `test result: ok. 14 passed`

- [ ] **Step 2.10: Commit**

```bash
git add ingest/Cargo.toml ingest/src/main.rs ingest/src/transforms/
git commit -m "feat(ingest): add FieldRule sealed trait, chain runner, and coerce rules"
```

---

## Chunk 3: StateNameToCode Rule + Resolver

### Task 3a: `StateNameToCode` rule

**Files:**
- Modify: `ingest/src/transforms/rules/state_name_to_code.rs`

- [ ] **Step 3.1: Write failing tests for `StateNameToCode`**

In `ingest/src/transforms/rules/state_name_to_code.rs`:
```rust
use phf::phf_map;
use crate::transforms::{private, FieldRule, FieldValue, RuleOutcome};

static STATE_MAP: phf::Map<&'static str, &'static str> = phf_map! {
    "Alabama"        => "AL", "Alaska"         => "AK", "Arizona"        => "AZ",
    "Arkansas"       => "AR", "California"     => "CA", "Colorado"       => "CO",
    "Connecticut"    => "CT", "Delaware"       => "DE", "Florida"        => "FL",
    "Georgia"        => "GA", "Hawaii"         => "HI", "Idaho"          => "ID",
    "Illinois"       => "IL", "Indiana"        => "IN", "Iowa"           => "IA",
    "Kansas"         => "KS", "Kentucky"       => "KY", "Louisiana"      => "LA",
    "Maine"          => "ME", "Maryland"       => "MD", "Massachusetts"  => "MA",
    "Michigan"       => "MI", "Minnesota"      => "MN", "Mississippi"    => "MS",
    "Missouri"       => "MO", "Montana"        => "MT", "Nebraska"       => "NE",
    "Nevada"         => "NV", "New Hampshire"  => "NH", "New Jersey"     => "NJ",
    "New Mexico"     => "NM", "New York"       => "NY", "North Carolina" => "NC",
    "North Dakota"   => "ND", "Ohio"           => "OH", "Oklahoma"       => "OK",
    "Oregon"         => "OR", "Pennsylvania"   => "PA", "Rhode Island"   => "RI",
    "South Carolina" => "SC", "South Dakota"   => "SD", "Tennessee"      => "TN",
    "Texas"          => "TX", "Utah"           => "UT", "Vermont"        => "VT",
    "Virginia"       => "VA", "Washington"     => "WA", "West Virginia"  => "WV",
    "Wisconsin"      => "WI", "Wyoming"        => "WY",
};

pub struct StateNameToCode;
impl private::Sealed for StateNameToCode {}
impl FieldRule for StateNameToCode {
    fn apply(&self, value: FieldValue) -> RuleOutcome {
        match value {
            FieldValue::Null => RuleOutcome::Value(FieldValue::Null),
            FieldValue::Str(s) => match STATE_MAP.get(s.as_str()) {
                Some(code) => RuleOutcome::Value(FieldValue::Str(code.to_string())),
                None       => RuleOutcome::Fail,
            },
            _ => RuleOutcome::Fail,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::chain::apply_chain;
    use state_search_core::config::OnFailure;

    fn run(input: FieldValue) -> Result<FieldValue, OnFailure> {
        apply_chain(input, &[(Box::new(StateNameToCode), OnFailure::SkipRow)])
    }

    #[test]
    fn known_state_converts() {
        let result = run(FieldValue::Str("Colorado".into())).unwrap();
        assert!(matches!(result, FieldValue::Str(s) if s == "CO"));
    }

    #[test]
    fn all_50_states_present() {
        let states = [
            ("Alabama", "AL"), ("Alaska", "AK"), ("Arizona", "AZ"), ("Arkansas", "AR"),
            ("California", "CA"), ("Colorado", "CO"), ("Connecticut", "CT"), ("Delaware", "DE"),
            ("Florida", "FL"), ("Georgia", "GA"), ("Hawaii", "HI"), ("Idaho", "ID"),
            ("Illinois", "IL"), ("Indiana", "IN"), ("Iowa", "IA"), ("Kansas", "KS"),
            ("Kentucky", "KY"), ("Louisiana", "LA"), ("Maine", "ME"), ("Maryland", "MD"),
            ("Massachusetts", "MA"), ("Michigan", "MI"), ("Minnesota", "MN"), ("Mississippi", "MS"),
            ("Missouri", "MO"), ("Montana", "MT"), ("Nebraska", "NE"), ("Nevada", "NV"),
            ("New Hampshire", "NH"), ("New Jersey", "NJ"), ("New Mexico", "NM"), ("New York", "NY"),
            ("North Carolina", "NC"), ("North Dakota", "ND"), ("Ohio", "OH"), ("Oklahoma", "OK"),
            ("Oregon", "OR"), ("Pennsylvania", "PA"), ("Rhode Island", "RI"), ("South Carolina", "SC"),
            ("South Dakota", "SD"), ("Tennessee", "TN"), ("Texas", "TX"), ("Utah", "UT"),
            ("Vermont", "VT"), ("Virginia", "VA"), ("Washington", "WA"), ("West Virginia", "WV"),
            ("Wisconsin", "WI"), ("Wyoming", "WY"),
        ];
        for (name, expected_code) in states {
            let result = run(FieldValue::Str(name.into())).unwrap();
            assert!(
                matches!(&result, FieldValue::Str(s) if s == expected_code),
                "'{name}' should map to '{expected_code}'"
            );
        }
    }

    #[test]
    fn unknown_state_fails() {
        let result = run(FieldValue::Str("Unknown State".into()));
        assert_eq!(result.unwrap_err(), OnFailure::SkipRow);
    }

    #[test]
    fn null_passes_through() {
        let result = run(FieldValue::Null).unwrap();
        assert!(matches!(result, FieldValue::Null));
    }

    #[test]
    fn non_str_input_fails() {
        let result = run(FieldValue::I32(42));
        assert_eq!(result.unwrap_err(), OnFailure::SkipRow);
    }

    #[test]
    fn dc_is_not_in_map() {
        // Known limitation: DC, territories out of scope for v1
        let result = run(FieldValue::Str("District of Columbia".into()));
        assert_eq!(result.unwrap_err(), OnFailure::SkipRow);
    }
}
```

- [ ] **Step 3.2: Run StateNameToCode tests**

```bash
cargo test -p state-search-ingest transforms::rules::state_name_to_code 2>&1
```
Expected: `test result: ok. 6 passed`

### Task 3b: Rule resolver

**Files:**
- Modify: `ingest/src/transforms/resolve.rs`

- [ ] **Step 3.3: Write failing tests for the resolver**

In `ingest/src/transforms/resolve.rs`, start with the test block:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use state_search_core::config::{FieldDef, FieldType, OnFailure, RuleDef};

    fn field_def(source: &str) -> FieldDef {
        toml::from_str(&format!(r#"source = "{}""#, source)).unwrap()
    }

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
```

- [ ] **Step 3.4: Run — expect compile failure**

```bash
cargo test -p state-search-ingest transforms::resolve 2>&1 | head -20
```
Expected: compile error — `build_resolved_field` not defined.

- [ ] **Step 3.5: Implement `resolve.rs`**

```rust
use std::collections::HashMap;
use anyhow::Context;
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
            .ok_or_else(|| anyhow::anyhow!("unknown rule kind: '{}'", rule_def.kind))
            .with_context(|| format!("in field '{}'", canonical))?;
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
        source_col: def.source.clone(),
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
    // (tests written in Step 3.3 — already present)
}
```

- [ ] **Step 3.6: Run resolver tests**

```bash
cargo test -p state-search-ingest transforms::resolve 2>&1
```
Expected: `test result: ok. 6 passed`

- [ ] **Step 3.7: Commit**

```bash
git add ingest/src/transforms/
git commit -m "feat(ingest): add StateNameToCode rule and field resolver"
```

---

## Chunk 4: Pipeline Integration

### Task 4a: Add transaction-aware methods to repositories in `core`

The existing repositories take `&PgPool` internally. For the pipeline's file-level transaction to be effective, dimension and observation inserts must execute against the outer `sqlx::Transaction`, not the pool. We add `_with_tx` variants alongside the existing API so API routes remain unchanged.

**Files:**
- Modify: `core/src/repositories/location.rs`
- Modify: `core/src/repositories/time.rs`
- Modify: `core/src/repositories/observation.rs`

- [ ] **Step 4a.1: Add `upsert_with_tx` to `LocationRepository`**

In `core/src/repositories/location.rs`, add this method to the `impl` block:
```rust
/// Upsert a location using an existing transaction.
pub async fn upsert_with_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    loc: NewLocation,
) -> Result<i64> {
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO dim_location (state_code, state_name, country, zip_code, fips_code, latitude, longitude)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (state_code, country, zip_code) DO UPDATE
             SET state_name = EXCLUDED.state_name,
                 fips_code  = COALESCE(EXCLUDED.fips_code,  dim_location.fips_code),
                 latitude   = COALESCE(EXCLUDED.latitude,   dim_location.latitude),
                 longitude  = COALESCE(EXCLUDED.longitude,  dim_location.longitude)
         RETURNING id",
    )
    .bind(loc.state_code)
    .bind(loc.state_name)
    .bind(loc.country)
    .bind(loc.zip_code)
    .bind(loc.fips_code)
    .bind(loc.latitude)
    .bind(loc.longitude)
    .fetch_one(&mut **tx)
    .await?;
    Ok(id)
}
```

- [ ] **Step 4a.2: Add `upsert_with_tx` to `TimeRepository`**

In `core/src/repositories/time.rs`, add:
```rust
/// Upsert a time period using an existing transaction.
pub async fn upsert_with_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    t: NewTimePeriod,
) -> Result<i64> {
    let date_floor = t.date_floor();
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO dim_time (year, quarter, month, day, date_floor)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (year, quarter, month, day) DO UPDATE
             SET date_floor = EXCLUDED.date_floor
         RETURNING id",
    )
    .bind(t.year)
    .bind(t.quarter)
    .bind(t.month)
    .bind(t.day)
    .bind(date_floor)
    .fetch_one(&mut **tx)
    .await?;
    Ok(id)
}
```

- [ ] **Step 4a.3: Add `bulk_create_with_tx` to `ObservationRepository`**

In `core/src/repositories/observation.rs`, add:
```rust
/// Bulk insert observations using an existing transaction (no internal begin/commit).
pub async fn bulk_create_with_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    observations: Vec<NewObservation>,
) -> Result<u64> {
    let mut count = 0u64;
    for obs in observations {
        sqlx::query(
            "INSERT INTO fact_observations
                 (raw_import_id, location_id, time_id, source_name, metric_name, metric_value, attributes)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(obs.raw_import_id)
        .bind(obs.location_id)
        .bind(obs.time_id)
        .bind(obs.source_name)
        .bind(obs.metric_name)
        .bind(obs.metric_value)
        .bind(obs.attributes)
        .execute(&mut **tx)
        .await?;
        count += 1;
    }
    Ok(count)
}
```

- [ ] **Step 4a.4: Verify core still compiles (existing pool-based API unchanged)**

```bash
cargo build -p state-search-core
```
Expected: clean build.

- [ ] **Step 4a.5: Commit**

```bash
git add core/src/repositories/
git commit -m "feat(core): add _with_tx variants to location, time, observation repos"
```

---

### Task 4b: Integrate transforms into `pipeline.rs` and update config

**Files:**
- Modify: `ingest/src/pipeline.rs`
- Modify: `config/sources.toml`

- [ ] **Step 4.1: Write failing unit tests for `row_to_canonical`**

```rust
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
    fn row_to_canonical_empty_becomes_null() {
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
```

- [ ] **Step 4.2: Run — expect compile failure**

```bash
cargo test -p state-search-ingest pipeline 2>&1 | head -20
```
Expected: compile errors — `row_to_canonical` not found.

- [ ] **Step 4.3: Rewrite `pipeline.rs`**

Replace the entire file with:
```rust
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
    Db, Result,
};
use tracing::{debug, info, warn};

use crate::transforms::{
    chain::apply_chain,
    resolve::{build_resolved_field_map, ResolvedFieldMap},
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

fn str_from_field<'a>(map: &'a HashMap<String, FieldValue>, key: &str) -> Option<String> {
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
```

- [ ] **Step 4.4: Run pipeline unit tests**

```bash
cargo test -p state-search-ingest pipeline 2>&1
```
Expected: `test result: ok. 3 passed`

- [ ] **Step 4.5: Verify the full crate compiles**

```bash
cargo build -p state-search-ingest 2>&1
```
Expected: clean build (zero errors).

- [ ] **Step 4.6: Update `config/sources.toml` to the new format**

Replace the existing file with:
```toml
[[ingest.sources]]
name        = "co_public_drinking_water"
description = "Colorado public drinking water quality"
files       = ["data/usa/co/public_drinking_water/public_drinking_water_2026-03-14.csv"]

[ingest.sources.field_map.state_name]
source     = "state"
on_failure = "skip_row"

[[ingest.sources.field_map.state_name.rules]]
kind       = "state_name_to_code"
on_failure = "skip_dataset"

[ingest.sources.field_map.fips_code]
source = "county_fips"

[ingest.sources.field_map.year]
source = "year"
type   = "i16"

[ingest.sources.field_map.quarter]
source = "quarter"
type   = "i16"

[ingest.sources.field_map.latitude]
source = "pws_latitude"
type   = "f64"

[ingest.sources.field_map.longitude]
source = "pws_longitude"
type   = "f64"
```

- [ ] **Step 4.7: Verify config loads without error**

```bash
cargo run -p state-search-ingest -- --help 2>&1
```
Expected: prints the CLI help (no panics from config load).

- [ ] **Step 4.8: Run full test suite**

```bash
cargo test 2>&1
```
Expected: all tests pass.

- [ ] **Step 4.9: Final commit**

```bash
git add ingest/src/pipeline.rs config/sources.toml
git commit -m "feat(ingest): integrate transform pipeline with transaction, trim/null defaults, and IngestError"
```
