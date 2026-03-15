# Ingest Transform Pipeline Design

**Date:** 2026-03-14
**Status:** Approved
**Scope:** `ingest` crate + `core/src/config.rs`

---

## Overview

Extend the ingest pipeline with three capabilities:

1. **Global implicit defaults** — trim all string values and coerce empty strings to `None` at the CSV parsing stage, before any field-level processing.
2. **Per-field metadata** — each field in `field_map` declares its source column, target type, optional format, optional failure behavior, and an ordered list of transform rules.
3. **Chainable rule system** — a sealed `FieldRule` trait drives sequential transforms. Built-in rules are registered via a factory. The declared `type` implicitly appends a coerce rule at the end of each chain, after all user-defined rules have run.

---

## Config Schema

`SourceConfig.field_map` changes from `HashMap<String, String>` to `HashMap<String, FieldDef>`. The outer key remains the canonical field name (e.g. `state_name`); `FieldDef.source` holds the CSV column name.

### `FieldDef`

```rust
struct FieldDef {
    source: String,                   // CSV column name (required)
    #[serde(rename = "type", default)]
    field_type: FieldType,            // TOML key is "type"; defaults to String
    format: Option<FieldFormat>,      // only valid for Date / DateTime; hard error otherwise
    #[serde(default)]
    on_failure: Option<OnFailure>,    // field-level default; None → OnFailure::Ignore at resolve time
    #[serde(default)]
    rules: Vec<RuleDef>,              // ordered transform rules; all run before implicit coerce
}
```

> **Note:** `field_type` uses `#[serde(rename = "type")]` because `type` is a Rust keyword and cannot be a struct field name. In TOML, authors write `type = "i16"`.

### `FieldType`

```rust
enum FieldType {
    String,                           // default
    I8, I16, I32, I64,
    U8, U16, U32, U64,
    F32, F64,
    Bool,
    Date,
    DateTime,
}
```

### `FieldFormat`

A single extensible enum wrapping type-specific format enums. New types can add inner enums without changing the outer structure.

```rust
enum FieldFormat {
    Date(DateFormat),
    DateTime(DateTimeFormat),
    // Future: Bool(BoolFormat), Number(NumberFormat), etc.
}

enum DateFormat {
    Iso8601,        // default — %Y-%m-%d
    MonthDayYear,   // %m/%d/%Y
    DayMonthYear,   // %d/%m/%Y
    YearMonthDay,   // %Y%m%d (compact)
    LongUs,         // %B %d, %Y
}

enum DateTimeFormat {
    Rfc3339,        // default — %Y-%m-%dT%H:%M:%S%z
    Rfc2822,        // %a, %d %b %Y %H:%M:%S %z
    UnixSeconds,    // string parsed as i64 seconds since Unix epoch
    UnixMillis,     // string parsed as i64 milliseconds since Unix epoch
    UsDateTime,     // %m/%d/%Y %H:%M:%S
}
```

`format` is deserialized from a plain string (e.g. `"month_day_year"`) and validated against the declared `type` at startup:
- `format` on any type other than `Date` or `DateTime` is a hard startup error.
- A `DateFormat` variant used with `type = "datetime"` (or vice versa) is a hard startup error.

`UnixSeconds` and `UnixMillis` parse the field's string value as `i64`; non-integer content produces `RuleOutcome::Fail`.

### `RuleDef`

```rust
struct RuleDef {
    kind: String,                     // e.g. "state_name_to_code"
    on_failure: Option<OnFailure>,    // rule-level override; None = rank 0
}
```

### `OnFailure`

```rust
enum OnFailure {
    Ignore      = 0,   // treat the field as Null and continue; row is not dropped
    SkipRow     = 1,   // discard this row, continue ingest
    SkipDataset = 2,   // rollback the entire file transaction
}
```

`OnFailure` is a concrete type at every call site — there are no `Option<OnFailure>` values in resolved structures. `FieldDef.on_failure` and `RuleDef.on_failure` are `Option<OnFailure>` in config only; `None` maps to `OnFailure::Ignore` during resolution.

**Rank resolution:** `effective_on_failure = max(field.on_failure ?? Ignore, rule.on_failure ?? Ignore)`. Higher rank always wins. `field_on_failure` stored in `ResolvedField` (see §Startup Resolution) is used as the `effective_on_failure` for the implicit coerce rule, which has no `RuleDef.on_failure` of its own.

---

## TOML Example

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
on_failure = "skip_dataset"       # escalates above field-level default

[ingest.sources.field_map.fips_code]
source = "county_fips"            # type defaults to string, no rules

[ingest.sources.field_map.year]
source = "year"
type   = "i16"                    # CoerceToI16 appended after all rules (none here)

[ingest.sources.field_map.latitude]
source = "pws_latitude"
type   = "f64"

[ingest.sources.field_map.recorded_at]
source = "RecordDate"
type   = "date"
format = "month_day_year"         # overrides default iso8601
```

> The TOML example must be validated with `toml::from_str` as part of config loading tests. A minimal working fixture should be included in the test suite.

**Config invariants enforced at startup:**
- `trim` is not a valid rule `kind` — trimming is a global pipeline invariant.
- There is no explicit `coerce` rule kind — coercion is driven solely by `type` and always runs last.
- `format` on any type other than `Date` or `DateTime` is a hard startup error.
- A format variant mismatched to the declared type (e.g. `DateFormat` with `type = "datetime"`) is a hard startup error.
- Unknown `kind` values in `rules` are a hard startup error.

---

## Transform Module Layout

```
ingest/src/transforms/
  mod.rs                    # FieldRule trait, FieldValue, RuleOutcome, OnFailure
  chain.rs                  # apply_chain() — sequential rule runner
  resolve.rs                # RuleDef → Box<dyn FieldRule> factory / registry
  coerce.rs                 # implicit coerce rules (CoerceToI16, CoerceToF64, CoerceToDate, etc.)
  rules/
    mod.rs
    state_name_to_code.rs   # StateNameToCode struct + phf_map lookup
```

---

## Sealed Trait Design

```rust
// ingest/src/transforms/mod.rs

mod private { pub trait Sealed {} }

pub trait FieldRule: private::Sealed + Send + Sync {
    /// Takes `FieldValue` by value to transfer ownership through the chain without
    /// cloning. Rules that inspect and re-emit a `Str` value should consume and
    /// reconstruct it rather than cloning.
    fn apply(&self, value: FieldValue) -> RuleOutcome;
}

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

pub enum RuleOutcome {
    Value(FieldValue),   // transformed value; Null = field is missing
    Fail,                // rule could not complete — caller applies effective OnFailure
}
```

Only types inside the `ingest` crate can implement `FieldRule` (sealed via the private `Sealed` supertrait).

---

## Chain Runner

```rust
// ingest/src/transforms/chain.rs

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
```

Rules are stored in declaration order (TOML array order). The implicit coerce rule is always the last entry appended at resolve time. A `Fail` with `OnFailure::Ignore` sets the current value to `Null` and continues. A `Fail` with `SkipRow` or `SkipDataset` short-circuits immediately via `Err`.

---

## Startup Resolution

Before the CSV loop begins, `SourceConfig` is resolved once into a `ResolvedFieldMap`:

```rust
struct ResolvedField {
    source_col: String,
    /// User rules in declaration order, with implicit coerce rule appended last.
    /// Each entry carries its pre-computed effective_on_failure.
    chain: Vec<(Box<dyn FieldRule>, OnFailure)>,
    /// The field-level OnFailure (rank 0 if None). Applied as effective_on_failure
    /// for the implicit coerce rule, which has no RuleDef.on_failure of its own.
    field_on_failure: OnFailure,
}

type ResolvedFieldMap = HashMap<String, ResolvedField>;
```

Resolution steps per field:
1. Validate `format` is compatible with `field_type` (hard error if not).
2. Look up each `RuleDef.kind` in the rule registry → `Box<dyn FieldRule>` (hard error if unknown).
3. Compute `effective_on_failure = max(field.on_failure ?? 0, rule.on_failure ?? 0)` per rule.
4. If `field_type != String`, append the appropriate implicit coerce rule at the end of the chain, using `field_on_failure` as its `effective_on_failure`.

---

## Built-in Rules

### `StateNameToCode`

- **Accepts:** `FieldValue::Str` — values arrive pre-trimmed from the global stage.
- **`Null` input:** passes through as `Null` (no failure).
- **Any other variant** (e.g. a pre-coerced typed value): `RuleOutcome::Fail`.
- **Lookup:** compile-time `phf_map` of all 50 US state names → 2-char abbreviations.
- **Match:** `RuleOutcome::Value(FieldValue::Str("CO"))`
- **No match:** `RuleOutcome::Fail` — caller's `effective_on_failure` applies.

> **Known limitation (v1):** The map covers the 50 US states only. Washington D.C., Puerto Rico, Guam, the U.S. Virgin Islands, and other territories are out of scope and will produce `Fail` if passed through this rule.

### Implicit Coerce Rules (appended by resolver, not user-configurable)

All coerce rules accept `FieldValue::Str` as input (the final output of user rules). `Null` passes through as `Null` without failure. Any variant other than `Str` or `Null` produces `Fail`.

| `FieldType`  | Parse behavior | Failure |
|--------------|----------------|---------|
| `I8`–`I64`   | Parse string as signed integer of stated width | `RuleOutcome::Fail` if unparseable or out of range |
| `U8`–`U64`   | Parse string as unsigned integer of stated width | `RuleOutcome::Fail` if unparseable or out of range |
| `F32`, `F64` | Parse string as float of stated width | `RuleOutcome::Fail` if unparseable |
| `Bool`       | Accepts `"true"`/`"false"`, `"1"`/`"0"`, `"yes"`/`"no"` (case-insensitive) | `RuleOutcome::Fail` otherwise |
| `Date`       | Parsed using `DateFormat` (default `Iso8601`) | `RuleOutcome::Fail` if unparseable |
| `DateTime`   | Parsed using `DateTimeFormat` (default `Rfc3339`); `UnixSeconds`/`UnixMillis` parse string as `i64` | `RuleOutcome::Fail` if unparseable |
| `String`     | No coerce rule appended | n/a |

---

## Pipeline Integration

### Global CSV stage (`row_to_json`)

Applied to every field on every row, before any field-level rules:

1. Trim leading/trailing whitespace from all string values.
2. Empty string after trim → `FieldValue::Null`.

### Unmapped columns

CSV columns with no corresponding `FieldDef` entry are passed through under their original CSV header name, preserving the existing behavior where unrecognized columns become metric fields via `extract_metrics`.

### Skip-check

The already-ingested check (`SELECT EXISTS ... FROM raw_imports WHERE source_file = $1`) runs **before** the transaction is opened, using a plain pool connection. This ensures a file that was previously rolled back (e.g. by `SkipDataset`) is correctly retried on the next run, since the rollback leaves no `raw_imports` rows for that file.

### Per-row flow

The entire file loop runs inside a single `sqlx` transaction opened after the skip-check passes. All inserts — including `raw_imports` — are part of this transaction.

```
Open sqlx transaction
│
└─ for each CSV row:
     │
     ▼  row_to_json — trim all strings, empty → Null  (implicit, global)
     │
     ▼  INSERT INTO raw_imports (inside transaction)
     │
     ▼  apply_field_map — rename source columns → canonical names
     │  (unmapped columns pass through under original CSV header name)
     │
     ▼  for each canonical field, run its ResolvedField chain (left to right):
     │     [user rules in declaration order] → [implicit coerce rule last]
     │
     │     Ok(FieldValue)              → proceed
     │     Err(SkipRow)  (rank 1)      → warn + log row/field, discard row, next row
     │     Err(SkipDataset)  (rank 2)  → rollback transaction, return Err to caller
     │     rank 0 (both None)          → treat field as Null, continue
     │
     ▼  resolve_location / resolve_time / extract_metrics  (unchanged)
     │
     ▼  bulk insert observations (inside transaction)

Commit transaction (all rows succeeded or only SkipRow discards occurred)
```

### Error propagation

`SkipDataset` propagates as a new `IngestError::DatasetSkipped` variant (a local error type in `ingest/src/pipeline.rs`). `pipeline::run` holds the `sqlx::Transaction` at the top of the function, calls `.rollback()` on `DatasetSkipped`, and returns the error to `main`. `main` logs the failure and exits non-zero.

### Replacing obsolete helpers

The existing `build_field_map` and `apply_field_map` helpers in `pipeline.rs` are fully superseded by the `ResolvedFieldMap` resolution step and must be deleted. The new `apply_field_map` step in the per-row flow is handled by iterating `ResolvedFieldMap` to look up each canonical field's `source_col`.

---

## What Does Not Change

- `resolve_location`, `resolve_time`, `extract_metrics` in `pipeline.rs`
- `LocationRepository`, `TimeRepository`, `ObservationRepository`
- API routes, `AppState`, frontend
- Migration files

---

## Dependencies to Add

| Crate | Use |
|-------|-----|
| `phf` (with `macros` feature) | Compile-time perfect hash map for `StateNameToCode` via `phf::phf_map!` macro — no `build.rs` required |
