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
