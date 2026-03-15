mod private {
    pub trait Sealed {}
}

pub trait FieldRule: private::Sealed + Send + Sync {
    /// Takes `FieldValue` by value to transfer ownership through the chain without
    /// cloning. Rules that inspect and re-emit a `Str` should consume and reconstruct.
    fn apply(&self, value: FieldValue) -> RuleOutcome;
}

#[derive(Debug, Clone)]
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
