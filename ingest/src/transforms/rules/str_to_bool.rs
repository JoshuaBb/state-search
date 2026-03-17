use crate::transforms::{private, FieldRule, FieldValue, RuleOutcome};

/// Converts common boolean string representations to `FieldValue::Bool`.
///
/// Recognized truthy values (case-insensitive): `"true"`, `"1"`, `"yes"`
/// Recognized falsy values (case-insensitive):  `"false"`, `"0"`, `"no"`
///
/// Useful when a CSV column encodes booleans as "TRUE"/"FALSE" text strings
/// and you want explicit conversion as a named rule step.
pub struct StrToBool;

impl private::Sealed for StrToBool {}

impl FieldRule for StrToBool {
    fn apply(&self, value: FieldValue) -> RuleOutcome {
        match value {
            FieldValue::Null      => RuleOutcome::Value(FieldValue::Null),
            FieldValue::Bool(b)   => RuleOutcome::Value(FieldValue::Bool(b)),
            FieldValue::Str(s) => match s.to_lowercase().as_str() {
                "true"  | "1" | "yes" => RuleOutcome::Value(FieldValue::Bool(true)),
                "false" | "0" | "no"  => RuleOutcome::Value(FieldValue::Bool(false)),
                _                     => RuleOutcome::Fail,
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
        apply_chain(input, &[(Box::new(StrToBool), OnFailure::SkipRow)])
    }

    #[test]
    fn null_passes_through() {
        assert!(matches!(run(FieldValue::Null).unwrap(), FieldValue::Null));
    }

    #[test]
    fn bool_passes_through() {
        assert!(matches!(run(FieldValue::Bool(true)).unwrap(), FieldValue::Bool(true)));
        assert!(matches!(run(FieldValue::Bool(false)).unwrap(), FieldValue::Bool(false)));
    }

    #[test]
    fn truthy_strings() {
        for s in ["true", "TRUE", "True", "1", "yes", "YES"] {
            assert!(
                matches!(run(FieldValue::Str(s.into())).unwrap(), FieldValue::Bool(true)),
                "expected true for '{s}'"
            );
        }
    }

    #[test]
    fn falsy_strings() {
        for s in ["false", "FALSE", "False", "0", "no", "NO"] {
            assert!(
                matches!(run(FieldValue::Str(s.into())).unwrap(), FieldValue::Bool(false)),
                "expected false for '{s}'"
            );
        }
    }

    #[test]
    fn unrecognized_string_fails() {
        assert_eq!(run(FieldValue::Str("maybe".into())).unwrap_err(), OnFailure::SkipRow);
    }
}
