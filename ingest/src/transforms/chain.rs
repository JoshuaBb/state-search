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
