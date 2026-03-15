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
