use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Normalized location dimension.
/// All fields are optional — a row may only carry `state_code`, or only `zip_code`.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Location {
    pub id:         i64,
    pub state_code: Option<String>, // e.g. "TX"
    pub state_name: Option<String>, // e.g. "Texas"
    pub country:    Option<String>, // ISO-3166-1 alpha-3, default "USA"
    pub zip_code:   Option<String>,
    pub fips_code:  Option<String>,
    pub latitude:   Option<f64>,
    pub longitude:  Option<f64>,
}

#[derive(Debug, Default, Deserialize)]
pub struct NewLocation {
    pub state_code: Option<String>,
    pub state_name: Option<String>,
    pub country:    Option<String>,
    pub zip_code:   Option<String>,
    pub fips_code:  Option<String>,
    pub latitude:   Option<f64>,
    pub longitude:  Option<f64>,
}

impl NewLocation {
    pub fn is_empty(&self) -> bool {
        self.state_code.is_none()
            && self.state_name.is_none()
            && self.country.is_none()
            && self.zip_code.is_none()
            && self.fips_code.is_none()
    }
}
