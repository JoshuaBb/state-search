use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::FromRow;
use uuid::Uuid;

/// Normalized location dimension.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Location {
    pub id:             Uuid,
    pub county:         Option<String>,
    pub country:        Option<String>,
    pub zip_code:       Option<String>,
    pub fips_code:      Option<String>,
    pub latitude:       Option<f64>,
    pub longitude:      Option<f64>,
    pub city:           Option<String>,
    pub state_code:     Option<String>,
    pub state_name:     Option<String>,
    pub zcta:           Option<String>,
    pub parent_zcta:    Option<String>,
    pub population:     Option<f64>,
    pub density:        Option<f64>,
    pub county_weights: Option<JsonValue>,
    pub county_names_all: Option<String>,
    pub county_fips_all:  Option<String>,
    pub imprecise:      Option<bool>,
    pub military:       Option<bool>,
    pub timezone:       Option<String>,
}

/// `Default` is intentionally NOT derived — the nil UUID is not a valid id.
/// Callers must always provide an explicit `id` via `derive_uuid` or `Uuid::new_v4()`.
#[derive(Debug, Deserialize)]
pub struct NewLocation {
    pub id:             Uuid,
    pub county:         Option<String>,
    pub country:        Option<String>,
    pub zip_code:       Option<String>,
    pub fips_code:      Option<String>,
    pub latitude:       Option<f64>,
    pub longitude:      Option<f64>,
    pub city:           Option<String>,
    pub state_code:     Option<String>,
    pub state_name:     Option<String>,
    pub zcta:           Option<String>,
    pub parent_zcta:    Option<String>,
    pub population:     Option<f64>,
    pub density:        Option<f64>,
    pub county_weights: Option<JsonValue>,
    pub county_names_all: Option<String>,
    pub county_fips_all:  Option<String>,
    pub imprecise:      Option<bool>,
    pub military:       Option<bool>,
    pub timezone:       Option<String>,
}

impl NewLocation {
    pub fn is_empty(&self) -> bool {
        self.county.is_none()
            && self.country.is_none()
            && self.zip_code.is_none()
            && self.fips_code.is_none()
    }
}
