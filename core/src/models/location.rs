use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// Normalized location dimension.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Location {
    pub id:        Uuid,
    pub county:    Option<String>,
    pub country:   Option<String>,
    pub zip_code:  Option<String>,
    pub fips_code: Option<String>,
    pub latitude:  Option<f64>,
    pub longitude: Option<f64>,
}

/// `Default` is intentionally NOT derived — the nil UUID is not a valid id.
/// Callers must always provide an explicit `id` via `derive_uuid` or `Uuid::new_v4()`.
#[derive(Debug, Deserialize)]
pub struct NewLocation {
    pub id:        Uuid,
    pub county:    Option<String>,
    pub country:   Option<String>,
    pub zip_code:  Option<String>,
    pub fips_code: Option<String>,
    pub latitude:  Option<f64>,
    pub longitude: Option<f64>,
}

impl NewLocation {
    pub fn is_empty(&self) -> bool {
        self.county.is_none()
            && self.country.is_none()
            && self.zip_code.is_none()
            && self.fips_code.is_none()
    }
}
