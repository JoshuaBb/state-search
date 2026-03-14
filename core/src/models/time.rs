use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Normalized time dimension.
/// Only `year` is required; quarter/month/day are optional and mutually independent.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TimePeriod {
    pub id:         i64,
    pub year:       i16,
    pub quarter:    Option<i16>, // 1–4
    pub month:      Option<i16>, // 1–12
    pub day:        Option<i16>, // 1–31
    /// Best-effort resolved date (floor of the period).
    pub date_floor: Option<NaiveDate>,
}

#[derive(Debug, Default, Deserialize)]
pub struct NewTimePeriod {
    pub year:    i16,
    pub quarter: Option<i16>,
    pub month:   Option<i16>,
    pub day:     Option<i16>,
}

impl NewTimePeriod {
    /// Compute the floor date from available fields.
    pub fn date_floor(&self) -> Option<NaiveDate> {
        let month = self
            .month
            .or_else(|| self.quarter.map(|q| (q - 1) * 3 + 1))
            .unwrap_or(1) as u32;
        let day = self.day.unwrap_or(1) as u32;
        NaiveDate::from_ymd_opt(self.year as i32, month, day)
    }
}
