use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// Normalized time dimension.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TimePeriod {
    pub id:         Uuid,
    pub year:       i16,
    pub quarter:    Option<i16>,
    pub month:      Option<i16>,
    pub day:        Option<i16>,
    pub date_floor: Option<NaiveDate>,
}

/// `Default` is intentionally NOT derived — the nil UUID is not a valid id.
#[derive(Debug, Deserialize)]
pub struct NewTimePeriod {
    pub id:      Uuid,
    pub year:    i16,
    pub quarter: Option<i16>,
    pub month:   Option<i16>,
    pub day:     Option<i16>,
}

impl NewTimePeriod {
    pub fn date_floor(&self) -> Option<NaiveDate> {
        let month = self
            .month
            .or_else(|| self.quarter.map(|q| (q - 1) * 3 + 1))
            .unwrap_or(1) as u32;
        let day = self.day.unwrap_or(1) as u32;
        NaiveDate::from_ymd_opt(self.year as i32, month, day)
    }
}
