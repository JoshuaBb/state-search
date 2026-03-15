use std::{collections::HashMap, sync::Arc};

use state_search_core::{
    models::{location::NewLocation, time::NewTimePeriod},
    repositories::{location::LocationRepository, time::TimeRepository},
    Db,
};
use tracing::{debug, warn};

use crate::transforms::FieldValue;
use super::row::{f64_from_field, i16_from_field, str_from_field};

/// Shared cache mapping a location's content hash to its `dim_location` primary key.
/// Avoids repeated DB round-trips for rows that share the same location.
pub(super) type LocationCache = Arc<tokio::sync::RwLock<HashMap<String, i64>>>;

pub(super) fn new_location_cache() -> LocationCache {
    Arc::new(tokio::sync::RwLock::new(HashMap::new()))
}

// ── Location ──────────────────────────────────────────────────────────────────

pub(super) async fn resolve_location_id(
    transformed: &HashMap<String, FieldValue>,
    default_country: Option<&str>,
    db: &Db,
    row_num: u64,
    cache: &LocationCache,
) -> Option<i64> {
    let loc = resolve_location(transformed, default_country).ok()?;
    let key = location_cache_key(&loc);

    if let Some(&id) = cache.read().await.get(&key) {
        return Some(id);
    }

    match LocationRepository::new(db).upsert(loc).await {
        Ok(id) => {
            cache.write().await.insert(key, id);
            debug!(row = row_num, location_id = id, "resolved location");
            Some(id)
        }
        Err(e) => {
            warn!(row = row_num, error = %e, "could not resolve location");
            None
        }
    }
}

fn resolve_location(
    map: &HashMap<String, FieldValue>,
    default_country: Option<&str>,
) -> Result<NewLocation, ()> {
    let loc = NewLocation {
        county:    str_from_field(map, "county"),
        country:   str_from_field(map, "country")
                       .or_else(|| default_country.map(str::to_string)),
        zip_code:  str_from_field(map, "zip_code"),
        fips_code: str_from_field(map, "fips_code"),
        latitude:  f64_from_field(map, "latitude"),
        longitude: f64_from_field(map, "longitude"),
    };
    if loc.is_empty() { Err(()) } else { Ok(loc) }
}

/// Stable cache key for a `NewLocation`.
/// Uses `.to_bits()` for f64 fields to avoid the `Hash` limitation on floats.
fn location_cache_key(loc: &NewLocation) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}",
        loc.county   .as_deref().unwrap_or(""),
        loc.country  .as_deref().unwrap_or(""),
        loc.zip_code .as_deref().unwrap_or(""),
        loc.fips_code.as_deref().unwrap_or(""),
        loc.latitude .map(f64::to_bits).unwrap_or(0),
        loc.longitude.map(f64::to_bits).unwrap_or(0),
    )
}

// ── Time ──────────────────────────────────────────────────────────────────────

pub(super) async fn resolve_time_id(
    transformed: &HashMap<String, FieldValue>,
    db: &Db,
    row_num: u64,
) -> Option<i64> {
    match resolve_time(transformed) {
        Ok(t) => match TimeRepository::new(db).upsert(t).await {
            Ok(id) => {
                debug!(row = row_num, time_id = id, "resolved time");
                Some(id)
            }
            Err(e) => {
                warn!(row = row_num, error = %e, "could not resolve time");
                None
            }
        },
        Err(_) => None,
    }
}

fn resolve_time(map: &HashMap<String, FieldValue>) -> Result<NewTimePeriod, ()> {
    let year = i16_from_field(map, "year").ok_or(())?;
    Ok(NewTimePeriod {
        year,
        // Clamp to DB check-constraint ranges; out-of-range values become NULL.
        quarter: i16_from_field(map, "quarter").filter(|&q| (1..=4).contains(&q)),
        month:   i16_from_field(map, "month")  .filter(|&m| (1..=12).contains(&m)),
        day:     i16_from_field(map, "day")    .filter(|&d| (1..=31).contains(&d)),
    })
}
