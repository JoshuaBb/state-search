use std::{collections::HashMap, sync::Arc};

use state_search_core::{
    models::{location::NewLocation, time::NewTimePeriod},
    repositories::{location::LocationRepository, time::TimeRepository},
    Db,
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::transforms::FieldValue;
use super::row::{f64_from_field, i16_from_field, str_from_field};
use super::uuid::derive_uuid;

/// Shared cache mapping a location's content hash to its `dim_location` UUID PK.
pub(super) type LocationCache = Arc<tokio::sync::RwLock<HashMap<String, Uuid>>>;

pub(super) fn new_location_cache() -> LocationCache {
    Arc::new(tokio::sync::RwLock::new(HashMap::new()))
}

// Hardcoded internal unique-key columns for fact-path dim upserts (alphabetical for stable sort)
const LOC_KEY_COLS: &[&str] = &["county", "country", "fips_code", "latitude", "longitude", "zip_code"];
const TIME_KEY_COLS: &[&str] = &["day", "month", "quarter", "year"];

// ── Location ──────────────────────────────────────────────────────────────────

pub(super) async fn resolve_location_id(
    transformed: &HashMap<String, FieldValue>,
    default_country: Option<&str>,
    db: &Db,
    row_num: u64,
    cache: &LocationCache,
) -> Option<Uuid> {
    let loc = resolve_location(transformed, default_country).ok()?;
    let key = location_cache_key(&loc);

    if let Some(&id) = cache.read().await.get(&key) {
        return Some(id);
    }

    match LocationRepository::new(db).upsert(loc).await {
        Ok(id) => {
            cache.write().await.insert(key, id);
            debug!(row = row_num, ?id, "resolved location");
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
    // Build a temporary map including the default country so derive_uuid sees it
    let mut tmp = map.clone();
    if tmp.get("country").map_or(true, |v| matches!(v, FieldValue::Null)) {
        if let Some(c) = default_country {
            tmp.insert("country".to_string(), FieldValue::Str(c.to_string()));
        }
    }

    let loc = NewLocation {
        id:        derive_uuid(LOC_KEY_COLS, &tmp),
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

fn location_cache_key(loc: &NewLocation) -> String {
    // Cache key uses the UUID itself — guaranteed unique and consistent
    loc.id.to_string()
}

// ── Time ──────────────────────────────────────────────────────────────────────

pub(super) async fn resolve_time_id(
    transformed: &HashMap<String, FieldValue>,
    db: &Db,
    row_num: u64,
) -> Option<Uuid> {
    match resolve_time(transformed) {
        Ok(t) => match TimeRepository::new(db).upsert(t).await {
            Ok(id) => {
                debug!(row = row_num, ?id, "resolved time");
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
        id:      derive_uuid(TIME_KEY_COLS, map),
        year,
        quarter: i16_from_field(map, "quarter").filter(|&q| (1..=4).contains(&q)),
        month:   i16_from_field(map, "month")  .filter(|&m| (1..=12).contains(&m)),
        day:     i16_from_field(map, "day")    .filter(|&d| (1..=31).contains(&d)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn location_cache_holds_uuid_values() {
        let cache = new_location_cache();
        let _: LocationCache = cache;
    }
}
