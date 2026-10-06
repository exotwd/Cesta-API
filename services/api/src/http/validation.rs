use crate::*;

fn invalid(message: &str) -> ApiError {
    ApiError {
        code: "validation_error".into(),
        message: message.into(),
    }
}
pub(crate) fn bounded_text(value: &str, name: &str, maximum: usize) -> Result<(), ApiError> {
    if value.trim().is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(invalid(&format!(
            "{name} is empty, too long or contains control characters"
        )));
    }
    Ok(())
}
pub(crate) fn coordinates(lat: f64, lon: f64) -> Result<(), ApiError> {
    if !lat.is_finite()
        || !lon.is_finite()
        || !(-90.0..=90.0).contains(&lat)
        || !(-180.0..=180.0).contains(&lon)
    {
        return Err(invalid(
            "Coordinates must be finite WGS84 latitude and longitude",
        ));
    }
    Ok(())
}
impl NearbyQuery {
    pub(crate) fn validate(&self) -> Result<(), ApiError> {
        coordinates(self.lat, self.lon)?;
        if self
            .radius
            .is_some_and(|radius| !radius.is_finite() || !(1.0..=5000.0).contains(&radius))
        {
            return Err(invalid("radius must be between 1 and 5000 metres"));
        }
        Ok(())
    }
}
impl DeparturesQuery {
    pub(crate) fn validate(&self) -> Result<(), ApiError> {
        bounded_text(&self.stop_id, "stopId", 256)?;
        if self.limit.is_some_and(|limit| !(1..=100).contains(&limit)) {
            return Err(invalid("limit must be between 1 and 100"));
        }
        if let Some(value) = &self.time {
            bounded_text(value, "time", 64)?;
            if parse_query_time_seconds(value).is_none() {
                return Err(invalid("time must be a supported time or datetime"));
            }
        }
        Ok(())
    }
}
impl JourneySearchBody {
    pub(crate) fn validate_limits(&self) -> Result<(), ApiError> {
        bounded_text(&self.datetime, "datetime", 64)?;
        if self.max_transfers > 8 {
            return Err(invalid("max_transfers must be between 0 and 8"));
        }
        if self.transport_modes.is_empty() || self.transport_modes.len() > 8 {
            return Err(invalid(
                "transport_modes must contain between 1 and 8 modes",
            ));
        }
        if !matches!(self.walking_speed.as_str(), "slow" | "normal" | "fast") {
            return Err(invalid("walking_speed must be slow, normal or fast"));
        }
        for point in [&self.from, &self.to] {
            match point.point_type.as_str() {
                "stop" | "city" => {
                    bounded_text(point.id.as_deref().unwrap_or_default(), "point.id", 256)?
                }
                "coordinate" => coordinates(
                    point.lat.ok_or_else(|| invalid("point.lat is required"))?,
                    point.lon.ok_or_else(|| invalid("point.lon is required"))?,
                )?,
                _ => return Err(invalid("point.type must be stop, city or coordinates")),
            }
        }
        Ok(())
    }
}
