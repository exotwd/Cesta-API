use serde::{Deserialize, Serialize};
use transit_model::{CoordinateConfidence, Stop, normalize_czech_name};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeodataCandidate {
    pub source: String,
    pub stop_name: String,
    pub municipality: Option<String>,
    pub lat: f64,
    pub lon: f64,
    pub confidence: CoordinateConfidence,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeodataMatch {
    pub lat: f64,
    pub lon: f64,
    pub source: String,
    pub confidence: CoordinateConfidence,
}

pub fn match_stop(stop: &Stop, candidates: &[GeodataCandidate]) -> Option<GeodataMatch> {
    let normalized = normalize_czech_name(&stop.name);
    candidates
        .iter()
        .filter(|candidate| normalize_czech_name(&candidate.stop_name) == normalized)
        .max_by_key(|candidate| confidence_rank(&candidate.confidence))
        .map(|candidate| GeodataMatch {
            lat: candidate.lat,
            lon: candidate.lon,
            source: candidate.source.clone(),
            confidence: candidate.confidence.clone(),
        })
}

fn confidence_rank(confidence: &CoordinateConfidence) -> u8 {
    match confidence {
        CoordinateConfidence::Exact => 5,
        CoordinateConfidence::High => 4,
        CoordinateConfidence::Medium => 3,
        CoordinateConfidence::Low => 2,
        CoordinateConfidence::Unresolved => 1,
    }
}

#[cfg(test)]
mod tests {
    use transit_model::{AccessibilityStatus, SourceRef, StopLocationType, TransportMode};

    use super::*;

    #[test]
    fn exact_name_match() {
        let stop = Stop {
            id: "s1".to_string(),
            source_ids: vec![SourceRef {
                feed_id: "fixture".to_string(),
                original_id: "s1".to_string(),
                import_run_id: None,
                priority: 10,
                confidence: None,
                suppressed_as_duplicate: false,
            }],
            name: "Praha hl.n.".to_string(),
            normalized_name: "praha hl.n.".to_string(),
            municipality: None,
            district: None,
            region: None,
            lat: None,
            lon: None,
            geom: None,
            coordinate_confidence: CoordinateConfidence::Unresolved,
            coordinate_source: None,
            stop_area_id: None,
            platform_code: None,
            location_type: StopLocationType::Stop,
            parent_station_id: None,
            wheelchair_boarding: AccessibilityStatus::Unknown,
            modes: vec![TransportMode::Train],
            is_active: true,
        };
        let matched = match_stop(
            &stop,
            &[GeodataCandidate {
                source: "manual".to_string(),
                stop_name: "Praha hl.n.".to_string(),
                municipality: None,
                lat: 50.083,
                lon: 14.435,
                confidence: CoordinateConfidence::High,
            }],
        )
        .unwrap();

        assert_eq!(matched.confidence, CoordinateConfidence::High);
    }
}
