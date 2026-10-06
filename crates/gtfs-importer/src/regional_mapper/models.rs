use geo_types::Point;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct ParsedStopRecord {
    pub node_id: String,
    pub stop_id: String,
    pub name: String,
    pub platform_code: Option<String>,
    pub cis_id: Option<String>,
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Clone)]
pub struct StopAreaNode {
    pub id: String,
    pub name: String,
    pub centroid: Point<f64>,
    pub child_platforms: Vec<ParsedStopRecord>,
}

#[derive(Debug, Deserialize)]
pub struct XmlZastavka {
    #[serde(rename = "@Druh")]
    pub druh: Option<String>,
    #[serde(rename = "@Lat")]
    pub lat: f64,
    #[serde(rename = "@Long")]
    pub lon: f64,
    #[serde(rename = "@Nazev")]
    pub nazev: String,
    #[serde(rename = "@Oznaceni")]
    pub oznaceni: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "VdvZast")]
pub struct XmlVdvZast {
    #[serde(rename = "Zastavka", default)]
    pub zastavky: Vec<XmlZastavka>,
}
