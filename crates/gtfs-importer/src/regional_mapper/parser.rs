use calamine::{DataType, open_workbook, Data, Reader, Xlsx};
use geo_types::Point;
use quick_xml::de::from_reader;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use tracing::debug;

use super::error::HierarchyImportError;
use super::models::{ParsedStopRecord, StopAreaNode, XmlVdvZast};

pub fn parse_excel(path: &Path) -> Result<Vec<ParsedStopRecord>, HierarchyImportError> {
    let mut workbook: Xlsx<_> = open_workbook(path).map_err(HierarchyImportError::Excel)?;
    let sheet_name = workbook.sheet_names().first().cloned().unwrap_or_default();

    let mut records = Vec::new();
    if let Ok(range) = workbook.worksheet_range(&sheet_name) {
        let mut header_map = HashMap::new();

        for (row_idx, row) in range.rows().enumerate() {
            if row_idx == 0 {
                for (col_idx, cell) in row.iter().enumerate() {
                    let text = cell.as_string().unwrap_or("".to_string()).trim().to_string();
                    header_map.insert(text, col_idx);
                }
                continue;
            }

            let get_col = |name: &str| -> Option<&Data> {
                header_map.get(name).and_then(|&idx| row.get(idx))
            };

            let cu = get_col("ČU").and_then(|c| {
                c.as_string().map(|s| s.to_string()).or_else(|| {
                    c.as_f64().map(|f| f.to_string())
                }).or_else(|| c.as_i64().map(|i| i.to_string()))
            });
            let cz = get_col("ČZ").and_then(|c| {
                c.as_string().map(|s| s.to_string()).or_else(|| {
                    c.as_f64().map(|f| f.to_string())
                }).or_else(|| c.as_i64().map(|i| i.to_string()))
            });
            let name = get_col("Název").and_then(|c| c.as_string()).unwrap_or("".to_string()).to_string();
            let cis1 = get_col("CIS 1").and_then(|c| {
                c.as_string().map(|s| s.to_string()).or_else(|| {
                    c.as_f64().map(|f| f.to_string())
                }).or_else(|| c.as_i64().map(|i| i.to_string()))
            });
            
            // Allow commas or dots for floats if they are strings
            let parse_float = |c: Option<&Data>| -> Option<f64> {
                c.and_then(|c| {
                    if let Some(f) = c.as_f64() {
                        Some(f)
                    } else if let Some(s) = c.as_string() {
                        s.replace(',', ".").parse().ok()
                    } else {
                        None
                    }
                })
            };

            let wgs_n = parse_float(get_col("WGS-N"));
            let wgs_e = parse_float(get_col("WGS-E"));
            let platform = get_col("Stanoviště").and_then(|c| c.as_string().map(|s| s.to_string()));

            if let (Some(cu), Some(cz), Some(lat), Some(lon)) = (cu, cz, wgs_n, wgs_e) {
                records.push(ParsedStopRecord {
                    node_id: cu,
                    stop_id: cz,
                    name,
                    platform_code: platform,
                    cis_id: cis1.filter(|s| !s.trim().is_empty()),
                    lat,
                    lon,
                });
            } else {
                debug!("Skipping row due to missing essential fields: {:?}", row);
            }
        }
    }

    Ok(records)
}

pub fn parse_xml(path: &Path) -> Result<Vec<ParsedStopRecord>, HierarchyImportError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let vdv: XmlVdvZast = from_reader(reader)?;

    let mut records = Vec::new();
    for z in vdv.zastavky {
        // Map XML elements to ParsedStopRecord if we have the mapping
        // In the absence of clear grouping ids (ČU, ČZ) in the XML snippet,
        // we might just parse them out if they contain any.
        // If "Oznaceni" is the stop id, etc.
        // Assuming user mainly focuses on Excel for hierarchy.
        let cis_id = Some(z.oznaceni.clone());
        records.push(ParsedStopRecord {
            node_id: "unknown".to_string(), // we might not know without custom rules
            stop_id: z.oznaceni.clone(),
            name: z.nazev,
            platform_code: None,
            cis_id,
            lat: z.lat,
            lon: z.lon,
        });
    }

    Ok(records)
}

pub fn group_into_stop_areas(records: Vec<ParsedStopRecord>) -> Vec<StopAreaNode> {
    let mut groups: HashMap<String, Vec<ParsedStopRecord>> = HashMap::new();
    for r in records {
        if r.node_id == "unknown" {
            continue;
        }
        groups.entry(r.node_id.clone()).or_default().push(r);
    }

    groups
        .into_iter()
        .map(|(node_id, child_platforms)| {
            let name = child_platforms
                .first()
                .map(|c| c.name.clone())
                .unwrap_or_default();
            let lat_sum: f64 = child_platforms.iter().map(|c| c.lat).sum();
            let lon_sum: f64 = child_platforms.iter().map(|c| c.lon).sum();
            let count = child_platforms.len() as f64;

            let centroid = if count > 0.0 {
                Point::new(lon_sum / count, lat_sum / count)
            } else {
                Point::new(0.0, 0.0)
            };

            StopAreaNode {
                id: node_id,
                name,
                centroid,
                child_platforms,
            }
        })
        .collect()
}
