use thiserror::Error;

#[derive(Error, Debug)]
pub enum HierarchyImportError {
    #[error("Failed to read file: {0}")]
    Io(#[from] std::io::Error),
    #[error("Excel parsing error: {0}")]
    Excel(#[from] calamine::XlsxError),
    #[error("XML parsing error: {0}")]
    Xml(#[from] quick_xml::DeError),
    #[error("XML Error: {0}")]
    XmlError(#[from] quick_xml::Error),
    #[error("Other error: {0}")]
    Other(String),
}
