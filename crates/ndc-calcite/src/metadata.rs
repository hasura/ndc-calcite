use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ExportedKey {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "pkTableCatalog")]
    pub pk_table_catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "pkTableSchema")]
    pub pk_table_schema: Option<String>,
    #[serde(rename = "pkTableName")]
    pub pk_table_name: String,
    #[serde(rename = "pkColumnName")]
    pub pk_column_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "pkName")]
    pub pk_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "fkTableCatalog")]
    pub fk_table_catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "fkTableSchema")]
    pub fk_table_schema: Option<String>,
    #[serde(rename = "fkTableName")]
    pub fk_table_name: String,
    #[serde(rename = "fkColumnName")]
    pub fk_column_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "fkName")]
    pub fk_name: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub columns: HashMap<String, ColumnMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "primaryKeys")]
    pub primary_keys: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "exportedKeys")]
    pub exported_keys: Option<Vec<ExportedKey>>
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ColumnMetadata {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "scalarType")]
    pub scalar_type: String,
    pub nullable: bool
}
