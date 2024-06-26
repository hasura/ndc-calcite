extern crate serde_json;

use serde::{Deserialize, Serialize};

/// The type of the schema.
// ANCHOR: Schema
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Schema {
    #[serde(rename = "type")]
    pub r#type: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "sqlDialectFactory")]
    pub sql_dialect_factory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "jdbcUser")]
    pub jdbc_user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "jdbcPassword")]
    pub jdbc_password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "jdbcUrl")]
    pub jdbc_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "jdbcCatalog")]
    pub jdbc_catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "jdbcSchema")]
    pub jdbc_schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub factory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operand: Option<Operand>,
}
// ANCHOR_END: Schema

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Field {
    #[serde(rename = "type")]
    pub r#type: String,
    pub name: String,
}

/// Represents a table.
///
/// ## Fields
///
/// - `name` - The name of the table. It is an optional field.
/// - `factory` - The factory of the table. It is an optional field.
/// - `operand` - The operand of the table. It is an optional field.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Table {
    name: Option<String>,
    factory: Option<String>,
    operand: Option<Operand>,
}

/// Represents the operand used in the schema.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Operand {
    #[serde(skip_serializing_if = "Option::is_none")]
    directory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    database: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "dataFormat")]
    data_format: Option<String>,
}

/// Represents a model.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Model {
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "defaultSchema")]
    pub default_schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schemas: Option<Vec<Schema>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<Table>>,
}

/// Represents the configuration for the Calcite engine.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CalciteConfiguration {
    pub version: String,
    #[serde(rename = "$schema")]
    pub _schema: String,
    pub model: Model,
    pub model_file_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fixes: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jars: Option<String>
}