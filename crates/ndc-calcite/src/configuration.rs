extern crate serde_json;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Schema {
    #[serde(rename = "type")]
    pub r#type: String,
    pub name: String,
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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Field {
    #[serde(rename = "type")]
    pub r#type: String,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Table {
    name: Option<String>,
    factory: Option<String>,
    operand: Option<Operand>,
}

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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CalciteConfiguration {
    pub version: String,
    #[serde(rename = "$schema")]
    pub _schema: String,
    pub model: Model,
    pub model_file_path: Option<String>,
    pub fixes: Option<bool>,
}