extern crate serde_json;

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::metadata::TableMetadata;

/// The type of the schema.
// ANCHOR: Schema
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Schema {
    #[serde(rename = "type")]
    pub r#type: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<Vec<Value>>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub types: Option<Vec<Type>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub materializations: Option<Vec<Materialization>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lattices: Option<Vec<Lattice>>,
    /// If the schema cannot infer table structures (think NoSQL) define them here.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<Table>>,
}
// ANCHOR_END: Schema

/// Represents a lattice in the schema. A lattice (in Calcite)
/// refers to aggregates.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Lattice {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "algorithmMaxMillis")]
    pub algorithm_max_millis: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "rowCountEstimate")]
    pub row_count_estimate: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "defaultMeasures")]
    pub default_measures: Option<Vec<Measure>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tiles: Option<Vec<Tile>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Tile {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measures: Option<Vec<Measure>>
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Measure {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Value>
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Materialization {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Column {
    #[serde(rename = "type")]
    pub r#type: String,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Type {
    #[serde(rename = "type")]
    pub r#type: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<Vec<Type>>,
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
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub factory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operand: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<Column>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modifiable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<bool>
}

/// Represents a function.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Function {
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "className")]
    pub class_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "methodName")]
    pub method_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<Vec<String>>
}

/// Represents the operand used in the schema.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Operand {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub directory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "dataFormat")]
    pub data_format: Option<String>,
}

/// Represents a model. This is explained in greater detail
/// in the Apache Calcite docs.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Model {
    /// Calcite version
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "defaultSchema")]
    /// You can define multiple schemas - this will be the default one
    pub default_schema: Option<String>,
    /// An array of Schemas. Schemas represent a connection/configuration of a data source.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schemas: Option<Vec<Schema>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub functions: Option<Vec<Function>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub types: Option<Vec<Type>>
}

/// Represents the configuration for the Calcite engine.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CalciteConfiguration {
    /// Hasura NDC version
    pub version: String,
    /// JSON Schema file that defines a valid configuration
    #[serde(rename = "$schema")]
    pub _schema: String,
    /// The Calcite Model - somewhat dependent on type of calcite adapter being used.
    /// Better documentation can be found [here](https://calcite.apache.org/docs/model.html).
    pub model: Model,
    /// Used internally
    pub model_file_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Certain fixes that will solve for missing field values, for non-existing fields.
    /// It's expensive and probably not necessary, but required to pass the NDC
    /// tests. You can set the value to false in order to improve performance.
    pub fixes: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Many common JDBC jars are included by default. Some are not you can
    /// create a directory with additional required JARS and point to that
    /// directory here.
    pub jars: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, TableMetadata>>
}

