//! # Configuration File Structure
//!
//! A call to get_schema will update the configuration files.
//! Subsequent calls will use these data structures rather than
//! introspecting data source.
//!
//! If metadata is not in the configuration file on first run it will
//! call get_schema first to populate the config file.
extern crate serde_json;

use std::collections::HashMap;
use ndc_models::{FieldName};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The type of the schema.
// ANCHOR: Schema
#[derive(PartialEq, Eq, JsonSchema, Serialize, Deserialize, Clone, Debug)]
pub struct Schema {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "type")]
    pub r#type: Option<String>,
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
#[derive(Eq, PartialEq, JsonSchema, Serialize, Deserialize, Clone, Debug)]
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

#[derive(Eq, PartialEq, JsonSchema, Serialize, Deserialize, Clone, Debug)]
pub struct Tile {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measures: Option<Vec<Measure>>
}
#[derive(Eq, PartialEq, JsonSchema, Serialize, Deserialize, Clone, Debug)]
pub struct Measure {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Value>
}
#[derive(Eq, PartialEq, JsonSchema, Serialize, Deserialize, Clone, Debug)]
pub struct Materialization {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>
}

#[derive(Eq, PartialEq, JsonSchema, Serialize, Deserialize, Clone, Debug)]
pub struct Column {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    pub name: String,
}

#[derive(Eq, PartialEq, JsonSchema, Serialize, Deserialize, Clone, Debug)]
pub struct Type {
    #[serde(rename = "type")]
    pub r#type: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<Vec<Type>>,
}

#[derive(Eq, PartialEq, JsonSchema, Serialize, Deserialize, Clone, Debug)]
pub struct FieldDef {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub th: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "matchGroup")]
    pub match_group: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "selectedElement")]
    pub selected_element: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "type")]
    pub r#type: Option<String>,
}
/// Represents a table.
///
/// ## Fields
///
/// - `name` - The name of the table. It is an optional field.
/// - `factory` - The factory of the table. It is an optional field.
/// - `operand` - The operand of the table. It is an optional field.
#[derive(Eq, PartialEq, JsonSchema, Serialize, Deserialize, Clone, Debug)]
pub struct Table {
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub factory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "tableName")]
    pub table_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operand: Option<Operand>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<Column>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename="fieldDefs")]
    pub field_defs: Option<Vec<FieldDef>>,
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
#[derive(Eq, PartialEq, JsonSchema, Serialize, Deserialize, Clone, Debug)]
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
#[derive(Eq, PartialEq, JsonSchema, Serialize, Deserialize, Clone, Debug)]
pub struct Operand {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub directory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keyspace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "dataFormat")]
    pub data_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dc: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "pathToCert")]
    pub path_to_cert: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "pathToPrivateKey")]
    pub path_to_private_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "keyPassword")]
    pub key_password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "pathToRootCert")]
    pub path_to_root_cert: Option<String>,
}

/// Represents a model. This is explained in greater detail
/// in the Apache Calcite docs.
#[derive(JsonSchema, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

/// Represents an exported key between two tables in a database.
#[derive(JsonSchema, Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
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

/// Represents metadata for a database table.
///
/// # Fields
///
/// - `catalog` - The catalog of the table.
/// - `schema` - The schema of the table.
/// - `name` - The name of the table.
/// - `description` - The description of the table.
/// - `columns` - A `HashMap` containing the columns of the table.
/// - `primary_keys` - An optional `Vec` of primary key column names.
/// - `exported_keys` - An optional `Vec` of exported keys to other tables.
#[derive(JsonSchema, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TableMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "physicalCatalog")]
    pub physical_catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "physicalSchema")]
    pub physical_schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub columns: HashMap<FieldName, ColumnMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "primaryKeys")]
    pub primary_keys: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "exportedKeys")]
    pub exported_keys: Option<Vec<ExportedKey>>
}

/// Represents the metadata of a column in a database table.
#[derive(PartialEq, Eq, JsonSchema, Serialize, Deserialize, Clone, Debug)]
pub struct ColumnMetadata {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "scalarType")]
    pub scalar_type: String,
    pub nullable: bool
}

