//! # Get Schema
//!
//! Introspects Calcite metadata and generates the NDC schema.

use ndc_models as models;
use ndc_models::SchemaResponse;
use ndc_sdk::connector::Result;
use tracing::Level;
use crate::{collections, scalars};
use crate::version5::ParsedConfiguration;

/// Get the schema information from the metadata present in the configuration.
///
///
/// The `procedures` and `functions` are empty vectors.
///
/// Finally, the schema information is populated into a `SchemaResponse` struct and returned.
///
/// # Returns
///
/// Returns a `Result` containing the `SchemaResponse` on success, or a boxed `dyn Error` on failure.
///
/// # Example
///
/// ```rust
/// use std::error::Error;
/// use ndc_calcite_schema::version5::ParsedConfiguration;
///
/// fn main(configuration: &ParsedConfiguration) -> Result<(), Box<dyn Error>> {
///     // Initialize the Calcite reference
///     use jni::objects::GlobalRef;
///
///     use ndc_calcite_schema::schema::get_schema;
///
///     // Get the schema
///     let schema = get_schema(configuration)?;
///
///     // Print the schema
///     println!("Schema: {:?}", schema);
///
///     Ok(())
/// }
/// ```
// ANCHOR: get_schema
#[tracing::instrument(skip(configuration), level=Level::INFO)]
pub fn get_schema(configuration: &ParsedConfiguration) -> Result<SchemaResponse> {
    let data_models = configuration.metadata.clone().unwrap_or_default();

    let scalar_types = scalars::scalars();
    let (object_types, collections) = match collections::collections(&data_models, &scalar_types) {
        Ok(value) => value,
        Err(value) => return Err(value),
    };
    let procedures = vec![];
    let functions: Vec<models::FunctionInfo> = vec![];
    let schema = SchemaResponse {
        scalar_types,
        object_types,
        collections,
        functions,
        procedures,
    };
    Ok(schema)

}
// ANCHOR_END: get_schema
