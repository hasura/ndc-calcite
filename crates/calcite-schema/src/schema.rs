//! # Get Schema
//!
//! Introspects Calcite metadata and generates a new schema. Updates
//! the config file with the new schema.
//!

use std::fs::File;
use std::io::Write;
use std::path::Path;
use jni::objects::GlobalRef;
use ndc_models as models;
use ndc_models::SchemaResponse;
use ndc_sdk::connector::Result;
use tracing::Level;
use crate::{collections, scalars};
use crate::models::get_models;
use crate::version5::ParsedConfiguration;

/// Get the schema information from the given `calcite_ref`.
///
/// This function retrieves the data models using `calcite::get_models` function and the scalar types using `scalars::scalars` function.
/// It then calls `collections::collections` function with the data models and scalar types to get the object types and collections.
/// If any error occurs during the retrieval of object types and collections, the function returns the error immediately.
///
/// The `procedures` and `functions` are empty vectors.
///
/// Finally, the schema information is populated into a `SchemaResponse` struct and returned.
///
/// # Arguments
///
/// * `calcite_ref` - A `GlobalRef` representing the Calcite reference.
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
///     let calcite_ref = GlobalRef::new();
///
///     // Get the schema
///     let schema = get_schema(configuration, calcite_ref)?;
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
