//! # Get Schema
//!
//! Introspects Calcite metadata and generates the NDC schema.

use jni::objects::GlobalRef;
use ndc_models as models;
use ndc_models::SchemaResponse;
use std::env;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::models::get_models;
use crate::version5::ParsedConfiguration;
use crate::{collections, scalars};
use ndc_calcite_values::values::{CONFIGURATION_FILENAME, DOCKER_CONNECTOR_RW};
use ndc_sdk::connector::{ErrorResponse, Result};
use tracing::{debug, event, Level};

/// Retrieves the schema metadata based on the provided configuration and Calcite global reference.
///
/// # Parameters
/// - `configuration`: A mutable reference to the parsed configuration containing metadata.
/// - `calcite_ref`: A global reference to Calcite.
///
/// # Returns
/// A Result containing the SchemaResponse if the schema retrieval was successful, or an error if an issue occurred.
///
/// # Errors
/// Returns an error if there was a problem obtaining the collection information.
///
/// # Panics
/// This function will panic if unable to clone the metadata or if the collection retrieval fails.
///
/// # Example
/// ```
/// use some_module::{get_schema, ParsedConfiguration, GlobalRef};
/// let mut config = ParsedConfiguration::new();
/// let calcite_ref = GlobalRef::default();
/// match get_schema(&mut config, calcite_ref) {
///     Ok(schema) => println!("Schema retrieved successfully: {:?}", schema),
///     Err(e) => eprintln!("Error retrieving schema: {:?}", e),
/// }
/// ```
// ANCHOR: get_schema
#[tracing::instrument(skip(configuration, calcite_ref), level=Level::INFO)]
pub fn get_schema(
    configuration: &ParsedConfiguration,
    calcite_ref: GlobalRef,
) -> Result<SchemaResponse> {
    let mut data_models = configuration.metadata.clone().unwrap_or_default();
    if data_models.is_empty() {
        data_models = get_models(&calcite_ref);
    }
    let scalar_types = scalars::scalars();
    let (object_types, collections) = collections::collections(&data_models, &scalar_types)?;
    let procedures = vec![];
    let functions: Vec<models::FunctionInfo> = vec![];
    let schema = SchemaResponse {
        scalar_types,
        object_types,
        collections,
        functions,
        procedures,
    };

    // Check for CONFIG_FILE_PATH environment variable, fall back to default if not set
    let file_path = match env::var("CONFIG_FILE_PATH") {
        Ok(custom_path) if !custom_path.is_empty() => PathBuf::from(custom_path),
        _ => Path::new(DOCKER_CONNECTOR_RW).join(CONFIGURATION_FILENAME),
    };

    event!(
        Level::INFO,
        config_path = format!("Configuration file path: {}", file_path.display())
    );
    let mut new_configuration = configuration.clone();
    new_configuration.metadata = Some(data_models.clone());
    let file_path_clone = file_path.clone();
    let file = File::create(file_path);
    match file {
        Ok(mut file) => {
            let serialized_json = serde_json::to_string_pretty(&new_configuration)
                .map_err(ErrorResponse::from_error)?;
            file.write_all(serialized_json.as_bytes())
                .map_err(ErrorResponse::from_error)?;
            event!(
                Level::INFO,
                "Wrote metadata to config: {}",
                serde_json::to_string(&schema).unwrap()
            );
        }
        Err(_) => {
            debug!("Unable to create config file: {:?}", file_path_clone);
            event!(
                Level::DEBUG,
                "Unable to create config file {:?}, schema: {:?}",
                file_path_clone,
                serde_json::to_string(&schema).unwrap()
            );
            // Not updating the config file is not fatal
        }
    }
    Ok(schema)
}
// ANCHOR_END: get_schema