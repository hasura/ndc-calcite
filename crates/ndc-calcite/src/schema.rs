use std::error::Error;

use jni::objects::GlobalRef;
use ndc_models as models;
use ndc_models::SchemaResponse;
use tracing::{event, Level};

use crate::{calcite, collections, scalars};

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
/// use crate::models::GlobalRef;
/// use crate::models::SchemaResponse;
/// use crate::models::FunctionInfo;
/// use crate::scalars;
/// use crate::collections;
/// use crate::calcite;
///
/// fn main() -> Result<(), Box<dyn Error>> {
///     // Initialize the Calcite reference
///     let calcite_ref = GlobalRef::new();
///
///     // Get the schema
///     let schema = get_schema(calcite_ref)?;
///
///     // Print the schema
///     println!("Schema: {:?}", schema);
///
///     Ok(())
/// }
/// ```
// ANCHOR: get_schema
#[tracing::instrument]
pub fn get_schema(calcite_ref: GlobalRef) -> Result<SchemaResponse, Box<dyn Error>> {
    let data_models = calcite::get_models(calcite_ref);
    let scalar_types = scalars::scalars();
    let (object_types, collections) = match collections::collections(&data_models, &scalar_types) {
        Ok(value) => value,
        Err(value) => return value,
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

    event!(
        Level::INFO,
        schema = serde_json::to_string(&schema).unwrap()
    );
    Ok(schema)
}
// ANCHOR_END: get_schema
