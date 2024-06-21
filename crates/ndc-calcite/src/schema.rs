use std::error::Error;
use jni::objects::GlobalRef;
use ndc_models::SchemaResponse;
use ndc_models as models;
use tracing::{event, Level};

use crate::{calcite, collections, scalars};

// ANCHOR: get_schema
#[tracing::instrument]
pub fn get_schema(calcite_ref: GlobalRef) -> std::result::Result<SchemaResponse, Box<dyn Error>> {
    let data_models = calcite::get_models(calcite_ref);
    let scalar_types = scalars::scalars();
    let (object_types, collections) = match collections::collections(&data_models, &scalar_types) {
        Ok(value) => value,
        Err(value) => return value,
    };
    let procedures = vec![];
    let functions: Vec<models::FunctionInfo> = vec![];
    let schema = models::SchemaResponse {
        scalar_types,
        object_types,
        collections,
        functions,
        procedures,
    };

    event!(Level::INFO, schema = serde_json::to_string(&schema).unwrap());
    Ok(schema)
}
// ANCHOR_END: get_schema