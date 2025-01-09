use ndc_models::{CollectionName, VariableName};
use thiserror::Error;
#[derive(Error, Debug)]
pub(crate) enum Error {
    #[error("Collection {0} not found")]
    CollectionNotFound(CollectionName),
    #[error("Variable {0} not found")]
    VariableNotFound(String),
    #[error("Variable {0} is an object and only scalar values are supported as variable values")]
    UnsupportedVariableObjectValue(VariableName),
    #[error("Variable {0} is an array and only scalar values are supported as variable values")]
    UnsupportedVariableArrayValue(VariableName),
}
