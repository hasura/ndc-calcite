use ndc_models::{CollectionName, ComparisonOperatorName, VariableName};
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
    #[error("Local relationships are not yet supported")]
    RelationshipsAreNotSupported,
    #[error("Operator {0} is not supported")]
    OperatorNotSupported(ComparisonOperatorName),
    #[error("Could not parse Calcite explain response: {0}")]
    CouldNotParseCalciteExplainResponse(serde_json::Error),
    #[error("No rows found in Calcite explain response")]
    FoundNoRowsInCalciteExplainResponse,
    #[error("Nested collections are not supported")]
    NestedCollectionNotSupported,
}
