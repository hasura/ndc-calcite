use std::collections::BTreeMap;

use ndc_models::{ComparisonOperatorDefinition, ScalarType, TypeRepresentation};

use crate::{aggregates, comparators};

// ANCHOR: scalars
#[tracing::instrument]
pub fn scalars() -> BTreeMap<String, ScalarType> {
    let string_comparison_operators =
        comparators::string_comparators(&comparators::numeric_comparators("VARCHAR".into()));
    let scalar_types = BTreeMap::from_iter([
        (
            "CHAR".into(),
            ScalarType {
                representation: Some(TypeRepresentation::String),
                aggregate_functions: BTreeMap::new(),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "VARCHAR".into(),
            ScalarType {
                representation: Some(TypeRepresentation::String),
                aggregate_functions: BTreeMap::new(),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "JSON".into(),
            ScalarType {
                representation: Some(TypeRepresentation::JSON),
                aggregate_functions: BTreeMap::new(),
                comparison_operators: BTreeMap::new(),
            },
        ),
        (
            "LIST".into(),
            ScalarType {
                representation: Some(TypeRepresentation::JSON),
                aggregate_functions: BTreeMap::new(),
                comparison_operators: BTreeMap::new(),
            },
        ),
        (
            "MAP".into(),
            ScalarType {
                representation: Some(TypeRepresentation::JSON),
                aggregate_functions: BTreeMap::new(),
                comparison_operators: BTreeMap::new(),
            },
        ),
        (
            "INTEGER".into(),
            ScalarType {
                representation: Some(TypeRepresentation::Int32),
                aggregate_functions: aggregates::numeric_aggregates("DOUBLE"),
                comparison_operators: comparators::numeric_comparators("INTEGER".into()),
            },
        ),
        (
            "BIGINT".into(),
            ScalarType {
                representation: Some(TypeRepresentation::String),
                aggregate_functions: BTreeMap::from_iter([]),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "FLOAT".into(),
            ScalarType {
                representation: Some(TypeRepresentation::Float32),
                aggregate_functions: aggregates::numeric_aggregates("DOUBLE"),
                comparison_operators: comparators::numeric_comparators("FLOAT".into()),
            },
        ),
        (
            "DOUBLE".into(),
            ScalarType {
                representation: Some(TypeRepresentation::Float64),
                aggregate_functions: aggregates::numeric_aggregates("DOUBLE"),
                comparison_operators: comparators::numeric_comparators("DOUBLE".into()),
            },
        ),
        (
            "DECIMAL".into(),
            ScalarType {
                representation: Some(TypeRepresentation::String),
                aggregate_functions: BTreeMap::from_iter([]),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "BOOLEAN".into(),
            ScalarType {
                representation: Some(TypeRepresentation::Boolean),
                aggregate_functions: BTreeMap::from_iter([]),
                comparison_operators: BTreeMap::from_iter([(
                    "_eq".into(),
                    ComparisonOperatorDefinition::Equal,
                )]),
            },
        ),
        (
            "VARBINARY".into(),
            ScalarType {
                representation: Some(TypeRepresentation::Bytes),
                aggregate_functions: BTreeMap::from_iter([]),
                comparison_operators: BTreeMap::from_iter([]),
            },
        ),
        (
            "BINARY".into(),
            ScalarType {
                representation: Some(TypeRepresentation::Bytes),
                aggregate_functions: BTreeMap::from_iter([]),
                comparison_operators: BTreeMap::from_iter([]),
            },
        ),
        (
            "DATE".into(),
            ScalarType {
                representation: Some(TypeRepresentation::Date),
                aggregate_functions: BTreeMap::new(),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "TIME".into(),
            ScalarType {
                representation: Some(TypeRepresentation::String),
                aggregate_functions: BTreeMap::new(),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "TIMESTAMP".into(),
            ScalarType {
                representation: Some(TypeRepresentation::Timestamp),
                aggregate_functions: BTreeMap::new(),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "TIMESTAMPTZ".into(),
            ScalarType {
                representation: Some(TypeRepresentation::TimestampTZ),
                aggregate_functions: BTreeMap::new(),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
    ]);
    scalar_types
}
// ANCHOR_END: scalars
