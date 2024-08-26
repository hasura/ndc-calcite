//! # Scalar definitions
//!
//! Make additional changes to scalar definitions here.
//!
use std::collections::BTreeMap;

use ndc_models::{ComparisonOperatorDefinition, ScalarType, ScalarTypeName, TypeRepresentation};

use crate::{aggregates, comparators};

/// Retrieves a mapping of scalar types with their respective properties.
///
/// # Example
///
/// ```
/// use std::collections::BTreeMap;
///
/// #[derive(Debug)]
/// pub struct ScalarType {
///     representation: Option<TypeRepresentation>,
///     aggregate_functions: BTreeMap<String, AggregateFunction>,
///     comparison_operators: BTreeMap<String, ComparisonOperatorDefinition>,
/// }
///
/// #[derive(Debug)]
/// pub enum TypeRepresentation {
///     String,
///     JSON,
///     Int32,
///     Float32,
///     Float64,
///     Boolean,
///     Bytes,
///     Date,
///     Timestamp,
///     TimestampTZ,
/// }
///
/// #[derive(Debug)]
/// pub enum AggregateFunction {
///     // definition of aggregate functions
/// }
///
/// #[derive(Debug)]
/// pub enum ComparisonOperatorDefinition {
///     Equal,
///     // definition of other comparison operators
/// }
///
/// /// Retrieves a mapping of scalar types with their respective properties.
/// #[tracing::instrument]
/// pub fn scalars() -> BTreeMap<String, ScalarType> {
///     let string_comparison_operators =
///         comparators::string_comparators(&comparators::numeric_comparators("VARCHAR".into()));
///     let scalar_types = BTreeMap::from_iter([
///         (
///             "CHAR".into(),
///             ScalarType {
///                 representation: Some(TypeRepresentation::String),
///                 aggregate_functions: BTreeMap::new(),
///                 comparison_operators: string_comparison_operators.clone(),
///             },
///         ),
///         // ... all other scalar types ...
///     ]);
///     scalar_types
/// }
/// ```
// ANCHOR: scalars
#[tracing::instrument(skip())]
pub fn scalars() -> BTreeMap<ScalarTypeName, ScalarType> {
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
            "DATETIME".into(),
            ScalarType {
                representation: Some(TypeRepresentation::Timestamp),
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
