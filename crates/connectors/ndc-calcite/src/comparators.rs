//! # Comparison functions.
//!
//! You can create new comparison functions here.
//!
use std::collections::BTreeMap;

use ndc_models::{ComparisonOperatorDefinition, Type};

/// Generate string comparison operators based on the underlying type.
///
/// The function takes an `underlying` string parameter to determine the name of the underlying type.
///
///
/// # Arguments
///
/// * `numeric_comparison_operators`:
///
/// returns: BTreeMap<String, ComparisonOperatorDefinition, Global>
///
/// # Examples
///
/// ```
///
/// ```
// ANCHOR: string_comparators
#[tracing::instrument]
pub fn string_comparators(
    numeric_comparison_operators: &BTreeMap<String, ComparisonOperatorDefinition>,
) -> BTreeMap<String, ComparisonOperatorDefinition> {
    let mut string_comparison_operators = numeric_comparison_operators.clone();
    string_comparison_operators.insert(
        "_like".into(),
        ComparisonOperatorDefinition::Custom {
            argument_type: Type::Named {
                name: "VARCHAR".into(),
            },
        },
    );
    string_comparison_operators
}
// ANCHOR_END: string_comparators

/// Generate numeric comparison operators based on the underlying type.
///
/// The function takes an `underlying` string parameter to determine the name of the underlying type.
///
/// # Arguments
///
/// * `underlying` - The name of the underlying type.
///
/// # Returns
///
/// A `BTreeMap` containing the numeric comparison operators.
///
/// # Example
///
/// ```rust
/// use std::collections::BTreeMap;
/// use std::iter::FromIterator;
/// use tracing::instrument;
///
/// #[derive(Debug)]
/// enum Type {
///     Named {
///         name: String,
///     },
/// }
///
/// #[derive(Debug)]
/// enum ComparisonOperatorDefinition {
///     Equal,
///     In,
///     Custom {
///         argument_type: Type,
///     },
/// }
///
/// #[tracing::instrument]
/// pub fn numeric_comparators(underlying: String) -> BTreeMap<String, ComparisonOperatorDefinition> {
///     // Code here
/// }
/// ```
// ANCHOR: numeric_comparators
#[tracing::instrument]
pub fn numeric_comparators(underlying: String) -> BTreeMap<String, ComparisonOperatorDefinition> {
    let numeric_comparison_operators = BTreeMap::from_iter([
        ("_eq".into(), ComparisonOperatorDefinition::Equal),
        ("_in".into(), ComparisonOperatorDefinition::In),
        (
            "_gt".into(),
            ComparisonOperatorDefinition::Custom {
                argument_type: Type::Named {
                    name: underlying.clone(),
                },
            },
        ),
        (
            "_lt".into(),
            ComparisonOperatorDefinition::Custom {
                argument_type: Type::Named {
                    name: underlying.clone(),
                },
            },
        ),
        (
            "_gte".into(),
            ComparisonOperatorDefinition::Custom {
                argument_type: Type::Named {
                    name: underlying.clone(),
                },
            },
        ),
        (
            "_lte".into(),
            ComparisonOperatorDefinition::Custom {
                argument_type: Type::Named {
                    name: underlying.clone(),
                },
            },
        ),
    ]);
    numeric_comparison_operators
}
// ANCHOR_END: numeric_comparators