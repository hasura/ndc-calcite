//! ## Aggregate Function definitions are managed here.
//!
//! - Numerics
//!   * min
//!   * max
//!   * average
//!   * sum
//!
//! - Strings
//!   * min
//!   * max
//!
//! Aggregates could be extended here.

use std::collections::BTreeMap;

use ndc_models::{AggregateFunctionDefinition, AggregateFunctionName, Type};

/// Generates numeric aggregate functions for a given underlying type.
///
/// # Arguments
///
/// * `underlying_type` - A string representing the underlying numeric type.
///
/// # Returns
///
/// A `BTreeMap` containing aggregate function definitions for `sum`, `max`, `avg`, and `min`.
#[tracing::instrument]
pub fn numeric_aggregates(
    underlying_type: &str,
) -> BTreeMap<AggregateFunctionName, AggregateFunctionDefinition> {
    let aggregate_functions: BTreeMap<AggregateFunctionName, AggregateFunctionDefinition> =
        ["sum", "max", "avg", "min"]
            .iter()
            .map(|function| {
                (
                    AggregateFunctionName::from(function.to_string()),
                    aggregate_function_definition(underlying_type),
                )
            })
            .collect();
    BTreeMap::from_iter(aggregate_functions)
}

#[tracing::instrument]
fn aggregate_function_definition(underlying_type: &str) -> AggregateFunctionDefinition {
    AggregateFunctionDefinition {
        result_type: Type::Nullable {
            underlying_type: Box::new(Type::Named {
                name: underlying_type.into(),
            }),
        },
    }
}
