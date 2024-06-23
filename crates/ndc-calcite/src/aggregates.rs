use std::collections::BTreeMap;

use ndc_models::{AggregateFunctionDefinition, Type};

// ANCHOR: numeric_aggregates
#[tracing::instrument]
pub fn numeric_aggregates(
    underlying_type: &str,
) -> BTreeMap<String, AggregateFunctionDefinition> {
    let aggregate_functions: BTreeMap<String, AggregateFunctionDefinition> =
        ["sum", "max", "avg", "min"]
            .iter()
            .map(|function| {
                (
                    function.to_string(),
                    aggregate_function_definition(underlying_type),
                )
            })
            .collect();
    BTreeMap::from_iter(aggregate_functions)
}
// ANCHOR_END: numeric_aggregates

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
