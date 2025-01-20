//! # Orchestrate Queries
//!
//! Queries are done as nested queries. Relationships are generated as
//! additional queries and then stitched together into the final RowSet rows.
//!
//! Aggregate are generated as additional queries, and stitched into the
//! RowSet aggregates response.
use std::collections::BTreeMap;
use indexmap::IndexMap;
use ndc_sdk::models::{self, RowSet};
use ndc_models::{CollectionName, FieldName, Query, RowFieldValue, VariableName};
use ndc_sdk::connector::error::Result as Result;
use serde_json::Value;

use ndc_calcite_schema::version5::ParsedConfiguration;

use crate::calcite::{connector_query, Row};
use crate::connector::calcite::CalciteState;
use crate::sql::{self, generate_aggregate_query, Alias, QualifiedTable, SqlQueryComponents, VariablesCTE};

/// A struct representing the parameters of a query.
///
/// The `QueryParams` struct is used to store various parameters of a query.
/// These parameters include the configuration, the collection, the collection relationships,
/// the relationship arguments, the query model, the variables, and the system state.
///
/// # Generic Parameters
///
/// `'a` - A lifetime parameter specifying the lifetime of the query parameters.
#[derive(Clone, Copy, Debug)]
pub struct QueryParams<'a> {
    pub config: &'a ParsedConfiguration,
    pub table_metadata: &'a ndc_calcite_schema::calcite::TableMetadata,
    pub coll: &'a CollectionName,
    pub query: &'a Query,
    pub vars: &'a Option<Vec<BTreeMap<VariableName, Value>>>,
    pub state: &'a CalciteState,
    pub explain: bool,
}

#[derive(Debug)]
pub enum SqlFrom {
    Table(QualifiedTable),
    SubQuery(Box<SqlQueryComponents>),
}

#[derive(Debug, Clone)]
pub struct ColumnName(pub String);



/// A struct representing the components of a query.
///
/// The `QueryComponents` struct is used to store various components of a query.
/// These components include the argument values, the SELECT clause, the ORDER BY clause,
/// the pagination settings, the aggregates, the predicates, the final aggregates, and the join clause.
#[derive(Debug)]
pub struct QueryComponents {
    pub variables_cte: Option<VariablesCTE>,
    pub select: Option<String>,
    pub order_by: Option<String>,
    pub pagination: Option<String>,
    pub predicates: Option<String>,
    pub join: Option<String>,
}

impl QueryComponents {
    pub fn default() -> Self {
        QueryComponents {
            variables_cte: None,
            select: None,
            order_by: None,
            pagination: None,
            predicates: None,

            join: None,
        }
    }
}



#[derive(Debug)]
pub struct QueryPlan {
    variables_count: Option<usize>,
    pub aggregate_query: Option<String>,
    pub row_query: Option<String>,
    is_explain: bool,
}

pub fn generate_query_plan(
    config: &ParsedConfiguration,
    coll: &CollectionName,
    query: &Query,
    vars: &Option<Vec<BTreeMap<VariableName, Value>>>,
    state: &CalciteState,
    explain: bool,
) -> Result<QueryPlan> {
    // Handle metadata lookup
    let empty_map = std::collections::HashMap::new();
    let metadata_map = config.metadata.as_ref().unwrap_or(&empty_map);
    let table_metadata = metadata_map.get(coll).ok_or(
        ndc_sdk::connector::ErrorResponse::from_error(
            crate::error::Error::CollectionNotFound(coll.clone())
        )
    )?;

    let query_params = QueryParams {
        config,
        table_metadata,
        coll,
        query: &query,
        vars,
        state,
        explain,
    };

    // Rest of the query plan generation logic...
    let qualified_table = sql::create_qualified_table_name(query_params.table_metadata);
    let query_components = sql::parse_query(
        &query_params.config,
        &qualified_table,
        query_params.query,
        query_params.vars
    ).map_err(|e| ndc_sdk::connector::ErrorResponse::from_error(e))?;

    // Generate aggregate query if needed...
    let aggregate_query = if let Some(aggregate_fields) = query_params.query.aggregates.clone() {
        Some(generate_aggregate_query(
            query_params.config.supports_json_object,
            &qualified_table,
            query_components.predicates.clone(),
            query_components.pagination.clone(),
            query_components.order_by.clone(),
            &query_components.variables_cte,
            &aggregate_fields
        ))
    } else {
        None
    };

    // Generate row query...
    let row_query = if let Some(select) = query_components.select.clone() {
        if !select.is_empty() {
            let sql_query_components = SqlQueryComponents {
                with: query_components.variables_cte.as_ref().map(|cte| cte.query.clone()),
                from: (Alias(qualified_table.to_string()), SqlFrom::Table(qualified_table)),
                order_by: query_components.order_by.clone(),
                pagination: query_components.pagination.clone(),
                select: select.to_string(),
                where_clause: query_components.predicates.clone(),
                join: query_components.join.clone(),
                group_by: None,
            };
            Some(sql_query_components.to_sql(query_params.config.supports_json_object))
        } else {
            None
        }
    } else {
        None
    };

    Ok(QueryPlan {
        variables_count: query_params.vars.as_ref().map(|v| v.len()),
        aggregate_query,
        row_query,
        is_explain: explain,
    })
}

pub fn execute_query_plan(
    calcite_reference: jni::objects::GlobalRef,
    plan: QueryPlan,
) -> Result<Vec<models::RowSet>> {
    let mut aggregates_response = None;
    let mut rows_data = None;

    // Execute row query if present
    if let Some(row_query) = plan.row_query {
        rows_data = Some(connector_query::<Vec<Row>>(
            &calcite_reference,
            &row_query,
            plan.is_explain
        )?);
    }

    // Execute aggregate query if present
    if let Some(aggregate_query) = plan.aggregate_query {
        aggregates_response = Some(connector_query::<Vec<IndexMap<FieldName, serde_json::Value>>>(
            &calcite_reference,
            &aggregate_query,
            plan.is_explain
        )?);
    }


    // Group results based on variables if present
    if let Some(vars_count) = plan.variables_count {
        Ok(response_processing_with_variables(rows_data, aggregates_response, vars_count))
    } else {
        let aggregate_response_in_rowset = aggregates_response
            .map(|r| r.first().map(|x| x.clone()))
            .flatten();
        Ok(vec![models::RowSet {
            aggregates: aggregate_response_in_rowset,
            rows: rows_data,
        }])
    }
}

#[derive(Debug)]
pub struct ExplainResponse {
    pub aggregates_explain: Option<String>,
    pub rows_explain: Option<String>,
}

pub fn explain_query_plan(
    calcite_reference: jni::objects::GlobalRef,
    plan: QueryPlan,
) -> Result<ExplainResponse> {
    let mut aggregates_explain = None;
    let mut rows_explain = None;

    // Execute explain row query if present
    if let Some(row_query) = plan.row_query {
        rows_explain = connector_query::<Vec<serde_json::Value>>(
            &calcite_reference,
            &row_query,
            plan.is_explain
        )?.first().map(|x| serde_json::to_string_pretty(x).unwrap());
    }

    // Execute explain aggregate query if present
    if let Some(aggregate_query) = plan.aggregate_query {
        aggregates_explain = connector_query::<Vec<serde_json::Value>>(
            &calcite_reference,
            &aggregate_query,
            plan.is_explain
        )?.first().map(|x| serde_json::to_string_pretty(x).unwrap());
    }

    Ok(ExplainResponse {
        aggregates_explain,
        rows_explain
    })
}

fn response_processing_with_variables(
    rows: Option<Vec<IndexMap<FieldName, RowFieldValue>>>,
    aggregates: Option<Vec<IndexMap<FieldName, Value>>>,
    variables_set_count: usize
) -> Vec<RowSet> {
    // Pre-create result vector with empty RowSets for each variable
    let mut result = Vec::new();

    // Create one RowSet for each variable with empty rows
    for _ in 0..variables_set_count {
        result.push(RowSet {
            aggregates: None,
            rows: None,
        });
    }

    // Group rows by var_set_index if rows are provided
    if let Some(rows) = rows {
        // Initialize rows vectors for all RowSets
        for rowset in result.iter_mut() {
            rowset.rows = Some(Vec::new());
        }

        for row in rows {
            if let Some(&RowFieldValue(Value::Number(ref index))) = row.get("__var_set_index") {
                if let Some(index) = index.as_i64() {
                    if let Some(rowset) = result.get_mut(index as usize) {
                        if let Some(ref mut group_rows) = rowset.rows {
                            let mut clean_row = row.clone();
                            clean_row.swap_remove("__var_set_index");
                            group_rows.push(clean_row);
                        }
                    }
                }
            }
        }
    }

    // Handle aggregates if they exist
    if let Some(aggs) = aggregates {
        for agg in aggs {
            if let Some(Value::Number(index)) = agg.get("__var_set_index").map(|v| v.clone()) {
                if let Some(index) = index.as_i64() {
                    if let Some(rowset) = result.get_mut(index as usize) {
                        let mut clean_agg = agg.clone();
                        clean_agg.swap_remove("__var_set_index");
                        rowset.aggregates = Some(clean_agg);
                    }
                }
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value as JsonValue};


    fn create_test_row(index: i64, field: &str, value: Value) -> IndexMap<FieldName, RowFieldValue> {
        let mut row = IndexMap::new();
        row.insert(
            "__var_set_index".into(),
            RowFieldValue(Value::Number(index.into()))
        );
        row.insert(field.into(), RowFieldValue(value));
        row
    }

    fn create_test_aggregate(index: i64, field: &str, value: Value) -> IndexMap<FieldName, Value> {
        let mut agg = IndexMap::new();
        agg.insert("__var_set_index".into(), Value::Number(index.into()));
        agg.insert(field.into(), value);
        agg
    }

    #[test]
    fn test_empty_inputs() {
        let result = response_processing_with_variables(None, None, 2);
        let json_result: JsonValue = serde_json::to_value(&result).unwrap();

        assert_eq!(json_result, json!([
            {},
            {}
        ]));
    }

    #[test]
    fn test_only_rows() {
        let rows = vec![
            create_test_row(0, "field1", json!("value1")),
            create_test_row(1, "field2", json!("value2")),
            create_test_row(0, "field3", json!("value3")),
        ];

        let result = response_processing_with_variables(Some(rows), None, 2);
        let json_result: JsonValue = serde_json::to_value(&result).unwrap();

        assert_eq!(json_result, json!([
            {
                "rows": [
                    { "field1": "value1" },
                    { "field3": "value3" }
                ]
            },
            {
                "rows": [
                    { "field2": "value2" }
                ]
            }
        ]));
    }

    #[test]
    fn test_only_aggregates() {
        let aggregates = vec![
            create_test_aggregate(0, "sum", json!(100)),
            create_test_aggregate(1, "sum", json!(50)),
        ];

        let result = response_processing_with_variables(None, Some(aggregates), 2);
        let json_result: JsonValue = serde_json::to_value(&result).unwrap();

        assert_eq!(json_result, json!([
            {
                "aggregates": { "sum": 100 },
            },
            {
                "aggregates": { "sum": 50 },
            }
        ]));
    }

    #[test]
    fn test_both_rows_and_aggregates() {
        let rows = vec![
            create_test_row(0, "field1", json!("value1")),
            create_test_row(1, "field2", json!("value2")),
        ];

        let aggregates = vec![
            create_test_aggregate(0, "sum", json!(100)),
            create_test_aggregate(1, "avg", json!(50)),
        ];

        let result = response_processing_with_variables(Some(rows), Some(aggregates), 2);
        let json_result: JsonValue = serde_json::to_value(&result).unwrap();

        assert_eq!(json_result, json!([
            {
                "aggregates": { "sum": 100 },
                "rows": [
                    { "field1": "value1" }
                ]
            },
            {
                "aggregates": { "avg": 50 },
                "rows": [
                    { "field2": "value2" }
                ]
            }
        ]));
    }

    #[test]
    fn test_invalid_index() {
        let rows = vec![
            create_test_row(5, "field1", json!("value1")), // Invalid index
            create_test_row(0, "field2", json!("value2")),
        ];

        let result = response_processing_with_variables(Some(rows), None, 2);
        let json_result: JsonValue = serde_json::to_value(&result).unwrap();

        assert_eq!(json_result, json!([
            {
                "rows": [
                    { "field2": "value2" }
                ]
            },
            {
                "rows": []
            }
        ]));
    }


}
