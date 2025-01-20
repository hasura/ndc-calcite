//! # Orchestrate Queries
//!
//! Queries are done as nested queries. Relationships are generated as
//! additional queries and then stitched together into the final RowSet rows.
//!
//! Aggregate are generated as additional queries, and stitched into the
//! RowSet aggregates response.
use std::collections::BTreeMap;
use http::StatusCode;
use indexmap::IndexMap;
use models::RowSet;
use ndc_models::{ArgumentName, CollectionName, ComparisonOperatorName, ComparisonTarget, ComparisonValue, Expression, Field, FieldName, Query, Relationship, RelationshipArgument, RelationshipName, RelationshipType, RowFieldValue, VariableName};
use ndc_sdk::connector::error::Result as Result;
use ndc_models as models;
use ndc_sdk::connector::ErrorResponse;
use serde_json::{Number, Value};
use tracing::{event, Level, span};

use ndc_calcite_schema::version5::ParsedConfiguration;

use crate::calcite::{connector_query, Row};
use crate::connector::calcite::CalciteState;
use crate::sql::{self, create_qualified_table_name, generate_aggregate_query, Alias, QualifiedTable, SqlQueryComponents, VariablesCTE};

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
    pub coll_rel: &'a BTreeMap<RelationshipName, Relationship>,
    pub args: &'a BTreeMap<ArgumentName, RelationshipArgument>,
    pub query: &'a Query,
    pub vars: &'a Option<Vec<BTreeMap<VariableName, Value>>>,
    pub state: &'a CalciteState,
    pub explain: &'a bool,
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


/**
Orchestrates a query by parsing query components, processing rows and aggregates,
and generating modified rows based on relationships.

# Arguments

* `query_params` - The query parameters.

# Returns

Returns a `Result` containing a `RowSet` if the query is successful, or a `QueryError` if there's an error.

# Example
```

use ndc_models::Query;
use calcite::connector::calcite::CalciteState;
use calcite::query::{orchestrate_query, QueryParams};
use ndc_calcite_schema::version5::{ParsedConfiguration, Version};

let params = QueryParams {
 config: &ParsedConfiguration { version: Version::This,_schema: None,model: None,model_file_path: None,fixes: None,supports_json_object: None,jars: None,metadata: None,},
 coll: &Default::default(),
 coll_rel: &Default::default(),
 args: &Default::default(),
 query: &Query { aggregates: None,fields: None,limit: None,offset: None,order_by: None,predicate: None,},
 vars: &Default::default(),
 state: &CalciteState { calcite_ref: ()},
 explain: &false,
};

let result = orchestrate_query(query_params);
match result {
    Ok(row_set) => {
        // Process the row set
    }
    Err(query_error) => {
        // Handle the query error
    }
}
```
*/

#[derive(Debug, serde::Serialize)]
pub struct QueryPlan {
    variables_count: Option<usize>,
    aggregate_query: Option<String>,
    row_query: Option<String>,
}

pub fn generate_query_plan(query_params: &QueryParams) -> Result<QueryPlan> {
    let qualified_table = sql::create_qualified_table_name(query_params.table_metadata);
    let query_components = sql::parse_query(
        &query_params.config,
        &qualified_table,
        query_params.query,
        query_params.vars
    ).map_err(|e| ndc_sdk::connector::ErrorResponse::from_error(e))?;

    // Generate aggregate query if needed
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

    // Generate row query using SqlQueryComponents
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
    })
}

pub fn execute_query_plan(
    query_params: QueryParams,
    plan: QueryPlan,
) -> Result<Vec<models::RowSet>> {
    let mut aggregates_response = None;
    let mut rows_data = None;

    // Execute aggregate query if present
    if let Some(aggregate_query) = plan.aggregate_query {
        aggregates_response = Some(connector_query::<Vec<IndexMap<FieldName, serde_json::Value>>>(
            query_params.config,
            query_params.state.clone().calcite_ref,
            &aggregate_query,
            query_params.query,
            query_params.explain
        )?);
    }

    // Execute row query if present
    if let Some(row_query) = plan.row_query {
        rows_data = Some(connector_query::<Vec<Row>>(
            query_params.config,
            query_params.state.clone().calcite_ref,
            &row_query,
            query_params.query,
            query_params.explain
        )?);
    }

    // Group results based on variables if present
    if let Some(vars_count) = plan.variables_count {
        Ok(group_rows_by_variables(rows_data, aggregates_response, vars_count))
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


// Updated orchestrate_query to use the new functions
pub fn orchestrate_query(query_params: QueryParams) -> Result<Vec<models::RowSet>> {
    let plan = generate_query_plan(&query_params)?;
    execute_query_plan(query_params, plan)
}

fn group_rows_by_variables(
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
        let result = group_rows_by_variables(None, None, 2);
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

        let result = group_rows_by_variables(Some(rows), None, 2);
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

        let result = group_rows_by_variables(None, Some(aggregates), 2);
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

        let result = group_rows_by_variables(Some(rows), Some(aggregates), 2);
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

        let result = group_rows_by_variables(Some(rows), None, 2);
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

#[tracing::instrument(skip(params, query_components), level = Level::INFO)]
fn process_rows(params: QueryParams, query_components: &QueryComponents) -> Result<Option<Vec<Row>>> {
    execute_query_collection(params, query_components)
}

#[tracing::instrument(
    fields(internal.visibility = "user"), skip(params, query_components), level = Level::INFO
)]
fn execute_query_collection(
    params: QueryParams,
    query_components: &QueryComponents,
) -> Result<Option<Vec<Row>>> {
    let select = query_components.select.clone();
    if select.is_none() || select.clone().unwrap().is_empty() {
        return Ok(None);
    }

    let span = span!(tracing::Level::INFO, "query_collection_span", collection = params.coll.to_string(), explain = params.explain, internal_visibility="user");

    // Parent span attach to the current context
    let _enter = span.enter();
    match params.query.clone().fields {
        Some(fields) => {
            // Create sub-span for each field attribute
            for field in fields.keys() {
                let sub_span = span!(tracing::Level::INFO, "field_span", field_attribute = format!("{}.{}", params.coll.to_string(), field));
                let _enter_sub_span = sub_span.enter();
            }
        }
        None => {
            // Handle the 'None' case here
        }
    }

    let qualified_table = create_qualified_table_name(params.table_metadata);

    let sql_query_components = SqlQueryComponents {
        with: query_components.variables_cte.as_ref().map(|cte| cte.query.clone()),
        from: (Alias(qualified_table.to_string()), SqlFrom::Table(qualified_table)),
        order_by: query_components.order_by.clone(),
        pagination: query_components.pagination.clone(),
        select: select.clone().unwrap().to_string(),
        where_clause: query_components.predicates.clone(),
        join: query_components.join.clone(),
        group_by: None,
    };

    let sql_query = sql_query_components.to_sql(params.config.supports_json_object);

    connector_query::<Vec<Row>>(
        params.config,
        params.state.clone().calcite_ref,
        &sql_query,
        params.query,
        params.explain).map(Some)
}
