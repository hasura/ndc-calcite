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
#[tracing::instrument(skip(query_params), level = Level::INFO)]
// TODO: Break this function into two separate functions that will generate the
// query plan and execution of the query plan
pub fn orchestrate_query(
    query_params: QueryParams
) -> Result<Vec<models::RowSet>> {

    let mut aggregates_response = None;
    let qualified_table = sql::create_qualified_table_name(query_params.table_metadata);
    let query_components = sql::parse_query(
        &query_params.config,
        &qualified_table,
        query_params.query,
        query_params.vars)
        .map_err(|e| ndc_sdk::connector::ErrorResponse::from_error(e))?;

    if let Some(aggregate_fields) = query_params.query.aggregates.clone() {
        let aggregate_query = generate_aggregate_query(
            query_params.config.supports_json_object,
            &qualified_table,
            query_components.predicates.clone(),
            query_components.pagination.clone(),
            query_components.order_by.clone(),
            &query_components.variables_cte,
            &aggregate_fields
        );

        aggregates_response = Some(connector_query:: <Vec<IndexMap<FieldName,serde_json::Value> > >(query_params.config,query_params.state.clone().calcite_ref, &aggregate_query,query_params.query,query_params.explain)? );

    }

    let rows_data: Option<Vec<Row>> = process_rows(query_params, &query_components)?;
    // let parsed_aggregates: Option<IndexMap<FieldName, Value>> = process_aggregates(query_params, &query_components)?;


    if let Some(vars) = query_params.vars {
            return Ok(group_rows_by_variables(rows_data.unwrap(), aggregates_response, vars.len()));
    } else {
        let aggregate_response_in_rowset = aggregates_response.map(|r| r.first().map(|x| x.clone())).flatten();
        return Ok(vec![models::RowSet { aggregates: aggregate_response_in_rowset, rows: rows_data }]);
    }

}

fn group_rows_by_variables(
    rows: Vec<IndexMap<FieldName, RowFieldValue>>,
    aggregates: Option<Vec<IndexMap<FieldName, Value>>>,
    variables_set_count: usize
) -> Vec<RowSet> {
    // Pre-create result vector with empty RowSets for each variable
    let mut result = Vec::new();

    // Create one RowSet for each variable with empty rows
    for _ in 0..variables_set_count {
        result.push(RowSet {
            aggregates: None,
            rows: Some(Vec::new()),
        });
    }

    // Group rows by var_set_index
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


#[tracing::instrument(skip(rows_data, sub_relationship), level = Level::DEBUG)]
fn generate_value_from_rows(rows_data: &Vec<Row>, sub_relationship: &Relationship) -> Result<Value> {
    let relationship_value: Value = rows_data.into_iter().map(|row| {
        let mut row_values: Vec<Value> = Vec::new();
        for (foreign_key, _) in sub_relationship.column_mapping.iter() {
            let column_value = match row.get(foreign_key) {
                Some(value) => value.0.clone(),
                None => Value::Null,
            };
            row_values.push(column_value);
        }
        if row_values.len() == 1 {
            row_values[0].clone()
        } else {
            Value::Array(row_values)
        }
    }).collect();
    Ok(relationship_value)
}

#[tracing::instrument(skip(sub_relationship), level = Level::DEBUG)]
fn parse_relationship(sub_relationship: &Relationship) -> Result<(Vec<(FieldName, FieldName)>, Vec<&FieldName>, RelationshipType)> {
    let pks: Vec<(FieldName, FieldName)> = sub_relationship.column_mapping
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if pks.len() > 1 {
        return Err(ErrorResponse::new(StatusCode::from_u16(500).unwrap(), "Cannot create a sub-query based on a composite key".to_string(), Value::Null));
    }
    let fks: Vec<&FieldName> = sub_relationship.column_mapping.keys().collect();
    let relationship_type = sub_relationship.relationship_type.clone();
    Ok((pks, fks, relationship_type))
}

#[tracing::instrument(skip(params, query_components), level = Level::INFO)]
fn process_rows(params: QueryParams, query_components: &QueryComponents) -> Result<Option<Vec<Row>>> {
    execute_query_collection(params, query_components)
}


#[tracing::instrument(skip(pks, value), level = Level::INFO)]
fn generate_predicate(pks: &Vec<(FieldName, FieldName)>, value: Value) -> Result<Expression> {
    let (_, name) = pks[0].clone();
    Ok(Expression::BinaryComparisonOperator {
        column: ComparisonTarget::Column {
            name,
            field_path: None,
            path: vec![],
        },
        operator: ComparisonOperatorName::from("_in".to_string()),
        value: ComparisonValue::Scalar { value },
    })
}

#[tracing::instrument(skip(query, predicate, pks), level = Level::INFO)]
fn revise_query(query: Box<Query>, predicate: Expression, pks: &Vec<(FieldName, FieldName)>) -> Result<Box<Query>> {
    let mut revised_query = query.clone();
    revised_query.predicate = Some(predicate);
    revised_query.offset = None;
    revised_query.limit = None;
    let mut fields = query.fields.unwrap();
    for pk in pks {
        let (key, name) = pk;
        if !fields.contains_key(name) {
            fields.insert(FieldName::from(key.to_string()), Field::Column {
                column: FieldName::from(name.to_string()),
                fields: None,
                arguments: Default::default(),
            });
        }
    }
    revised_query.fields = Some(fields);
    Ok(revised_query)
}

// #[tracing::instrument(
//     skip(params, arguments, sub_relationship, revised_query), level = Level::INFO
// )]
// fn execute_query(params: QueryParams, arguments: &BTreeMap<ArgumentName, RelationshipArgument>, sub_relationship: &Relationship, revised_query: &Query) -> Result<Vec<Row>> {
//     let fk_rows = orchestrate_query(QueryParams {
//         config: params.config,
//         coll: &sub_relationship.target_collection,
//         coll_rel: params.coll_rel,
//         args: arguments,
//         query: revised_query,
//         vars: params.vars,
//         state: params.state,
//         explain: params.explain,
//     })?;
//     Ok(fk_rows.rows.unwrap())
// }

#[tracing::instrument(skip(rows, field_name, fk_rows, pks, fks), level = Level::INFO)]
fn process_object_relationship(rows: Vec<Row>, field_name: &FieldName, fk_rows: &Vec<Row>, pks: &Vec<(FieldName, FieldName)>, fks: &Vec<&FieldName>) -> Result<Option<Vec<Row>>> {
    let modified_rows: Vec<Row> = rows.clone().into_iter().map(|mut row| {
        event!(Level::DEBUG, "fk_rows: {:?}, row: {:?}, field_name: {:?}", serde_json::to_string_pretty(&fk_rows), serde_json::to_string_pretty(&row), field_name);
        let pk_value = row.get(fks[0]).unwrap().0.clone();
        let rowset = serde_json::map::Map::new();
        if let Some(value) = row.get_mut(field_name) {
            event!(Level::DEBUG, "value: {:?}", value);
            let (key, name) = pks[0].clone();
            event!(Level::DEBUG, "key: {:?}, name: {:?}", key, name);
            let mut child_rows = Vec::new();
            for x in fk_rows {
                if let Some(value) = x.get(&key) {
                    event!(Level::DEBUG, "value: {:?}", value);
                    if value.0 == pk_value {
                        child_rows.push(x);
                    }
                } else {
                    event!(Level::DEBUG, "value: {:?}", value);
                }
            }
            if child_rows.len() > 1 {
                child_rows = vec![child_rows[0]];
            }
            process_child_rows(&child_rows, rowset, value).expect("TODO: panic message");

        }
        row
    }).collect();
    Ok(Some(modified_rows))
}

#[tracing::instrument(skip(rows, field_name, fk_rows, pks, fks, query), level = Level::DEBUG)]
fn process_array_relationship(rows: Option<Vec<Row>>, field_name: &FieldName, fk_rows: &Vec<Row>, pks: &Vec<(FieldName, FieldName)>, fks: &Vec<&FieldName>, query: &Query) -> Result<Option<Vec<Row>>> {
    let modified_rows: Vec<Row> = rows.clone().unwrap().into_iter().map(|mut row| {
        event!(Level::DEBUG, "fk_rows: {:?}, row: {:?}, field_name: {:?}", serde_json::to_string_pretty(&fk_rows), serde_json::to_string_pretty(&row), field_name);
        let rowset = serde_json::map::Map::new();
        let offset = query.offset.unwrap_or(0);
        let limit = query.limit.unwrap_or(0);
        let pk_value = row.get(fks[0]).unwrap().0.clone();
        if let Some(value) = row.get_mut(field_name) {
            event!(Level::DEBUG, "value: {:?}", value);

                let (key, name) = pks[0].clone();
                event!(Level::DEBUG, "key: {:?}, name: {:?}", key, name);
                let mut child_rows = Vec::new();
                for x in fk_rows {
                    if let Some(sub_value) = x.get(&key) {
                        if sub_value.0 == pk_value {
                            child_rows.push(x);
                        }
                    }
                }
                if limit > 0 && !child_rows.is_empty() {
                    let max_rows = (offset + limit).min(child_rows.len() as u32);
                    child_rows = child_rows[offset as usize..max_rows as usize].to_vec();
                }
                event!(Level::DEBUG, "Key: {:?}, Name: {:?}, Child Rows: {:?}", key, name, child_rows);
                process_child_rows(&child_rows, rowset, value).expect("TODO: panic message");

        }
        row
    }).collect();
    Ok(Some(modified_rows))
}

#[tracing::instrument(skip(child_rows, rowset, value), level = Level::DEBUG)]
fn process_child_rows(child_rows: &Vec<&Row>, mut rowset: serde_json::map::Map<String, Value>, value: &mut RowFieldValue) -> Result<()> {
    rowset.insert("aggregates".to_string(), Value::Null);
    if !child_rows.is_empty() {
        let mut result: Vec<serde_json::map::Map<String, Value>> = vec![];
        for child in child_rows {
            let mut map = serde_json::map::Map::new();
            for (key, value) in *child {
                map.insert(key.to_string(), value.0.clone());
            }
            result.push(map);
        }
        rowset.insert("rows".to_string(), Value::from(result));
        *value = RowFieldValue(Value::from(rowset));
    } else {
        rowset.insert("rows".to_string(), Value::Null);
        *value = RowFieldValue(Value::from(rowset));
    }
    Ok(())
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
