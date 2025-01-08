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
use ndc_models::{ArgumentName, CollectionName, ComparisonOperatorName, ComparisonTarget, ComparisonValue, Expression, Field, FieldName, Query, Relationship, RelationshipArgument, RelationshipName, RelationshipType, RowFieldValue, VariableName};
use ndc_sdk::connector::error::Result;
use ndc_models as models;
use ndc_sdk::connector::ErrorResponse;
use serde_json::{Number, Value};
use tracing::{event, Level, span};

use ndc_calcite_schema::version5::ParsedConfiguration;

use crate::calcite::{connector_query, Row};
use crate::connector::calcite::CalciteState;
use crate::sql;

/// A struct representing the parameters of a query.
///
/// The `QueryParams` struct is used to store various parameters of a query.
/// These parameters include the configuration, the collection, the collection relationships,
/// the relationship arguments, the query model, the variables, and the system state.
///
/// # Generic Parameters
///
/// `'a` - A lifetime parameter specifying the lifetime of the query parameters.
#[derive(Clone, Copy)]
pub struct QueryParams<'a> {
    pub config: &'a ParsedConfiguration,
    pub coll: &'a CollectionName,
    pub coll_rel: &'a BTreeMap<RelationshipName, Relationship>,
    pub args: &'a BTreeMap<ArgumentName, RelationshipArgument>,
    pub query: &'a Query,
    pub vars: &'a Vec<BTreeMap<VariableName, Value>>,
    pub state: &'a CalciteState,
    pub explain: &'a bool,
}

/// A struct representing the components of a query.
///
/// The `QueryComponents` struct is used to store various components of a query.
/// These components include the argument values, the SELECT clause, the ORDER BY clause,
/// the pagination settings, the aggregates, the predicates, the final aggregates, and the join clause.
pub struct QueryComponents {
    pub select: Option<String>,
    pub order_by: Option<String>,
    pub pagination: Option<String>,
    pub aggregates: Option<String>,
    pub predicates: Option<String>,
    pub final_aggregates: String,
    pub join: Option<String>,
}

/// Orchestrates a query by parsing query components, processing rows and aggregates,
/// and generating modified rows based on relationships.
///
/// # Arguments
///
/// * `query_params` - The query parameters.
///
/// # Returns
///
/// Returns a `Result` containing a `RowSet` if the query is successful, or a `QueryError` if there's an error.
///
/// # Example
/// ```
///
/// use ndc_models::Query;
/// use calcite::connector::calcite::CalciteState;
/// use calcite::query::{orchestrate_query, QueryParams};
/// use ndc_calcite_schema::version5::{ParsedConfiguration, Version};
///
/// let params = QueryParams {
///  config: &ParsedConfiguration { version: Version::This,_schema: None,model: None,model_file_path: None,fixes: None,supports_json_object: None,jars: None,metadata: None,},
///  coll: &Default::default(),
///  coll_rel: &Default::default(),
///  args: &Default::default(),
///  query: &Query { aggregates: None,fields: None,limit: None,offset: None,order_by: None,predicate: None,},
///  vars: &Default::default(),
///  state: &CalciteState { calcite_ref: ()},
///  explain: &false,
/// };
///
/// let result = orchestrate_query(query_params);
/// match result {
///     Ok(row_set) => {
///         // Process the row set
///     }
///     Err(query_error) => {
///         // Handle the query error
///     }
/// }
/// ```
#[tracing::instrument(skip(query_params), level = Level::INFO)]
pub fn orchestrate_query(
    query_params: QueryParams
) -> Result<Vec<models::RowSet>> {
    let query_components = sql::parse_query(&query_params.config, query_params.coll, query_params.coll_rel, query_params.query, query_params.vars).map_err(ErrorResponse::from_error)?;
    let mut rows_data: Option<Vec<Row>> = process_rows(query_params, &query_components)?;
    let parsed_aggregates: Option<IndexMap<FieldName, Value>> = process_aggregates(query_params, &query_components)?;
    let query_fields = query_params.query.clone().fields.unwrap_or_default();
    // for (field_name, field_data) in &query_fields {
    //     match &rows_data {
    //         None => {}
    //         Some(rows) => {
    //             match field_data {
    //                 Field::Column { .. } => {}
    //                 Field::Relationship { query, relationship, arguments } => {
    //                     let sub_relationship = query_params.coll_rel.get(relationship).unwrap();
    //                     let (primary_keys, foreign_keys, relationship_type) = parse_relationship(sub_relationship)?;
    //                     let relationship_value = generate_value_from_rows(rows, &sub_relationship)?;
    //                     event!(Level::DEBUG, "Primary Keys: {:?}, Values: {:?}", primary_keys, relationship_value);
    //                     let predicate_expression = generate_predicate(&primary_keys, relationship_value)?;
    //                     event!(Level::DEBUG, "Predicate expression: {:?}", predicate_expression);
    //                     let revised_query = revise_query(query.clone(), predicate_expression, &primary_keys)?;
    //                     let res_relationship_rows = execute_query(query_params.clone(), arguments, &sub_relationship, &revised_query)?;
    //                     if RelationshipType::Object == relationship_type {
    //                         rows_data = process_object_relationship(rows_data.unwrap(), &field_name, &res_relationship_rows, &primary_keys, &foreign_keys)?
    //                     } else {
    //                         rows_data = process_array_relationship(rows_data, &field_name, &res_relationship_rows, &primary_keys, &foreign_keys, &query)?
    //                     }
    //                     event!(Level::DEBUG, "Result of relationship: {:?}", serde_json::to_string_pretty(&rows_data))
    //                 }
    //             }
    //         }
    //     }
    // }

    return Ok(vec![models::RowSet { aggregates: parsed_aggregates, rows: rows_data }]); // FIXME: This is the whole point of this PR.
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
    execute_query_collection(params, query_components, query_components.select.clone())
}


#[tracing::instrument(skip(params, query_components), level = Level::INFO)]
fn process_aggregates(params: QueryParams, query_components: &QueryComponents) -> Result<Option<IndexMap<FieldName, Value>>> {
    match execute_query_collection(params, query_components, query_components.aggregates.clone()) {
        Ok(collection_option) => {
            if let Some(collection) = collection_option {
                let mut row = collection
                    .first()
                    .cloned()
                    .unwrap_or(IndexMap::new());
                let aggregates = params.query.clone().aggregates.unwrap_or_default();
                for (key, _) in aggregates {
                    if !row.contains_key(&key) {
                        row.insert(key.into(), RowFieldValue(Value::from(Number::from(0))));
                    }
                }
                let map: IndexMap<FieldName, Value> = row.into_iter()
                    .map(|(k, v)| (k, v.0))
                    .collect();
                Ok(Some(map))
            } else {
                Ok(None)
            }
        }
        Err(e) => {
            event!(Level::ERROR, "{}", e);
            Err(e)
        }
    }
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
    fields(internal.visibility = "user"), skip(params, query_components, phrase), level = Level::INFO
)]
fn execute_query_collection(
    params: QueryParams,
    query_components: &QueryComponents,
    phrase: Option<String>,
) -> Result<Option<Vec<Row>>> {
    if phrase.is_none() || phrase.clone().unwrap().is_empty() {
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

    let q = sql::query_collection(
        params.config,
        params.coll,
        Some(phrase.unwrap().to_string()),
        query_components.order_by.clone(),
        query_components.pagination.clone(),
        query_components.predicates.clone(),
        query_components.join.clone(),
    );

    match connector_query(
        params.config,
        params.state.clone().calcite_ref,
        &q,
        params.query,
        params.explain) {
        Ok(v) => Ok(Some(v)),
        Err(e) => Err(e)
    }
}
