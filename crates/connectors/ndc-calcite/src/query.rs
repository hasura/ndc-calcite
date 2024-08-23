//! # Orchestrate Queries
//!
//! Queries are done as nested queries. Relationships are generated as
//! additional queries and then stitched together into the final RowSet rows.
//!
//! Aggregate are generated as additional queries, and stitched into the
//! RowSet aggregates response.
use std::collections::BTreeMap;

use indexmap::IndexMap;
use ndc_models::{ArgumentName, CollectionName, ComparisonOperatorName, ComparisonTarget, ComparisonValue, Expression, Field, FieldName, Query, Relationship, RelationshipArgument, RelationshipName, RelationshipType, RowFieldValue, VariableName};
use ndc_sdk::connector::QueryError;
use ndc_sdk::models;
use serde_json::{Number, Value};

use crate::calcite::{calcite_query, Row};
use ndc_calcite_schema::version5::ParsedConfiguration;
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
    pub vars: &'a BTreeMap<VariableName, Value>,
    pub state: &'a CalciteState,
    pub explain: &'a bool
}

/// A struct representing the components of a query.
///
/// The `QueryComponents` struct is used to store various components of a query.
/// These components include the argument values, the SELECT clause, the ORDER BY clause,
/// the pagination settings, the aggregates, the predicates, the final aggregates, and the join clause.
pub struct QueryComponents {
    pub argument_values: BTreeMap<ArgumentName, Value>,
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
/// * `params` - The query parameters.
///
/// # Returns
///
/// Returns a `Result` containing a `RowSet` if the query is successful, or a `QueryError` if there's an error.
///
/// # Example
/// ```
/// use crate::models::{RowSet, QueryError};
/// use crate::sql;
///
/// let params = QueryParams { ... };
/// let result = orchestrate_query(params);
/// match result {
///     Ok(row_set) => {
///         // Process the row set
///     }
///     Err(error) => {
///         // Handle the query error
///     }
/// }
/// ```
pub fn orchestrate_query(
    params: QueryParams
) -> Result<models::RowSet, QueryError> {
    let components = sql::parse_query(&params.config, params.coll, params.coll_rel, params.args, params.query, params.vars)?;
    let mut rows: Option<Vec<Row>> = process_rows(params, &components)?;
    let aggregates: Option<IndexMap<FieldName, Value>> = process_aggregates(params, &components)?;
    let fields = params.query.clone().fields.unwrap_or_default();
    for (field_name, field) in &fields {
        match &rows {
            None => {}
            Some(r) => {
                match field {
                    Field::Column { .. } => {}
                    Field::Relationship { query, relationship, arguments } => {
                        let sub_relationship = params.coll_rel.get(relationship).unwrap();
                        let (pks, fks, relationship_type) = parse_relationship(sub_relationship)?;
                        let value = generate_value_from_rows(r, &sub_relationship)?;
                        let predicate = generate_predicate(&pks, value)?;
                        let revised_query = revise_query(query.clone(), predicate, &pks)?;
                        let fk_rows = execute_query(params.clone(), arguments, &sub_relationship, &revised_query)?;
                        if RelationshipType::Object == relationship_type {
                            rows = process_object_relationship(rows.unwrap(), &field_name, &fk_rows, &pks, &fks)?
                        } else {
                            rows = process_array_relationship(rows, &field_name, &fk_rows, &pks, &fks, &query)?
                        }
                    }
                }
            }
        }
    }
    return Ok(models::RowSet { aggregates, rows });
}

fn generate_value_from_rows(rows: &Vec<Row>, sub_relationship: &Relationship) -> Result<Value, QueryError> {
    let value: Value = rows.into_iter().map(|row| {
        let mut row_values: Vec<Value> = Vec::new();
        for (fk, _) in sub_relationship.column_mapping.iter() {
            let value = match row.get(fk) {
                Some(v) => v.0.clone(),
                None => Value::Null,
            };
            row_values.push(value);
        }
        if row_values.len() == 1 {
            row_values[0].clone()
        } else {
            Value::Array(row_values)
        }
    }).collect();
    Ok(value)
}

fn parse_relationship(sub_relationship: &Relationship) -> Result<(Vec<&FieldName>, Vec<&FieldName>, RelationshipType), QueryError> {
    let pks: Vec<&FieldName> = sub_relationship.column_mapping.values().collect();
    if pks.len() > 1 {
        return Err(QueryError::Other(Box::from("Cannot create a sub-query based on a composite key"), Value::Null));
    }
    let fks: Vec<&FieldName> = sub_relationship.column_mapping.keys().collect();
    let relationship_type = sub_relationship.relationship_type.clone();
    Ok((pks, fks, relationship_type))
}
fn process_rows(params: QueryParams, query_components: &QueryComponents) -> Result<Option<Vec<Row>>, QueryError> {
    if let Some(phrase) = &query_components.select {
        if phrase.is_empty() && !query_components.final_aggregates.is_empty() {
            return Ok(None);
        }
        let q = sql::query_collection(
            params.config,
            params.coll,
            &query_components.argument_values,
            Some(phrase.to_string()),
            query_components.order_by.clone(),
            query_components.pagination.clone(),
            query_components.predicates.clone(),
            query_components.join.clone(),
        );
        match calcite_query(params.config, params.state.clone().calcite_ref, &q, params.query, params.explain) {
            Ok(value) => Ok(Some(value)),
            Err(e) => Err(e)
        }
    } else {
        Ok(None)
    }
}


fn process_aggregates(params: QueryParams, query_components: &QueryComponents) -> Result<Option<IndexMap<FieldName, Value>>, QueryError> {
    if let Some(phrase) = &query_components.aggregates {
        if phrase.is_empty() {
            return Ok(None);
        }
        let q = sql::query_collection(
            params.config,
            params.coll,
            &query_components.argument_values,
            Some(phrase.to_string()),
            query_components.order_by.clone(),
            query_components.pagination.clone(),
            query_components.predicates.clone(),
            query_components.join.clone(),
        );
        match calcite_query(params.config, params.state.clone().calcite_ref, &q, params.query, params.explain) {
            Ok(collection) => {
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
            }
            Err(_) => todo!(),
        }
    } else {
        Ok(None)
    }
}

fn generate_predicate(pks: &Vec<&FieldName>, value: Value) -> Result<Expression, QueryError> {
    Ok(Expression::BinaryComparisonOperator {
        column: ComparisonTarget::Column {
            name: pks[0].clone(),
            field_path: None,
            path: vec![]
        },
        operator: ComparisonOperatorName::from("_in".to_string()),
        value: ComparisonValue::Scalar { value },
    })
}

fn revise_query(query: Box<Query>, predicate: Expression, pks: &Vec<&FieldName>) -> Result<Box<Query>, QueryError> {
    let mut revised_query = query.clone();
    revised_query.predicate = Some(predicate);
    revised_query.offset = None;
    revised_query.limit = None;
    let mut fields = query.fields.unwrap();
    for pk in pks {
        if !fields.contains_key(*pk) {
            fields.insert(FieldName::from(pk.to_string()), Field::Column {
                column: FieldName::from(pk.to_string()),
                fields: None,
                arguments: Default::default(),
            });
        }
    }
    revised_query.fields = Some(fields);
    Ok(revised_query)
}

fn execute_query(params: QueryParams, arguments: &BTreeMap<ArgumentName, RelationshipArgument>, sub_relationship: &Relationship, revised_query: &Query) -> Result<Vec<Row>, QueryError> {
    let fk_rows = orchestrate_query(QueryParams {
        config: params.config,
        coll: &sub_relationship.target_collection,
        coll_rel: params.coll_rel,
        args: arguments,
        query: revised_query,
        vars: params.vars,
        state: params.state,
        explain: params.explain
    })?;
    Ok(fk_rows.rows.unwrap())
}

fn process_object_relationship(rows: Vec<Row>, field_name: &FieldName, fk_rows: &Vec<Row>, pks: &Vec<&FieldName>, fks: &Vec<&FieldName>) -> Result<Option<Vec<Row>>, QueryError> {
    let modified_rows: Vec<Row> = rows.clone().into_iter().map(|mut row| {
        let pk_value = row.get(fks[0]).unwrap().0.clone();
        let rowset = serde_json::map::Map::new();
        if let Some(value) = row.get_mut(field_name) {
            if let RowFieldValue(_) = *value {
                let mut child_rows: Vec<&Row> = fk_rows.iter().filter(|&x| x.get(pks[0]).unwrap().0 == pk_value).collect();
                if child_rows.len() > 1 {
                    child_rows = vec![child_rows[0]];
                }
                process_child_rows(&child_rows, rowset, value).expect("TODO: panic message");
            }
        }
        row
    }).collect();
    Ok(Some(modified_rows))
}

fn process_array_relationship(rows: Option<Vec<Row>>, field_name: &FieldName, fk_rows: &Vec<Row>, pks: &Vec<&FieldName>, fks: &Vec<&FieldName>, query: &Query) -> Result<Option<Vec<Row>>, QueryError> {
    let modified_rows: Vec<Row> = rows.clone().unwrap().into_iter().map(|mut row| {
        let pk_value = row.get(fks[0]).unwrap().0.clone();
        let rowset = serde_json::map::Map::new();
        let offset = query.offset.unwrap_or(0);
        let limit = query.limit.unwrap_or(0);
        if let Some(value) = row.get_mut(field_name) {
            if let RowFieldValue(_) = *value {
                let mut child_rows: Vec<&Row> = fk_rows.iter().filter(|&x| x.get(pks[0]).unwrap().0 == pk_value).collect();
                if limit > 0 && !child_rows.is_empty() {
                    let max_rows = (offset + limit).min(child_rows.len() as u32);
                    child_rows = child_rows[offset as usize..max_rows as usize].to_vec();
                }
                process_child_rows(&child_rows, rowset, value).expect("TODO: panic message");
            }
        }
        row
    }).collect();
    Ok(Some(modified_rows))
}

fn process_child_rows(child_rows: &Vec<&Row>, mut rowset: serde_json::map::Map<String, Value>, value: &mut RowFieldValue) -> Result<(), QueryError> {
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
