use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use axum::http::StatusCode;
use axum::Json;
use indexmap::IndexMap;
use serde_json::Value;

use ndc_models::{Aggregate, ComparisonTarget, ComparisonValue, ErrorResponse, Expression, RowFieldValue, UnaryComparisonOperator};
use tracing::{event, Level};

use crate::{AppState, calcite, eval_argument, models, Row, sql};

#[tracing::instrument]
pub fn select(query: &models::Query, prepend: Option<String>) -> Vec<String> {
    let mut field_statements: Vec<String> = Vec::new();
    let fields = query.fields.clone().unwrap_or_default();
    let prepend = prepend.unwrap_or_default().clone();
    for (key, field) in fields {
        if let models::Field::Column { fields, column,.. } = field {
            match fields {
                Some(_fields) => todo!(),
                None => {
                    let field_statement = format!("\"{}{}\" AS \"{}\"", prepend, key, column);
                    field_statements.push(field_statement);
                }
            }
        } else {
            // Handle other variants.
        }
    }
    return field_statements;
}

#[tracing::instrument]
pub fn order_by(query: &models::Query) -> Vec<String> {
    let mut order_statements: Vec<String> = Vec::new();
    match &query.order_by {
        Some(order) => {
            for element in &order.elements {
                let order_direction = serde_json::to_string(&element.order_direction)
                    .expect("Failed to serialize order_direction").to_uppercase().replace("\"", "");
                let target = &element.target;
                match target {
                    models::OrderByTarget::Column { name, field_path, .. } => {
                        let field_path = field_path.clone().unwrap_or_default();
                        let mut p: Vec<String> = vec!(name.clone());
                        p.extend(field_path);
                        order_statements.push(format!("{} {}", p.join("."), order_direction));
                    }
                    models::OrderByTarget::SingleColumnAggregate { .. } => todo!(),
                    models::OrderByTarget::StarCountAggregate { .. } => todo!(),
                };
            }
        }
        None => {}
    }
    return order_statements;
}

#[tracing::instrument]
pub fn pagination(query: &models::Query) -> Vec<String> {
    let mut pagination_statements: Vec<String> = Vec::new();
    if query.limit.is_some() {
        pagination_statements.push(format!(" LIMIT {}", query.limit.unwrap()));
    }
    if query.offset.is_some() {
        pagination_statements.push(format!("OFFSET {}", query.offset.unwrap()))
    }
    return pagination_statements;
}

#[tracing::instrument]
fn create_column_name(name: &str, field_path: &Option<Vec<String>>) -> String {
    match field_path {
        None => {
            name.into()
        }
        Some(f) => {
            format!("{}{}", f.join("."), name)
        }
    }
}

#[tracing::instrument]
pub fn aggregates(query: &models::Query) -> Vec<String> {
    let mut aggregates: Vec<String> = Vec::new();
    match &query.aggregates {
        None => {}
        Some(_) => {
            for (name, aggregate) in query.aggregates.as_ref().unwrap() {
                let aggregate_phrase = match aggregate {
                    Aggregate::ColumnCount { column, distinct, field_path } => {
                        format!("COUNT({}\"{}\") AS \"{}\"",
                                if *distinct { "DISTINCT " } else { "" },
                                create_column_name(column, field_path),
                                name)
                    }
                    Aggregate::SingleColumn { column, field_path, function } => {
                        format!("{}({}) AS \"{}\"",
                                function,
                                create_column_name(column, field_path),
                                name)
                    }
                    Aggregate::StarCount {} => {
                        format!("COUNT(*) AS \"{}\"", name)
                    }
                };
                aggregates.push(aggregate_phrase);
            }
        }
    }
    aggregates
}

#[tracing::instrument]
pub fn predicates(collection: &str, variables: &BTreeMap<String, serde_json::Value>, query: &models::Query) -> Result<String, Box<dyn Error>> {
    process_expression_option(collection, variables, query.clone().predicate)
}

#[tracing::instrument]
fn process_expression_option(collection: &str, variables: &BTreeMap<String, serde_json::Value>, predicate: Option<Expression>) -> Result<String, Box<dyn Error>> {
    match predicate {
        None => Ok("".into()),
        Some(expr) =>
            process_expression(collection, variables, &expr)
    }
}

#[tracing::instrument]
fn sql_brackets(input: &str) -> String {
    let mut chars: Vec<char> = input.chars().collect();
    if chars.first() == Some(&'[') && chars.last() == Some(&']') {
        chars[0] = '(';
        let len_minus_one = chars.len() - 1;
        chars[len_minus_one] = ')';
    }
    chars.into_iter().collect()
}

#[tracing::instrument]
fn sql_quotes(input: &str) -> String {
    input.replace("\"", "'").replace("\\'", "\"")
}

#[tracing::instrument]
fn process_expression(collection: &str, variables: &BTreeMap<String, serde_json::Value>, expr: &Expression) -> Result<String, Box<dyn Error>> {
    let operation_tuples: Vec<(String, String)> = vec![
        ("gt".into(), ">".into()),
        ("lt".into(), "<".into()),
        ("gte".into(), ">=".into()),
        ("lte".into(), "<=".into()),
        ("eq".into(), "=".into()),
        ("in".into(), "IN".into()),
        ("like".into(), "IN".into())
    ];
    let sql_operations: HashMap<_, _> = operation_tuples.into_iter().collect();
    match expr {
        Expression::And { expressions } => {
            let processed_expressions: Vec<String> = expressions.iter()
                .filter_map(|expression| process_expression(collection, variables, expression).ok())
                .collect();
            Ok(format!("({})", processed_expressions.join(" AND ")))
        }
        Expression::Or { expressions } => {
            let processed_expressions: Vec<String> = expressions.iter()
                .filter_map(|expression| process_expression(collection, variables, expression).ok())
                .collect();
            Ok(format!("({})", processed_expressions.join(" OR ")))
        }
        Expression::Not { expression } => {
            Ok(format!("(NOT {:?})", process_expression(collection, variables, expression)))
        }
        Expression::UnaryComparisonOperator { operator, column } => {
            match operator {
                UnaryComparisonOperator::IsNull => {
                    Ok(format!("{:?} IS NULL", column))
                }
            }
        },
        Expression::BinaryComparisonOperator { column, operator, value } => {
            // println!("Binary comparison: {:?} {} {:?}", column, operator, value);
            let sql_operation: &String = sql_operations.get(operator).unwrap();
            let left_side = match column {
                ComparisonTarget::Column { name, field_path, .. } => {
                    format!("\"{}\"", create_column_name(name, field_path))
                }
                ComparisonTarget::RootCollectionColumn { .. } => todo!()
            };
            let right_side = match value {
                ComparisonValue::Column { column } => {
                    match column {
                        ComparisonTarget::Column { name,field_path, .. } => {
                            create_column_name(name, field_path)
                        }
                        ComparisonTarget::RootCollectionColumn { .. } => todo!()
                    }
                },
                ComparisonValue::Scalar { value } => {
                    let sql_value = sql_quotes(&sql_brackets(&value.to_string()));
                    if sql_value == "()" {
                        format!("(SELECT {} FROM \"{}\" WHERE FALSE)", left_side, collection)
                    } else {
                        sql_value
                    }
                }
                ComparisonValue::Variable { name } => {
                    variables.get(name).unwrap().to_string()
                }
            };
            Ok(format!("{} {} {}", left_side, sql_operation, right_side))
        }
        Expression::Exists {  predicate, .. } => {
            if let Some(pred_expression) = predicate {
                let expression = process_expression(collection, variables, pred_expression);
                Ok(format!("EXISTS ({:?})", expression))
            } else {
                Ok("".into())
            }
        }
    }
}

// ANCHOR: query_collection
#[tracing::instrument]
pub fn query_collection(
    collection_name: &str,
    _arguments: &BTreeMap<String, serde_json::Value>,
    state: &AppState,
    select: Option<String>,
    order_by: Option<String>,
    pagination: Option<String>,
    where_clause: Option<String>,
    query_metadata: &models::Query,
) -> crate::Result<Vec<Row>> {
    let select_clause: String = match select {
        None => {
            "".into()
        }
        Some(select) => {
            if select.is_empty() {
                "1 AS CONSTANT".into()
            } else {
                select.to_string()
            }
        }
    };

    let order_by_clause = match order_by {
        None => {
            "".into()
        }
        Some(ord) => {
            if ord.is_empty() {
                "".into()
            } else {
                format!(" ORDER BY \"{}\"", ord)
            }
        }
    };

    let expanded_where_clause = match where_clause {
        None => { "".to_string() }
        Some(w) => {
            if w.is_empty() {
                "".to_string()
            } else {
                format!(" WHERE {}", w).to_string()
            }
        }
    };


    let pagination_clause = match pagination {
        None => {
            "".into()
        }
        Some(p) => {
            if p.is_empty() {
                "".into()
            } else {
                format!(" {}", p)
            }
        }
    };

    let query = format!(
        "SELECT {} FROM \"{}\"{}{}{}",
        select_clause,
        collection_name,
        expanded_where_clause,
        order_by_clause,
        pagination_clause
    );
    event!(Level::INFO, message = format!("Generated query {}", query));
    calcite::get_query(
        state.calcite_ref.clone(),
        state.java_vm.clone(),
        &query,
        &query_metadata
    )
}
// ANCHOR_END: query_collection

// ANCHOR: sql_query_with_variables
pub fn query_with_variables(
    collection: &str,
    arguments: &BTreeMap<String, models::Argument>,
    query: &models::Query,
    variables: &BTreeMap<String, serde_json::Value>,
    state: &AppState,
) -> crate::Result<models::RowSet> {
    // ANCHOR_END: execute_query_with_variables_signature
    let mut argument_values = BTreeMap::new();

    for (argument_name, argument_value) in arguments {
        if argument_values
            .insert(
                argument_name.clone(),
                eval_argument(variables, argument_value)?,
            )
            .is_some()
        {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    message: "duplicate argument names".into(),
                    details: serde_json::Value::Null,
                }),
            ));
        }
    }

    let select = Some(sql::select(query, None).join(", "));
    let order_by= Some(sql::order_by(query).join(", "));
    let pagination = Some(sql::pagination(query).join(" "));
    let aggregates = Some(sql::aggregates(query).join(", "));
    let predicates = sql::predicates(collection, variables, query);
    let predicates: Option<String> = match predicates {
        Ok(p) => { Some(p) }
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    message: format!("error in predicates: {:?}", e),
                    details: serde_json::Value::Null,
                })
            ));
        }
    };
    let final_aggregates = aggregates.clone().unwrap_or_default();

    let rows: Option<Vec<Row>> = match select {
        None => {
            None
        }
        Some(phrase) => {
            if phrase.is_empty() && !final_aggregates.is_empty(){
                None
            } else {
                match sql::query_collection(
                    collection,
                    &argument_values,
                    state,
                    Some(phrase),
                    order_by.clone(),
                    pagination.clone(),
                    predicates.clone(),
                    &query
                ) {
                    Ok(collection) => {
                        Some(collection)
                    }
                    Err(_) => todo!()
                }
            }
        }
    };


    let aggregates: Option<IndexMap<String, serde_json::Value>> = match aggregates {
        None => {
            None
        }
        Some(phrase) => {
            if phrase.is_empty() {
                None
            } else {
                match sql::query_collection(
                    collection,
                    &argument_values,
                    state,
                    Some(phrase),
                    order_by.clone(),
                    pagination.clone(),
                    predicates.clone(),
                    &query
                ) {
                    Ok(collection) => {
                        let mut row = collection.first().cloned().unwrap_or(IndexMap::new());
                        let aggregates = query.clone().aggregates.unwrap_or_default();
                        for (key, _) in aggregates {
                            if !row.contains_key(&key) {
                                row.insert(key.into(), RowFieldValue(Value::Number(serde_json::Number::from(0))));
                            } else if row.get(&key) == None {
                                row.swap_remove(&key);
                                row.insert(key.into(), RowFieldValue(Value::Number(serde_json::Number::from(0))));
                            }
                        }

                        row.into_iter().map(|(k, v)| {
                            let value = match v {
                                RowFieldValue(x) => {x}
                            };
                            Some((k, value))
                        }).collect()
                    }
                    Err(_) => todo!()
                }
            }
        }
    };

    event!(Level::DEBUG, message=serde_json::to_string_pretty(&models::RowSet { aggregates: aggregates.clone(), rows: rows.clone() }).unwrap());
    return Ok(models::RowSet { aggregates, rows });
}
// ANCHOR_END: sql_query_with_variables