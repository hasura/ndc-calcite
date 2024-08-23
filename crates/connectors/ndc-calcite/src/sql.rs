//! # Query Generator
//!
//! Creates the Calcite query statement for a single query.
//!
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use ndc_models::{Aggregate, ArgumentName, CollectionName, ComparisonOperatorName, ComparisonTarget, ComparisonValue, ErrorResponse, ExistsInCollection, Expression, Field, FieldName, Query, Relationship, RelationshipArgument, RelationshipName, UnaryComparisonOperator, VariableName};
use ndc_sdk::connector::QueryError;
use ndc_sdk::models;
use serde_json::{Value};
use tracing::{event, Level};

use ndc_calcite_schema::version5::ParsedConfiguration;
use ndc_calcite_schema::calcite::TableMetadata;
use crate::query::QueryComponents;

#[derive(Debug)]
struct VariableNotFoundError;

impl std::fmt::Display for VariableNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Variable not found")
    }
}

impl Error for VariableNotFoundError {}

#[tracing::instrument]
fn eval_argument(
    variables: &BTreeMap<VariableName, Value>,
    argument: &RelationshipArgument,
) -> Result<Value, QueryError> {
    match argument {
        RelationshipArgument::Variable { name } => variables
            .get(name.as_str())
            .ok_or(QueryError::Other(Box::new(VariableNotFoundError), Value::String(name.to_string())))
            .map(|val| val.clone()),
        RelationshipArgument::Literal { value } => Ok(value.clone()),
        RelationshipArgument::Column { .. } => { todo!() }
    }
}

#[tracing::instrument]
fn select(
    configuration: &ParsedConfiguration,
    variables: &BTreeMap<VariableName, Value>,
    collection: &CollectionName,
    query: &Query,
    collection_relationships: &BTreeMap<RelationshipName, Relationship>,
    prepend: Option<String>,
) -> (Vec<String>, Vec<String>) {
    let mut field_statements: Vec<String> = vec![];
    let join_statements: Vec<String> = vec![];
    let fields = query.fields.clone().unwrap_or_default();
    // let prepend = prepend.unwrap_or_default().clone();
    let table = create_qualified_table_name(
        configuration.clone().metadata.unwrap().get(collection).unwrap()
    );
    for (key, field) in fields {
        match field {
            Field::Column { column, .. } => {
                if configuration.supports_json_object.unwrap_or_else(|| false) {
                    let field_statement = format!("'{}', {}.\"{}\"", key, table, column, );
                    field_statements.push(field_statement);
                } else {
                    let field_statement = format!("{}.\"{}\" AS \"{}\"", table, column, key);
                    field_statements.push(field_statement);
                }
                // TODO: use nested fields???
            }
            Field::Relationship { relationship, .. } => {
                let field_statement = format!("'{}', 1", key);
                field_statements.push(field_statement);
                match collection_relationships.get(&relationship) {
                    None => {}
                    Some(r) => {
                        for (pk, _) in &r.column_mapping {
                            if configuration.supports_json_object.unwrap_or_else(|| false) {
                                let field_statement = format!("'{}', {}.\"{}\"", pk, table, pk, );
                                if !field_statements.contains(&field_statement) {
                                    field_statements.push(field_statement);
                                }
                            } else {
                                let field_statement = format!("{}.\"{}\"", table, pk, );
                                if !field_statements.contains(&field_statement) {
                                    field_statements.push(field_statement);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    return (field_statements, join_statements);
}

#[tracing::instrument]
fn order_by(query: &Query) -> Vec<String> {
    let mut order_statements: Vec<String> = Vec::new();
    match &query.order_by {
        Some(order) => {
            for element in &order.elements {
                let order_direction = serde_json::to_string(&element.order_direction)
                    .expect("Failed to serialize order_direction")
                    .to_uppercase()
                    .replace("\"", "");
                let target = &element.target;
                match target {
                    models::OrderByTarget::Column {
                        name, field_path, ..
                    } => {
                        let field_path = field_path.clone().unwrap_or_default();
                        let mut p: Vec<FieldName> = vec![name.clone()];
                        p.extend(field_path);
                        order_statements.push(format!("\"{}\" {}", p.join("."), order_direction));
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
fn pagination(query: &Query) -> Vec<String> {
    let mut pagination_statements: Vec<String> = Vec::new();
    if query.limit.is_some() {
        pagination_statements.push(format!(" LIMIT {}", query.limit.unwrap()));
    }
    if query.offset.is_some() {
        pagination_statements.push(format!("OFFSET {}", query.offset.unwrap()))
    }
    if pagination_statements.len() == 0 {
        println!("{}","how");
    }
    return pagination_statements;
}

#[tracing::instrument]
fn create_column_name(calcite_configuration: &ParsedConfiguration, name: &FieldName, field_path: &Option<Vec<FieldName>>) -> String {
    match field_path {
        None => name.to_string(),
        Some(f) => {
            format!("{}{}", f.join("."), name)
        }
    }
}

#[tracing::instrument]
fn aggregates(configuration: &ParsedConfiguration, query: &Query) -> Vec<String> {
    let mut aggregates: Vec<String> = Vec::new();
    match &query.aggregates {
        None => {}
        Some(_) => {
            for (name, aggregate) in query.aggregates.as_ref().unwrap() {
                let aggregate_phrase = match aggregate {
                    Aggregate::ColumnCount {
                        column,
                        distinct,
                        field_path,
                    } => {
                        if configuration.supports_json_object.unwrap_or_else(||false) {
                            format!(
                                "'{}', COUNT({}\"{}\")",
                                name,
                                if *distinct { "DISTINCT " } else { "" },
                                create_column_name(configuration, column, field_path),
                            )
                        } else {
                            format!(
                                "COUNT({}\"{}\") AS \"{}\"",
                                if *distinct { "DISTINCT " } else { "" },
                                create_column_name(configuration, column, field_path),
                                name
                            )
                        }
                    }
                    Aggregate::SingleColumn {
                        column,
                        field_path,
                        function,
                    } => {
                        if configuration.supports_json_object.unwrap_or_else(||false) {
                            format!(
                                "'{}', {}(\"{}\")",
                                name,
                                function,
                                create_column_name(configuration, column, field_path),
                            )
                        } else {
                            format!(
                                "{}(\"{}\") AS \"{}\"",
                                function,
                                create_column_name(configuration, column, field_path),
                                name,
                            )
                        }
                    }
                    Aggregate::StarCount {} => {
                        if configuration.supports_json_object.unwrap_or_else(||false) {
                            format!("'{}', COUNT(*)", name)
                        } else {
                            format!("COUNT(*) AS \"{}\"", name)
                        }
                    }
                };
                aggregates.push(aggregate_phrase);
            }
        }
    }
    aggregates
}

#[tracing::instrument]
fn predicates(
    configuration: &ParsedConfiguration,
    collection: &CollectionName,
    collection_relationships: &BTreeMap<RelationshipName, Relationship>,
    variables: &BTreeMap<VariableName, Value>,
    query: &Query,
) -> Result<String, Box<dyn Error>> {
    process_expression_option(configuration, collection, collection_relationships, variables, query.clone().predicate)
}

#[tracing::instrument]
fn process_expression_option(
    configuration: &ParsedConfiguration,
    collection: &CollectionName,
    collection_relationships: &BTreeMap<RelationshipName, Relationship>,
    variables: &BTreeMap<VariableName, Value>,
    predicate: Option<Expression>,
) -> Result<String, Box<dyn Error>> {
    match predicate {
        None => Ok("".into()),
        Some(expr) => process_sql_expression(configuration, collection, collection_relationships, variables, &expr),
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
    return chars.into_iter().collect();
}

#[tracing::instrument]
fn sql_quotes(input: &str) -> String {
    input.replace("'", "\\'").replace("\"", "__UTF8__")
}

#[tracing::instrument]
fn process_sql_expression(
    configuration: &ParsedConfiguration,
    collection: &CollectionName,
    collection_relationships: &BTreeMap<RelationshipName, Relationship>,
    variables: &BTreeMap<VariableName, Value>,
    expr: &Expression,
) -> Result<String, Box<dyn Error>> {
    let table = create_qualified_table_name(
        configuration.clone().metadata.unwrap().get(collection).unwrap()
    );
    let operation_tuples: Vec<(ComparisonOperatorName, String)> = vec![
        ("_gt".into(), ">".into()),
        ("_lt".into(), "<".into()),
        ("_gte".into(), ">=".into()),
        ("_lte".into(), "<=".into()),
        ("_eq".into(), "=".into()),
        ("_in".into(), "IN".into()),
        ("_like".into(), "LIKE".into()),
    ];
    let sql_operations: HashMap<_, _> = operation_tuples.into_iter().collect();
    match expr {
        Expression::And { expressions } => {
            let processed_expressions: Vec<String> = expressions
                .iter()
                .filter_map(|expression| {
                    process_sql_expression(configuration, collection, collection_relationships, variables, expression).ok()
                })
                .collect();
            Ok(format!("({})", processed_expressions.join(" AND ")))
        }
        Expression::Or { expressions } => {
            let processed_expressions: Vec<String> = expressions
                .iter()
                .filter_map(|expression| {
                    process_sql_expression(configuration, collection, collection_relationships, variables, expression).ok()
                })
                .collect();
            Ok(format!("({})", processed_expressions.join(" OR ")))
        }
        Expression::Not { expression } => Ok(format!(
            "(NOT {:?})",
            process_sql_expression(configuration, collection, collection_relationships, variables, expression)
        )),
        Expression::UnaryComparisonOperator { operator, column } => match operator {
            UnaryComparisonOperator::IsNull => {
                match column {
                    ComparisonTarget::Column { name, field_path, .. } => {
                        Ok(format!("\"{}\" IS NULL", create_column_name(&configuration, name, field_path)))
                    }
                    ComparisonTarget::RootCollectionColumn { .. } => {
                        todo!()
                    }
                }
            }
        },
        Expression::BinaryComparisonOperator {
            column,
            operator,
            value,
        } => {
            let sql_operation: &String = sql_operations.get(operator).unwrap();
            let left_side = match column {
                ComparisonTarget::Column {
                    name, field_path, ..
                } => {
                    format!("\"{}\"", create_column_name(&configuration, name, field_path))
                }
                ComparisonTarget::RootCollectionColumn { .. } => {
                    todo!()
                }
            };
            let right_side = match value {
                ComparisonValue::Column { column } => match column {
                    ComparisonTarget::Column {
                        name, field_path, ..
                    } => create_column_name(&configuration, name, field_path),
                    ComparisonTarget::RootCollectionColumn { .. } => {
                        todo!()
                    }
                },
                ComparisonValue::Scalar { value } => {
                    let sql_value = sql_quotes(&sql_brackets(&value.to_string()));
                    if sql_value == "()" {
                        let table = create_qualified_table_name(
                            configuration.clone().metadata.unwrap().get(collection).unwrap()
                        );
                        format!("(SELECT {} FROM {} WHERE FALSE)", left_side, table)
                    } else {
                        if value.is_string() {
                            sql_value
                        } else if value.is_number() {
                            sql_value
                        } else if value.is_object() {
                            sql_value
                        } else if value.is_array() {
                            sql_value
                        } else {
                            sql_value
                        }
                    }
                }
                ComparisonValue::Variable { name } => variables.get(name).unwrap().to_string(),
            };
            Ok(format!("{} {} {}", left_side, sql_operation, right_side))
        }
        Expression::Exists { in_collection, predicate } => {
            match in_collection {
                ExistsInCollection::Related { arguments, relationship } => {
                    let argument_parts = create_arguments(variables, arguments);
                    let root_relationship = collection_relationships.get(relationship).unwrap();
                    let foreign_table = create_qualified_table_name(
                        configuration.clone().metadata.unwrap().get(&root_relationship.target_collection).unwrap()
                    );
                    if let Some(pred_expression) = predicate {
                        let sub_query_clause: Vec<String> = root_relationship
                            .column_mapping
                            .iter()
                            .map(|(source_column, target_column)| format!("{}.\"{}\" = \"{}\"", table, source_column, target_column))
                            .collect();
                        let expression = process_sql_expression(
                            configuration,
                            collection,
                            collection_relationships,
                            variables,
                            pred_expression,
                        ).unwrap();
                        Ok(format!(
                            "EXISTS (SELECT 1 FROM {} WHERE ({} AND {}) {})",
                            foreign_table, sub_query_clause.join(" AND "),
                            expression, argument_parts.join(" ")
                        ))
                    } else {
                        Ok("".into())
                    }
                }
                ExistsInCollection::Unrelated { collection, arguments } => {
                    if let Some(pred_expression) = predicate {
                        let argument_parts = create_arguments(variables, arguments);
                        let expression = process_sql_expression(configuration, collection, collection_relationships, variables, pred_expression).unwrap();
                        let foreign_table = create_qualified_table_name(
                            configuration.clone().metadata.unwrap().get(collection).unwrap()
                        );
                        Ok(format!("EXISTS (SELECT 1 FROM {} WHERE {} {})", foreign_table, expression, argument_parts.join(" ")))
                    } else {
                        Ok("".into())
                    }
                }
            }
        }
    }
}

fn create_arguments(variables: &BTreeMap<VariableName, Value>, arguments: &BTreeMap<ArgumentName, RelationshipArgument>) -> Vec<String> {
    let arguments: Vec<String> = arguments.iter().map(|(name, arg)| {
        let value = match arg {
            RelationshipArgument::Variable { name } => variables.get(name).unwrap().to_string(),
            RelationshipArgument::Literal { value } => value.to_string(),
            RelationshipArgument::Column { name } => format!("\"{}\"", name)
        };
        match name.as_str() {
            "limit" => format!("LIMIT {}", value),
            "offset" => format!("OFFSET {}", value),
            _ => "".to_string()
        }
    }).filter(|arg| !arg.is_empty()).collect();
    arguments
}

fn create_qualified_table_name(table_metadata: &TableMetadata) -> String {
    let mut path: Vec<String> = Vec::new();
    let catalog = table_metadata.clone().catalog.unwrap_or_default();
    let schema = table_metadata.clone().schema.unwrap_or_default();
    let name = table_metadata.clone().name;
    if !catalog.is_empty() {
        path.push(format!("\"{}\"", catalog));
    }
    if !schema.is_empty() {
        path.push(format!("\"{}\"", schema));
    }
    path.push(format!("\"{}\"", name));
    path.join(".")
}

#[tracing::instrument]
pub fn query_collection(
    configuration: &ParsedConfiguration,
    collection_name: &CollectionName,
    _arguments: &BTreeMap<ArgumentName, Value>,
    select: Option<String>,
    order_by: Option<String>,
    pagination: Option<String>,
    where_clause: Option<String>,
    join_clause: Option<String>,
) -> String {
    let select_clause: String = match select {
        None => "".into(),
        Some(select) => {
            if select.is_empty() {
                if configuration.supports_json_object.unwrap_or_else(|| false) {
                    "'CONSTANT', 1".into()
                } else {
                    "1 AS \"CONSTANT\"".into()
                }
            } else {
                select.to_string()
            }
        }
    };

    let order_by_clause = match order_by {
        None => "".into(),
        Some(ord) => {
            if ord.is_empty() {
                "".into()
            } else {
                format!(" ORDER BY {}", ord)
            }
        }
    };

    let expanded_where_clause = match where_clause {
        None => "".to_string(),
        Some(w) => {
            if w.is_empty() {
                "".to_string()
            } else {
                format!(" WHERE {}", w).to_string()
            }
        }
    };

    let pagination_clause = match pagination {
        None => "".into(),
        Some(p) => {
            if p.is_empty() {
                "".into()
            } else {
                format!(" {}", p)
            }
        }
    };

    let join = match join_clause {
        None => "".into(),
        Some(p) => {
            if p.is_empty() {
                "".into()
            } else {
                format!(" {}", p)
            }
        }
    };

    let table = create_qualified_table_name(
        configuration.clone().metadata.unwrap().get(collection_name).unwrap()
    );

    if configuration.supports_json_object.unwrap_or_else(|| false) {
        let query = format!(
            "SELECT JSON_OBJECT({}) FROM {}{}{}{}{}",
            select_clause, table, join, expanded_where_clause, order_by_clause, pagination_clause
        );
        event!(Level::INFO, message = format!("Generated query {}", query));
        query
    } else {
        let query = format!(
            "SELECT {} FROM {}{}{}{}{}",
            select_clause, table, join, expanded_where_clause, order_by_clause, pagination_clause
        );
        event!(Level::INFO, message = format!("Generated query {}", query));
        query
    }
}

pub(crate) fn parse_query<'a>(configuration: &'a ParsedConfiguration, collection: &'a CollectionName, collection_relationships: &'a BTreeMap<RelationshipName, Relationship>, arguments: &'a BTreeMap<ArgumentName, RelationshipArgument>, query: &'a Query, variables: &'a BTreeMap<VariableName, Value>) -> Result<QueryComponents, QueryError> {
    let mut argument_values = BTreeMap::new();
    for (argument_name, argument_value) in arguments {
        if argument_values
            .insert(
                argument_name.clone(),
                eval_argument(variables, argument_value)?,
            )
            .is_some()
        {
            return Err(QueryError::InvalidRequest(ErrorResponse { message: format!(
                "Duplicate argument: {}",
                argument_name
            ),
                details: Value::String(argument_name.to_string()),
            }));
        }
    }
    let (select_clause, join_clause) = select(configuration, variables, collection, query, collection_relationships, None);
    let select = Some(select_clause.join(","));
    let join = Some(join_clause.join(" "));
    let order_by = Some(order_by(query).join(", "));
    let pagination = Some(pagination(query).join(" "));
    let aggregates = Some(aggregates(configuration, query).join(", "));
    let predicates = predicates(&configuration, collection, collection_relationships, variables, query);
    let predicates: Option<String> = match predicates {
        Ok(p) => Some(p),
        Err(e) => {
            return Err(QueryError::InvalidRequest(ErrorResponse {
                message: e.to_string(),
                details: Default::default()
            }));
        }
    };
    let final_aggregates = aggregates.clone().unwrap_or_default();
    Ok(QueryComponents { argument_values, select, order_by, pagination, aggregates, predicates, final_aggregates, join })
}
