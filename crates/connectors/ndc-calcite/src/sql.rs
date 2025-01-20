//! # Query Generator
//!
//! Creates the Calcite query statement for a single query.
//!
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use crate::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use ndc_models::{OrderByTarget, Aggregate, ArgumentName, CollectionName, ComparisonOperatorName, ComparisonTarget, ComparisonValue, ExistsInCollection, Expression, Field, FieldName, Query, Relationship, RelationshipArgument, RelationshipName, UnaryComparisonOperator, VariableName};
use ndc_sdk::connector::ErrorResponse;
use serde_json::Value;
use tracing::{event, Level};

use ndc_calcite_schema::version5::ParsedConfiguration;
use ndc_calcite_schema::calcite::TableMetadata;
use crate::query::{QueryComponents, SqlFrom};

const NOT_FOUND_MSG: &str = "Variable not found";

const HASURA_CTE_VARS: &str = "hasura_cte_vars";


// #[tracing::instrument(skip(variables, argument), level=Level::DEBUG)]
// fn eval_argument(
//     variables: &BTreeMap<VariableName, Value>,
//     argument: &RelationshipArgument,
// ) -> Result<Value, Error> {
//     match argument {
//         RelationshipArgument::Variable { name } => variables
//             .get(name.as_str())
//             .ok_or(ErrorResponse::from_error(Error::VariableNotFound))
//             .map(|val| val.clone()),
//         RelationshipArgument::Literal { value } => Ok(value.clone()),
//         RelationshipArgument::Column { .. } => { todo!() }
//     }
// }

// TODO: Separate the SQL operations according to the type of the column, e.g. string, number, etc.
// TODO: Have a single source of truth for the SQL operations, that's the same thing
// should be used in the Calcite schema generation and the SQL query generation.
static SQL_OPERATIONS: Lazy<HashMap<ComparisonOperatorName, String>> = Lazy::new(|| {
    vec![
    ("_gt".into(), ">".into()),
    ("_lt".into(), "<".into()),
    ("_gte".into(), ">=".into()),
    ("_lte".into(), "<=".into()),
    ("_eq".into(), "=".into()),
    ("_in".into(), "IN".into()),
    ("_like".into(), "LIKE".into()),
    ].into_iter().collect()
});

#[tracing::instrument(skip(supports_json_object, alias, item, table), level=Level::DEBUG)]
fn get_field_statement(supports_json_object: bool, alias: &FieldName, item: &FieldName, table: &QualifiedTable) -> String {
    if supports_json_object {
        format!("'{}', {}.\"{}\"", alias, table, item)
    } else {
        format!("{}.\"{}\" AS \"{}\"", table, item, alias) // TODO: Don't use `t`
    }
}

fn value_to_sql(variable_name: &VariableName, value: &Value) -> Result<String, Error> {
    match value {
        Value::Null => Ok("NULL".to_string()),
        Value::Bool(b) => Ok(if *b { "TRUE" } else { "FALSE" }.to_string()),
        Value::Number(n) => Ok(format!("{}", n)), // Uses Display trait directly to format number
        Value::String(s) => Ok(format!("'{}'", s.replace('\'', "''"))),
        Value::Array(_) => Err(Error::UnsupportedVariableArrayValue(variable_name.clone())),
        Value::Object(_) => Err(Error::UnsupportedVariableObjectValue(variable_name.clone())),
    }
}

#[derive(Debug)]
pub struct VariablesCTE {
    pub query: String,
    pub columns: Vec<VariableName>,
}

fn generate_cte_vars(vars: &Vec<BTreeMap<VariableName, Value>>) -> Result<VariablesCTE, Error> {

    // Collect all unique column names while preserving order
    let mut columns: BTreeSet<VariableName> = BTreeSet::new();
    for row in vars {
        for name in row.keys() {
            columns.insert(name.clone());
        }
    }

    let columns: Vec<VariableName> = columns.into_iter().collect();

    let mut cte = format!("WITH \"{HASURA_CTE_VARS}\" AS (\n");

    let mut selects = Vec::new();

    // Iterate with enumeration to get the index
    for (idx, row) in vars.iter().enumerate() {
        let mut pairs = Vec::new();
        // Add the index column
        pairs.push(format!("{} AS \"__var_set_index\"", idx));

        for (name, value) in row {
            let sql_value = value_to_sql(name, value)?;
            pairs.push(format!("{} AS \"{}\"", sql_value, name));
        }
        selects.push(format!("    SELECT {}", pairs.join(", ")));
    }

    cte.push_str(&selects.join("\n    UNION ALL\n"));
    cte.push_str("\n)");

    Ok(VariablesCTE { query: cte, columns })
}


#[tracing::instrument(skip(
   variables_cte,
), level=Level::DEBUG)]
fn select(
    variables_cte: &Option<VariablesCTE>,
    table: &QualifiedTable,
    does_supports_json_object: bool,
    fields: &IndexMap<FieldName, Field>,
) -> Result<Vec<String>, Error> {
    let mut field_statements: Vec<String> = vec![];

    if variables_cte.is_some() {
        field_statements.push(format!("{HASURA_CTE_VARS}.__var_set_index"));
    }

    for (key, field) in fields {
        match field {
            Field::Column { column, .. } => {
                let field_statement = get_field_statement(does_supports_json_object, &key, &column, &table);
                if !field_statements.contains(&field_statement) {
                    field_statements.push(field_statement);
                }
            }
            Field::Relationship { .. } => {
                return Err(Error::RelationshipsAreNotSupported);
            }
        }
    }

    Ok(field_statements)

}

#[tracing::instrument(skip(query), level=Level::DEBUG)]
fn order_by(query: &Query) -> Vec<String> {
    let mut order_statements: Vec<String> = Vec::new();
    match &query.order_by {
        Some(order) => {
            for element in &order.elements {
                let order_direction = serde_json::to_string(&element.order_direction)
                    .expect("Failed to serialize order_direction") // TODO: Handle error
                    .to_uppercase()
                    .replace("\"", "");
                let target = &element.target;
                match target {
                    OrderByTarget::Column {
                        name, field_path, ..
                    } => {
                        let field_path = field_path.clone().unwrap_or_default();
                        let mut p: Vec<FieldName> = vec![name.clone()];
                        p.extend(field_path);
                        order_statements.push(format!("\"{}\" {}", p.join("."), order_direction));
                    }
                    OrderByTarget::SingleColumnAggregate { .. } => todo!(),
                    OrderByTarget::StarCountAggregate { .. } => todo!(),
                };
            }
        }
        None => {}
    }
    return order_statements;
}

#[tracing::instrument(skip(query), level=Level::DEBUG)]
fn pagination(query: &Query) -> Vec<String> {
    let mut pagination_statements: Vec<String> = Vec::new();
    if let Some(limit) = query.limit {
        pagination_statements.push(format!(" LIMIT {}", limit));
    }
    if let Some(offset) = query.offset {
        pagination_statements.push(format!("OFFSET {}", offset));
    }
    if pagination_statements.is_empty() {
        event!(Level::DEBUG, "No pagination.");
    }
    pagination_statements
}

#[tracing::instrument(skip(name, field_path), level=Level::DEBUG)]
fn create_column_name(name: &FieldName, field_path: &Option<Vec<FieldName>>) -> String {
    match field_path {
        None => name.to_string(),
        Some(f) => {
            format!("{}{}", f.join("."), name)
        }
    }
}

#[tracing::instrument(skip(name, aggregate_expr), level=Level::DEBUG)]
fn generate_aggregate_statement(name: &FieldName, aggregate_expr: String, supports_json_object: bool) -> String {
    if supports_json_object {
        format!("'{}', {}", name, aggregate_expr)
    } else {
        format!("{} AS \"{}\"", aggregate_expr, name)
    }
}

#[tracing::instrument(skip(column, field_path), level=Level::DEBUG)]
fn aggregate_column_name(supports_json_object: bool, column: &FieldName, field_path: &Option<Vec<FieldName>>) -> String {
    let column_name = create_column_name(column, field_path);
     if supports_json_object {
        format!("\"{}\"", column_name)
     } else {
         column_name
     }
}


#[tracing::instrument(skip(aggregate_fields), level=Level::DEBUG)]
fn aggregates(supports_json_object: bool, aggregate_fields: &IndexMap<FieldName, Aggregate>, column_qualifier: String) -> Vec<String> {
    let mut aggregates: Vec<String> = Vec::new();
    let mut columns_to_select: HashSet<&FieldName> = HashSet::new();
    let qualify_column = |column: &str| format!("\"{}\".\"{}\"", column_qualifier, column);
    for (name, aggregate) in aggregate_fields.iter() {
        let aggregate_expr = match aggregate {
            Aggregate::ColumnCount { column, distinct, field_path } => {
                let column_name = aggregate_column_name(supports_json_object, column, field_path);
                columns_to_select.insert(column);
                format!("COUNT({}{})", if *distinct { "DISTINCT " } else { "" }, qualify_column( &column_name))
            }
            Aggregate::SingleColumn { column, field_path, function } => {
                let column_name = aggregate_column_name(supports_json_object, column, field_path);

                columns_to_select.insert(column);
                // TODO: We need to validate the function name here
                format!("{}({})", function, qualify_column (&column_name))
            }
            Aggregate::StarCount {} => format!("COUNT(*)"),
        };
        let aggregate_phrase = generate_aggregate_statement(name, aggregate_expr, supports_json_object);
        aggregates.push(aggregate_phrase);
    }

    aggregates
}

#[derive(Debug, Clone)]
pub struct Alias(pub String);

impl Display for Alias {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }

}

impl From<&str> for Alias {
    fn from(s: &str) -> Self {
        Alias(s.to_string())
    }
}


impl From<FieldName> for Alias {
    fn from(s: FieldName) -> Self {
        Alias(s.to_string())
    }
}


#[tracing::instrument(skip(configuration, variables, query), level=Level::DEBUG)]
fn predicates(
    configuration: &ParsedConfiguration,
    table: &QualifiedTable,
    variables: &Option<VariablesCTE>,
    query: &Query,
) -> Result<String, Error> {
    if let Some(predicate) = &query.predicate {
        process_sql_expression(configuration, table, variables, predicate)
    } else {
        Ok("".into())
    }
}

#[tracing::instrument(skip(input), level=Level::DEBUG)]
fn sql_brackets(input: &str) -> String {
    let mut chars: Vec<char> = input.chars().collect();
    if chars.first() == Some(&'[') && chars.last() == Some(&']') {
        chars[0] = '(';
        let len_minus_one = chars.len() - 1;
        chars[len_minus_one] = ')';
    }
    return chars.into_iter().collect();
}

#[tracing::instrument(skip(input), level=Level::DEBUG)]
fn sql_quotes(input: &str) -> String {
    input.replace("'", "\\'").replace("\"", "__UTF8__")
}



#[tracing::instrument(skip(
    configuration,
    variables,
    expr
), level=Level::DEBUG)]
fn process_sql_expression(
    configuration: &ParsedConfiguration,
    table: &QualifiedTable,
    variables: &Option<VariablesCTE>,
    expr: &Expression,
) -> Result<String, crate::error::Error> {

    match expr {
        Expression::And { expressions } => {
            let processed_expressions: Vec<String> = expressions
                .iter()
                .filter_map(|expression| {
                    process_sql_expression(configuration, table, variables, expression).ok()
                })
                .collect();
            Ok(format!("({})", processed_expressions.join(" AND ")))
        }
        Expression::Or { expressions } => {
            let processed_expressions: Vec<String> = expressions
                .iter()
                .filter_map(|expression| {
                    process_sql_expression(configuration, table, variables, expression).ok()
                })
                .collect();
            Ok(format!("({})", processed_expressions.join(" OR ")))
        }
        Expression::Not { expression } => {
            let embed_expression = process_sql_expression(configuration, table, variables, expression)?;
            Ok(format!("(NOT {})", embed_expression))
        },
        Expression::UnaryComparisonOperator { operator, column } => match operator {
            UnaryComparisonOperator::IsNull => {
                match column {
                    ComparisonTarget::Column { name, field_path, .. } => {
                        Ok(format!("\"{}\" IS NULL", create_column_name(name, field_path)))
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
            let sql_operation: &String =
                SQL_OPERATIONS
                .get(operator)
                .ok_or(crate::error::Error::OperatorNotSupported(operator.clone()))?;
            let left_side = match column {
                ComparisonTarget::Column {
                    name, field_path, ..
                } => {
                    format!("\"{}\"", create_column_name( name, field_path))
                }
                ComparisonTarget::RootCollectionColumn { .. } => {
                    todo!()
                }
            };
            let right_side = match value {
                ComparisonValue::Column { column } => match column {
                    ComparisonTarget::Column {
                        name, field_path, ..
                    } => create_column_name(name, field_path),
                    ComparisonTarget::RootCollectionColumn { .. } => {
                        todo!()
                    }
                },
                ComparisonValue::Scalar { value } => {
                    let sql_value = sql_quotes(&sql_brackets(&value.to_string()));
                    if sql_value == "()" {
                        format!("(SELECT {} FROM {} WHERE FALSE)", left_side, table)
                    } else {
                        sql_value
                    }
                }
                ComparisonValue::Variable { name } => {
                    let variable = variables
                        .as_ref()
                        .ok_or(Error::VariableNotFound(name.to_string()))?
                        .columns
                        .iter()
                        .find(|&var| var == name)
                        .ok_or(Error::VariableNotFound(name.to_string()))?;
                    format!("\"{HASURA_CTE_VARS}\".\"{}\"", variable)
                },

            };
            Ok(format!("{} {} {}", left_side, sql_operation, right_side))
        }
        Expression::Exists { in_collection, predicate } => {
            // match in_collection {
            //     ExistsInCollection::Related { arguments, relationship } => {
            //         let argument_parts = create_arguments(variables, arguments);
            //         let root_relationship = collection_relationships.get(relationship).unwrap();
            //         let foreign_table = create_qualified_table_name(
            //             configuration.clone().metadata.unwrap().get(&root_relationship.target_collection).unwrap()
            //         );
            //         if let Some(pred_expression) = predicate {
            //             let sub_query_clause: Vec<String> = root_relationship
            //                 .column_mapping
            //                 .iter()
            //                 .map(|(source_column, target_column)| format!("{}.\"{}\" = \"{}\"", table, source_column, target_column))
            //                 .collect();
            //             let expression = process_sql_expression(
            //                 configuration,
            //                 collection,
            //                 collection_relationships,
            //                 variables,
            //                 pred_expression,
            //             ).unwrap();
            //             Ok(format!(
            //                 "EXISTS (SELECT 1 FROM {} WHERE ({} AND {}) {})",
            //                 foreign_table, sub_query_clause.join(" AND "),
            //                 expression, argument_parts.join(" ")
            //             ))
            //         } else {
            //             Ok("".into())
            //         }
            //     }
            //     ExistsInCollection::Unrelated { collection, arguments } => {
            //         if let Some(pred_expression) = predicate {
            //             let argument_parts = create_arguments(variables, arguments);
            //             let expression = process_sql_expression(configuration, collection, collection_relationships, variables, pred_expression).unwrap();
            //             let foreign_table = create_qualified_table_name(
            //                 configuration.clone().metadata.unwrap().get(collection).unwrap()
            //             );
            //             Ok(format!("EXISTS (SELECT 1 FROM {} WHERE {} {})", foreign_table, expression, argument_parts.join(" ")))
            //         } else {
            //             Ok("".into())
            //         }
            //     }
            //     ExistsInCollection::NestedCollection { .. } => {
            //         Ok("".into())
            //     }
            // }
            todo!()
        }
    }
}

#[tracing::instrument(skip_all, level=Level::DEBUG)]
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

#[derive(Debug, Clone)]
pub struct QualifiedTable(String);

impl Display for QualifiedTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[tracing::instrument(skip(table_metadata), level=Level::DEBUG)]
pub fn create_qualified_table_name(table_metadata: &TableMetadata) -> QualifiedTable {
    let mut path: Vec<String> = Vec::new();

    if let Some(catalog) = &table_metadata.catalog {
        if !catalog.is_empty() {
            path.push(format!("\"{}\"", catalog));
        }
    }

    if let Some(schema) = &table_metadata.schema {
        if !schema.is_empty() {
            path.push(format!("\"{}\"", schema));
        }
    }

    path.push(format!("\"{}\"", &table_metadata.name));
    QualifiedTable( path.join("."))
}

#[derive(Debug)]
pub struct SqlQueryComponents {
    pub with: Option<String>,
    pub select: String,
    pub from: (Alias, SqlFrom),
    pub join: Option<String>,
    pub where_clause: Option<String>,
    pub order_by: Option<String>,
    pub pagination: Option<String>,
    pub group_by: Option<String>,
}

impl SqlQueryComponents {
    pub fn to_sql(&self, supports_json_object: bool) -> String {
        let with_clause = match &self.with {
            None => "".into(),
            Some(with) => {
                if with.is_empty() {
                    "".into()
                } else {
                    with.clone()
                }
            }
        };

        let from_clause = match &self.from.1 {
            SqlFrom::Table(table) => table.to_string(), // TODO: Handle the alias
            SqlFrom::SubQuery(subquery) => format!("({}) AS {}", subquery.to_sql(supports_json_object), self.from.0),
        };

        let select_clause = if self.select.is_empty() {
            if supports_json_object {
                "'CONSTANT', 1".into()
            } else {
                "1 AS \"CONSTANT\"".into()
            }
        } else {
            self.select.clone()
        };

        let order_by_clause = match &self.order_by {
            None => "".into(),
            Some(ord) => {
                if ord.is_empty() {
                    "".into()
                } else {
                    format!(" ORDER BY {}", ord)
                }
            }
        };

        let group_by_clause = match &self.group_by {
            None => "".into(),
            Some(ord) => {
                if ord.is_empty() {
                    "".into()
                } else {
                    format!(" GROUP BY {}", ord)
                }
            }
        };


        let expanded_where_clause = match &self.where_clause {
            None => "".to_string(),
            Some(w) => {
                if w.is_empty() {
                    "".to_string()
                } else {
                    format!(" WHERE {}", w).to_string()
                }
            }
        };

        let format_clause = |clause: &Option<String>| {
            match clause {
                None => "".into(),
                Some(p) => {
                    if p.is_empty() {
                        "".into()
                    } else {
                        format!(" {}", p)
                    }
                }
            }
        };



        let pagination_clause = format_clause(&self.pagination);
        let join = format_clause(&self.join);

        if supports_json_object {
            format!(
                "SELECT JSON_OBJECT({}) FROM {}{}{}{}{}",
                select_clause, from_clause, join, expanded_where_clause, order_by_clause, pagination_clause
            )
        } else {
            format!(
                "{} SELECT {} FROM {}{}{}{}{}{}",
                with_clause, select_clause, from_clause, join, expanded_where_clause, order_by_clause, pagination_clause,group_by_clause
            )
        }
    }
}

// TODO: Remove this! `SqlQueryComponents`'s `to_sql` does what this function does.
#[tracing::instrument(level=Level::INFO)]
fn generate_fields_query(
    supports_json_object: bool,
    from: String,
    with: Option<String>,
    select: Option<String>,
    order_by: Option<String>,
    pagination: Option<String>,
    where_clause: Option<String>,
    join_clause: Option<String>,
) -> String {
    let with_clause = match with {
        None => "".into(),
        Some(with) => {
            if with.is_empty() {
                "".into()
            } else {
                with
            }
        }
    };


    let select_clause: String = match select {
        None => "".into(),
        Some(select) => {
            if select.is_empty() {
                if supports_json_object {
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

    let format_clause = |clause: Option<String>| {
        match clause {
            None => "".into(),
            Some(p) => {
                if p.is_empty() {
                    "".into()
                } else {
                    format!(" {}", p)
                }
            }
        }
    };

    let pagination_clause = format_clause(pagination);
    let join = format_clause(join_clause);

    if supports_json_object {
        let query = format!(
            "SELECT JSON_OBJECT({}) FROM {}{}{}{}{}",
            select_clause, from, join, expanded_where_clause, order_by_clause, pagination_clause
        );
        event!(Level::INFO, message = format!("Generated query {}", query));
        query
    } else {
        let query = format!(
            "{} SELECT {} FROM {}{}{}{}{}",
            with_clause, select_clause, from, join, expanded_where_clause, order_by_clause, pagination_clause
        );
        event!(Level::INFO, message = format!("Generated query {}", query));
        query
    }
}



/*
WITH "vars" AS (
  SELECT 0 as "__var_set_index", 1 as "AlbumId"
  UNION ALL
  SELECT 1 as "__var_set_index", 2 as "AlbumId"
)
SELECT
  "t"."__var_set_index",
  COUNT("t".*) AS "how_many_albums",
  COUNT("t"."ArtistId") AS "how_many_artist_ids",
  COUNT(DISTINCT "t"."ArtistId") AS "how_many_distinct_artist_ids",
  MIN("t"."ArtistId") AS "min_artist_id",
  MAX("t"."ArtistId") AS "max_artist_id",
  AVG("t"."ArtistId") AS "avg_artist_id"
FROM (
  SELECT "a"."ArtistId", "vars"."__var_set_index"
  FROM "albums" "a"
  CROSS JOIN "vars"
  WHERE "a"."AlbumId" > "vars"."AlbumId"
) "t"
GROUP BY "t"."__var_set_index"
*/
pub fn generate_aggregate_query<'a>(
    supports_json_object: bool,
    table: &QualifiedTable,
    predicate: Option<String>,
    pagination: Option<String>,
    order_by: Option<String>,
    variables_cte: &Option<VariablesCTE>,
    aggregate_fields: &'a IndexMap<FieldName, Aggregate>
) -> String {

    let mut columns_to_select_in_subquery = HashSet::new();
    for (_, aggregate) in aggregate_fields {
        match aggregate {
            Aggregate::ColumnCount { column, .. } => {
                columns_to_select_in_subquery.insert(column.clone());
            }
            Aggregate::SingleColumn { column, .. } => {
                // TODO(KC): We need to handle `function_name` here
                columns_to_select_in_subquery.insert(column.clone());
            }
            Aggregate::StarCount {} => {}
        }
    }

    let mut subquery_select = columns_to_select_in_subquery.iter().map(|column| {
         get_field_statement(supports_json_object, column.into(), column, table)
    }).collect::<Vec<String>>();

    let mut join_clause = None;

    if variables_cte.is_some() {
        join_clause = Some(format!("CROSS JOIN \"{HASURA_CTE_VARS}\""));
        subquery_select.push(format!("\"{HASURA_CTE_VARS}\".\"__var_set_index\""));

    };

    let aggregates_subquery = SqlQueryComponents {
        with: None,
        select: subquery_select.join(", "),
        from: (Alias(table.to_string()), SqlFrom::Table(table.clone())),
        join: join_clause,
        where_clause: predicate,
        order_by,
        pagination,
        group_by: None,
    };

    let mut aggregates_group_by = None;
    if variables_cte.is_some() {
        aggregates_group_by = Some("\"aggregates_subquery\".\"__var_set_index\"".to_string());
    }

    let mut aggregates = aggregates(supports_json_object, aggregate_fields, "aggregates_subquery".to_string());


    if variables_cte.is_some() {
        aggregates_group_by = Some("\"aggregates_subquery\".\"__var_set_index\"".to_string());
        aggregates.push("\"aggregates_subquery\".\"__var_set_index\"".to_string());
    }




    let aggregates_query = SqlQueryComponents {
        with: variables_cte.as_ref().map(|vars| vars.query.clone()),
        select: aggregates.join(", "),
        from: (Alias("aggregates_subquery".to_string()), (SqlFrom::SubQuery(Box::new(aggregates_subquery)))),
        join: None,
        where_clause: None,
        order_by: None,
        pagination: None,
        group_by: aggregates_group_by,
    };

    aggregates_query.to_sql(supports_json_object)
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;

    fn setup_test_table() -> QualifiedTable {
        QualifiedTable("public.users".to_string())
    }

    fn normalize_sql(sql: &str) -> String {
        sql.split_whitespace().collect::<Vec<&str>>().join(" ")
    }

    fn assert_sql_eq(actual: &str, expected: &str) {
        assert_eq!(normalize_sql(actual), normalize_sql(expected));
    }

    #[test]
    fn test_simple_count_star() {
        let table = setup_test_table();
        let mut aggregates = IndexMap::new();
        aggregates.insert(
            "total_count".into(),
            Aggregate::StarCount {},
        );

        let result = generate_aggregate_query(
            false,
            &table,
            None,
            None,
            None,
            &None,
            &aggregates
        );

        assert_eq!(
            result.trim(),
            r#"SELECT "aggregates_subquery".COUNT(*) AS "total_count" FROM ( SELECT 1 AS "CONSTANT" FROM public.users)"#.trim()
        );
    }

    #[test]
    fn test_distinct_column_count_with_predicate() {
        let table = setup_test_table();
        let mut aggregates = IndexMap::new();
        aggregates.insert(
            "active_users".into(),
            Aggregate::ColumnCount {
                column: "status".into(),
                field_path: None,
                distinct: true,
            },
        );

        let result = generate_aggregate_query(
            false,
            &table,
            Some("status = 'active'".to_string()),
            None,
            None,
            &None,
            &aggregates
        );

        assert_sql_eq(
            result.trim(),
            r#"SELECT COUNT(DISTINCT "aggregate_subquery"."status") AS "active_users"
            FROM (
                SELECT public.users."status" as "status"
                FROM public.users
                WHERE status = 'active'
            ) AS aggregates_subquery"#.trim()
        );
    }

    #[test]
    fn test_single_column_aggregate() {
        let table = setup_test_table();
        let mut aggregates = IndexMap::new();
        aggregates.insert(
            "avg_age".into(),
            Aggregate::SingleColumn {
                column: "age".into(),
                field_path: None,
                function: "avg".into(),
            },
        );

        let result = generate_aggregate_query(
            false,
            &table,
            None,
            None,
            None,
            &None,
            &aggregates
        );

        assert_eq!(
            result.trim(),
            r#"SELECT AVG(age) AS avg_age
            FROM (SELECT age FROM public.users) AS aggregates_subquery"#.trim()
        );
    }

    #[test]
    fn test_with_variables_cte() {
        let table = setup_test_table();
        let mut aggregates = IndexMap::new();
        aggregates.insert(
            "total_count".into(),
            Aggregate::StarCount {},
        );

        let variables_cte = Some(VariablesCTE {
            query: "WITH vars AS (SELECT 1 AS var_index)".to_string(),
            columns: vec!["var_index".into()],
        });

        let result = generate_aggregate_query(
            false,
            &table,
            None,
            None,
            None,
            &variables_cte,
            &aggregates
        );

        assert_eq!(
            result.trim(),
            r#"WITH vars AS (SELECT 1 AS var_index)
            SELECT COUNT(*) AS total_count
            FROM (
                SELECT *
                FROM public.users
                CROSS JOIN "hasura_vars"
            ) AS aggregates_subquery
            GROUP BY aggregates_subquery.__var_set_index"#.trim()
        );
    }

    // #[test]
    // fn test_multiple_aggregates() {
    //     let table = setup_test_table();
    //     let query = setup_basic_query();
    //     let mut aggregates = IndexMap::new();

    //     aggregates.insert(
    //         "total_users".to_string(),
    //         Aggregate::StarCount {},
    //     );
    //     aggregates.insert(
    //         "avg_age".to_string(),
    //         Aggregate::SingleColumn {
    //             column: "age".to_string(),
    //             field_path: None,
    //             function: "avg".to_string(),
    //         },
    //     );
    //     aggregates.insert(
    //         "unique_statuses".to_string(),
    //         Aggregate::ColumnCount {
    //             column: "status".to_string(),
    //             field_path: None,
    //             distinct: true,
    //         },
    //     );

    //     let result = generate_aggregate_query(
    //         false,
    //         &table,
    //         &query,
    //         None,
    //         None,
    //         None,
    //         &None,
    //         &aggregates,
    //     );

    //     assert_eq!(
    //         result.trim(),
    //         r#"SELECT
    //             COUNT(*) AS total_users,
    //             AVG(age) AS avg_age,
    //             COUNT(DISTINCT status) AS unique_statuses
    //         FROM (
    //             SELECT age, status
    //             FROM public.users
    //         ) AS aggregates_subquery"#.trim()
    //     );
    // }
}

// write tests for generate_aggregate_query



#[tracing::instrument(skip(
    configuration,
    query,
    variables
), level=Level::DEBUG)]
pub fn parse_query<'a>(
    configuration: &'a ParsedConfiguration,
    qualified_table: &'a QualifiedTable,
    query: &'a Query,
    variables: &'a Option<Vec<BTreeMap<VariableName, Value>>>,
) -> Result<QueryComponents, Error> {


    let variables_cte = match variables {
        Some(v) => Some(generate_cte_vars(&v)?),
        None => None,
    };

    let predicates = predicates(&configuration, &qualified_table, &variables_cte, query)?;

    let mut query_components = QueryComponents::default();


    if let Some(fields) = &query.fields {
        let select_clause = select(&variables_cte, &qualified_table, configuration.supports_json_object, &fields)?;

        let join_clause = if variables_cte.is_some() {
            Some(format!("CROSS JOIN \"{HASURA_CTE_VARS}\""))
        } else {
            None
        };

        query_components.select = Some(select_clause.join(","));
        query_components.join = join_clause;
        query_components.variables_cte = variables_cte;

        query_components.order_by = Some(order_by(query).join(", "));
        query_components.pagination = Some(pagination(query).join(" "));
        query_components.predicates = Some(predicates);


    }

    Ok(query_components)
}
