use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;

use jni::JavaVM;
use jni::objects::GlobalRef;
use tracing::{event, Level};

use ndc_models::{ArgumentInfo, CollectionInfo, ObjectField, ObjectType, SchemaResponse};
use ndc_models as models;

use crate::calcite;

fn numeric_aggregates(underlying_type: &str) -> BTreeMap<String, models::AggregateFunctionDefinition> {
    BTreeMap::from_iter([
        (
            "sum".into(),
            models::AggregateFunctionDefinition {
                result_type: models::Type::Nullable {
                    underlying_type: Box::new(models::Type::Named {
                        name: underlying_type.into(),
                    }),
                },
            },
        ),
        (
            "count".into(),
            models::AggregateFunctionDefinition {
                result_type: models::Type::Nullable {
                    underlying_type: Box::new(models::Type::Named {
                        name: "INTEGER".into(),
                    }),
                },
            },
        ),
        (
            "avg".into(),
            models::AggregateFunctionDefinition {
                result_type: models::Type::Nullable {
                    underlying_type: Box::new(models::Type::Named {
                        name: "DOUBLE".into(),
                    }),
                },
            },
        ),
        (
            "max".into(),
            models::AggregateFunctionDefinition {
                result_type: models::Type::Nullable {
                    underlying_type: Box::new(models::Type::Named {
                        name: underlying_type.into(),
                    }),
                },
            },
        ),
        (
            "min".into(),
            models::AggregateFunctionDefinition {
                result_type: models::Type::Nullable {
                    underlying_type: Box::new(models::Type::Named {
                        name: underlying_type.into(),
                    }),
                },
            },
        ),
    ])
}
fn string_aggregates(underlying_type: &str) -> BTreeMap<String, models::AggregateFunctionDefinition> {
    BTreeMap::from_iter([
        (
            "count".into(),
            models::AggregateFunctionDefinition {
                result_type: models::Type::Nullable {
                    underlying_type: Box::new(models::Type::Named {
                        name: "INTEGER".into(),
                    }),
                },
            },
        ),
        (
            "max".into(),
            models::AggregateFunctionDefinition {
                result_type: models::Type::Nullable {
                    underlying_type: Box::new(models::Type::Named {
                        name: underlying_type.into(),
                    }),
                },
            },
        ),
        (
            "min".into(),
            models::AggregateFunctionDefinition {
                result_type: models::Type::Nullable {
                    underlying_type: Box::new(models::Type::Named {
                        name: underlying_type.into(),
                    }),
                },
            },
        ),
    ])
}
// ANCHOR: get_schema
#[tracing::instrument]
pub async fn get_schema(java_vm: Arc<JavaVM>, calcite_ref: GlobalRef) -> std::result::Result<SchemaResponse, Box<dyn Error>> {
    let env = java_vm.attach_current_thread().unwrap();
    let calcite = env.new_local_ref(calcite_ref).unwrap();
    let data_models = calcite::get_models(calcite, &java_vm);
    let numeric_comparison_operators = BTreeMap::from_iter([
        ("eq".into(), models::ComparisonOperatorDefinition::Equal),
        ("in".into(), models::ComparisonOperatorDefinition::In),
        ("gt".into(), models::ComparisonOperatorDefinition::GreaterThan),
        ("lt".into(), models::ComparisonOperatorDefinition::LessThan),
        ("gte".into(), models::ComparisonOperatorDefinition::GreaterThanOrEqualsTo),
        ("lte".into(), models::ComparisonOperatorDefinition::LessThanOrEqualsTo),
    ]);
    let mut string_comparison_operators = numeric_comparison_operators.clone();
    string_comparison_operators.insert(
            "like".into(),
            models::ComparisonOperatorDefinition::Custom {
                argument_type: models::Type::Named {
                    name: "VARCHAR".into(),
                },
            },
        );
    let _array_arguments: BTreeMap<String, _> = vec![(
        "limit".to_string(),
        ArgumentInfo {
            description: None,
            argument_type: models::Type::Nullable {
                underlying_type: Box::new(models::Type::Named { name: "INTEGER".into() }),
            },
        },
    )]
        .into_iter()
        .collect();

    // ANCHOR: schema_scalar_types
    let scalar_types = BTreeMap::from_iter([
        (
            "CHAR".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::String),
                aggregate_functions: string_aggregates("CHAR"),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "VARCHAR".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::String),
                aggregate_functions: string_aggregates("VARCHAR"),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "VARCHAR(65536)".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::String),
                aggregate_functions: string_aggregates("VARCHAR(65536)"),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "VARCHAR NOT NULL".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::String),
                aggregate_functions: string_aggregates("VARCHAR NOT NULL"),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "JavaType(class java.util.ArrayList)".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::JSON),
                aggregate_functions: BTreeMap::new(),
                comparison_operators: BTreeMap::new(),
            },
        ),
        (
            "JavaType(class java.lang.String)".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::String),
                aggregate_functions: string_aggregates("JavaType(class java.lang.String)"),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "INTEGER".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::Int32),
                aggregate_functions: numeric_aggregates("INTEGER"),
                comparison_operators: numeric_comparison_operators.clone(),
            },
        ),
        (
            "JavaType(class java.lang.Integer)".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::Int32),
                aggregate_functions: numeric_aggregates("INTEGER"),
                comparison_operators: numeric_comparison_operators.clone(),
            },
        ),
        (
            "SMALLINT".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::Int16),
                aggregate_functions: numeric_aggregates("INTEGER"),
                comparison_operators: numeric_comparison_operators.clone(),
            },
        ),
        (
            "TINYINT".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::Int8),
                aggregate_functions: numeric_aggregates("INTEGER"),
                comparison_operators: numeric_comparison_operators.clone(),
            },
        ),
        (
            "BIGINT".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::Int64),
                aggregate_functions: BTreeMap::from_iter([]),
                comparison_operators: numeric_comparison_operators.clone(),
            },
        ),
        (
            "BIGINT NOT NULL".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::Int64),
                aggregate_functions: BTreeMap::from_iter([]),
                comparison_operators: numeric_comparison_operators.clone(),
            },
        ),
        (
            "FLOAT".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::Float32),
                aggregate_functions: numeric_aggregates("DOUBLE"),
                comparison_operators: numeric_comparison_operators.clone(),
            },
        ),
        (
            "JavaType(class java.lang.Double)".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::Float64),
                aggregate_functions: numeric_aggregates("DOUBLE"),
                comparison_operators: numeric_comparison_operators.clone(),
            },
        ),
        (
            "DOUBLE".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::Float64),
                aggregate_functions: numeric_aggregates("DOUBLE"),
                comparison_operators: numeric_comparison_operators.clone(),
            },
        ),
        (
            "DECIMAL".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::BigDecimal),
                aggregate_functions: numeric_aggregates("DOUBLE"),
                comparison_operators: numeric_comparison_operators.clone(),
            },
        ),
        (
            "BOOLEAN".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::Boolean),
                aggregate_functions: BTreeMap::from_iter([]),
                comparison_operators: BTreeMap::from_iter([
                    ("eq".into(), models::ComparisonOperatorDefinition::Equal)
                ]),
            },
        ), (
            "VARBINARY".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::Bytes),
                aggregate_functions: BTreeMap::from_iter([]),
                comparison_operators: BTreeMap::from_iter([]),
            },
        ), (
            "BINARY".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::Bytes),
                aggregate_functions: BTreeMap::from_iter([]),
                comparison_operators: BTreeMap::from_iter([]),
            },
        ),
        (
            "DATE".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::Date),
                aggregate_functions: string_aggregates("DATE"),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "TIME(0)".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::String),
                aggregate_functions: string_aggregates("TIME(0)"),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "TIMESTAMP(0)".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::Timestamp),
                aggregate_functions: string_aggregates("TIMESTAMP(0)"),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "TIMESTAMP(3)".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::Timestamp),
                aggregate_functions: string_aggregates("TIMESTAMP(3)"),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
        (
            "TIMESTAMP".into(),
            models::ScalarType {
                representation: Some(models::TypeRepresentation::TimestampTZ),
                aggregate_functions: string_aggregates("TIMESTAMP"),
                comparison_operators: string_comparison_operators.clone(),
            },
        ),
    ]);
    // ANCHOR_END: scalar_types

    let mut object_types: BTreeMap<String, ObjectType> = BTreeMap::new();
    let mut collections: Vec<CollectionInfo> = Vec::new();
    for (table_name, columns) in &data_models {
        let mut fields: BTreeMap<String, ObjectField> = BTreeMap::new();
        for (column_name, column_type) in columns {
            fields.insert(column_name.into(), ObjectField {
                description: Some("".into()),
                r#type: models::Type::Nullable {
                    underlying_type: Box::new(models::Type::Named { name: column_type.into() })
                },
                arguments: BTreeMap::new(),
            });
        }
        if !scalar_types.contains_key(table_name) {
            object_types.insert(
                table_name.into(), ObjectType {
                    description: Some("".into()),
                    fields: fields.clone(),
                });
            collections.push(CollectionInfo {
                name: table_name.into(),
                description: Some(format!("A collection of {}", table_name)),
                collection_type: table_name.into(),
                arguments: BTreeMap::new(),
                foreign_keys: BTreeMap::from_iter([]),
                uniqueness_constraints: BTreeMap::from_iter([]),
            })
        } else {
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Table names cannot be same as a scalar type name: {}", table_name))));
        }
    }
    let procedures = vec![];
    let functions: Vec<models::FunctionInfo> = vec![];
    let schema = models::SchemaResponse {
        scalar_types,
        object_types,
        collections,
        functions,
        procedures,
    };

    event!(Level::INFO, schema = serde_json::to_string(&schema).unwrap());
    Ok(schema)
}
