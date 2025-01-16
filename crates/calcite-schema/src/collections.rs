//! # Generate NDC Collection metadata
//!
//! Introspect Calcite metadata and then reinterprets it into Calcite metadata.
//!
use std::collections::{BTreeMap, HashMap};

use http::StatusCode;
use ndc_models::Type::{Named, Nullable};
use ndc_models::{CollectionInfo, CollectionName, FieldName, ForeignKeyConstraint, ObjectField, ObjectType, ObjectTypeName, ScalarType, ScalarTypeName, Type, TypeName, UniquenessConstraint};
use ndc_sdk::connector::{Result, ErrorResponse};
use serde_json::Value;
use tracing::Level;

use crate::calcite::{ColumnMetadata, TableMetadata};

/// Extracts information from data models and scalar types to generate object types and collection information.
///
/// # Arguments
///
/// * `data_models` - A reference to a hashmap containing data models, where the key is the table name and the value is a hashmap of column names and their types.
/// * `scalar_types` - A reference to a BTreeMap containing scalar types, where the key is the type name and the value is the scalar type definition.
///
/// # Returns
///
/// A Result that contains either a tuple with the generated object types and collection information, or an error indicating an issue with the input data.
///
/// The generated object types are stored in a BTreeMap, where the key is the table name and the value is an ObjectType struct.
///
/// The collection information is stored in a vector of CollectionInfo structs.
///
/// An inner Result can also be returned, which contains an error indicating an issue with the input data.
// ANCHOR: collections
#[tracing::instrument(skip(data_models, scalar_types), level=Level::INFO)]
pub fn collections(
    data_models: &HashMap<CollectionName, TableMetadata>,
    scalar_types: &BTreeMap<ScalarTypeName, ScalarType>,
) -> Result<(BTreeMap<ObjectTypeName, ObjectType>, Vec<CollectionInfo>)> {
    let mut object_types: BTreeMap<ObjectTypeName, ObjectType> = BTreeMap::new();
    let mut collection_infos: Vec<CollectionInfo> = Vec::new();

    for (table, table_metadata) in data_models {
        let fields = build_fields(&table_metadata.columns);

        let tbl_name = ScalarTypeName::new(table_metadata.name.clone().into());
        if !scalar_types.contains_key(&tbl_name) {
            object_types.insert(ObjectTypeName::new(table.to_string().into()), ObjectType {
                description: table_metadata.description.clone(),
                fields,
            }, );
            let uniqueness_constraints = build_uniqueness_constraints(&table_metadata);
            let foreign_keys = build_foreign_keys(&table_metadata, data_models);
            collection_infos.push(CollectionInfo {
                name: CollectionName::new(table_metadata.name.clone().parse().unwrap()),
                description: Some(format!("A collection of {}", table)),
                collection_type: ObjectTypeName::new(TypeName::from(table_metadata.name.clone())),
                arguments: BTreeMap::new(),
                foreign_keys,
                uniqueness_constraints,
            })
        } else {
            return Err(ErrorResponse::new(StatusCode::from_u16(500).unwrap(),
                format!(
                    "Table names cannot be same as a scalar type name: {}",
                    table_metadata.name
                ), Value::Null
            ));
        }
    }
    Ok((object_types, collection_infos))
}

#[tracing::instrument(skip(column_metadata), level=Level::INFO)]
fn build_fields(column_metadata: &HashMap<FieldName, ColumnMetadata>) -> BTreeMap<FieldName, ObjectField> {
    column_metadata.iter().map(|(column_name, column_metadata)| {
        let scalar_type = TypeName::from(column_metadata.scalar_type.clone());
        let nullable = column_metadata.nullable.clone();
        let final_type: Type = if nullable {
            Nullable { underlying_type: Box::new(Named { name: scalar_type }) }
        } else {
            Named { name: scalar_type }
        };
        (column_name.clone(),
         ObjectField {
             description: column_metadata.description.clone(),
             r#type: final_type,
             arguments: BTreeMap::new(),
         })
    }).collect()
}

#[tracing::instrument(skip(table_metadata), level=Level::INFO)]
fn build_uniqueness_constraints(table_metadata: &TableMetadata) -> BTreeMap<String, UniquenessConstraint> {
    let mut unique_constraints = BTreeMap::new();
    unique_constraints.insert("PK".into(), UniquenessConstraint {
        unique_columns: table_metadata.primary_keys.clone().unwrap().iter().map(|s| FieldName::new(s.clone().parse().unwrap())).collect()
    });
    unique_constraints
}

#[tracing::instrument(skip(table_metadata, models_data_map), level=Level::INFO)]
fn build_foreign_keys(table_metadata: &TableMetadata, models_data_map: &HashMap<CollectionName, TableMetadata>) -> BTreeMap<String, ForeignKeyConstraint> {
    let mut foreign_key_constraints: BTreeMap<String, ForeignKeyConstraint> = Default::default();

    for (_, foreign_metadata) in models_data_map {
        for foreign_key in foreign_metadata.clone().exported_keys.unwrap_or_default() {
            if foreign_key.fk_table_catalog == table_metadata.physical_catalog
                && foreign_key.fk_table_schema == table_metadata.physical_schema
                && foreign_key.fk_table_name == table_metadata.name {

                let primary_table_name = foreign_key.pk_table_name.clone();

                match foreign_key_constraints.get_mut(&primary_table_name) {
                    // If there was no key exists in the map, insert the new one
                    None => {
                        let mut new_constraint = ForeignKeyConstraint {
                            column_mapping: Default::default(),
                            foreign_collection: CollectionName::from(primary_table_name.clone())
                        };

                        new_constraint.column_mapping.insert(FieldName::from(foreign_key.fk_column_name), FieldName::from(foreign_key.pk_column_name));
                        foreign_key_constraints.insert(primary_table_name.clone(), new_constraint);
                    }
                    // If the key already exists, simply update it
                    Some(existing_constraint) => {
                        existing_constraint.column_mapping.insert(FieldName::from(foreign_key.fk_column_name), FieldName::from(foreign_key.pk_column_name));
                    }
                }
            }
        }
    }

    return foreign_key_constraints;
}

// ANCHOR_END: collections
