//! # Generate NDC Collection metadata
//!
//! Introspect Calcite metadata and then reinterprets it into Calcite metadata.
//!
use std::collections::{BTreeMap, HashMap};
use std::error::Error;

use ndc_models::{CollectionInfo, ForeignKeyConstraint, ObjectField, ObjectType, ScalarType, SchemaResponse, Type, UniquenessConstraint};
use ndc_models::Type::{Named, Nullable};

use crate::configuration::{ColumnMetadata, TableMetadata};

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
pub fn collections(
    data_models: &HashMap<String, TableMetadata>,
    scalar_types: &BTreeMap<String, ScalarType>,
) -> Result<(BTreeMap<String, ObjectType>, Vec<CollectionInfo>), Result<SchemaResponse, Box<dyn Error>>, > {
    let mut object_types: BTreeMap<String, ObjectType> = BTreeMap::new();
    let mut collection_infos: Vec<CollectionInfo> = Vec::new();

    for (table, table_metadata) in data_models {
        let fields = build_fields(&table_metadata.columns);

        if !scalar_types.contains_key(&table_metadata.name) {
            object_types.insert(table.clone(), ObjectType {
                description: table_metadata.description.clone(),
                fields,
            }, );
            let uniqueness_constraints = build_uniqueness_constraints(&table_metadata);
            let foreign_keys = build_foreign_keys(&table_metadata, data_models);
            collection_infos.push(CollectionInfo {
                name: table_metadata.name.clone(),
                description: Some(format!("A collection of {}", table)),
                collection_type: table_metadata.name.clone(),
                arguments: BTreeMap::new(),
                foreign_keys,
                uniqueness_constraints,
            })
        } else {
            return Err(Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Table names cannot be same as a scalar type name: {}",
                    table_metadata.name
                ),
            ))));
        }
    }
    Ok((object_types, collection_infos))
}

#[tracing::instrument]
fn build_fields(column_metadata: &HashMap<String, ColumnMetadata>) -> BTreeMap<String, ObjectField> {
    column_metadata.iter().map(|(column_name, column_metadata)| {
        let scalar_type = column_metadata.scalar_type.clone();
        let nullable = column_metadata.nullable.clone();
        let final_type: Type = if nullable {
            Nullable { underlying_type: Box::new(Named { name: scalar_type }) }
        } else {
            Named { name: scalar_type }
        };
        (column_name.into(),
         ObjectField {
             description: column_metadata.description.clone(),
             r#type: final_type,
             arguments: BTreeMap::new(),
         })
    }).collect()
}

fn build_uniqueness_constraints(tb_metadata: &TableMetadata) -> BTreeMap<String, UniquenessConstraint> {
    let mut uc = BTreeMap::new();
    uc.insert("PK".into(), UniquenessConstraint {
        unique_columns: tb_metadata.primary_keys.clone().unwrap()
    });
    uc
}

fn build_foreign_keys(tb_metadata: &TableMetadata, data_models: &HashMap<String, TableMetadata>) -> BTreeMap<String, ForeignKeyConstraint> {
    let mut constraints: BTreeMap<String, ForeignKeyConstraint> = Default::default();
    for (foreign_table_name, foreign_table_metadata) in data_models {
        for ft in foreign_table_metadata.clone().exported_keys.unwrap_or_default() {
            if ft.fk_table_catalog == tb_metadata.catalog && ft.fk_table_schema == tb_metadata.schema && ft.fk_table_name == tb_metadata.name {
                let pk_table_name = ft.pk_table_name.clone();
                match constraints.get_mut(&pk_table_name) {
                    None => {
                        let mut constraint = ForeignKeyConstraint {
                            column_mapping: Default::default(),
                            foreign_collection: pk_table_name.clone()
                        };
                        constraint.column_mapping.insert(ft.fk_column_name, ft.pk_column_name);
                        constraints.insert(pk_table_name.clone(), constraint);
                    }
                    Some(value) => {
                        value.column_mapping.insert(ft.fk_column_name, ft.pk_column_name);
                    }
                }
            }
        }
    }
    constraints
}


// ANCHOR_END: collections
