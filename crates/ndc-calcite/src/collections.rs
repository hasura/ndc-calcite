use std::collections::{BTreeMap, HashMap};
use std::error::Error;

use ndc_models::{CollectionInfo, ObjectField, ObjectType, ScalarType, SchemaResponse};
use ndc_models::Type::{Named, Nullable};
use crate::metadata::TableMetadata;

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
#[tracing::instrument]
pub fn collections(
    data_models: &HashMap<String, TableMetadata>,
    scalar_types: &BTreeMap<String, ScalarType>,
) -> Result<
    (BTreeMap<String, ObjectType>, Vec<CollectionInfo>),
    Result<SchemaResponse, Box<dyn Error>>,
> {
    let mut object_types: BTreeMap<String, ObjectType> = BTreeMap::new();
    let mut collections: Vec<CollectionInfo> = Vec::new();
    for (table, table_metadata) in data_models {
        let mut fields: BTreeMap<String, ObjectField> = BTreeMap::new();
        for (column_name, column_metadata) in &table_metadata.columns {
            let scalar_type = column_metadata.clone().scalar_type;
            let nullable = column_metadata.clone().nullable;
            let final_type: ndc_models::Type = if nullable {
                Nullable { underlying_type: Box::new(Named { name: scalar_type, }) }
            } else {
                Named { name: scalar_type, }
            };
            fields.insert(
                column_name.into(),
                ObjectField {
                    description: Some(column_metadata.clone().description.unwrap_or_default()),
                    r#type: final_type,
                    arguments: BTreeMap::new(),
                },
            );
        }
        if !scalar_types.contains_key(&table_metadata.name) {
            object_types.insert(
                table.clone(),
                ObjectType {
                    description: table_metadata.description.clone(),
                    fields: fields.clone(),
                },
            );
            collections.push(CollectionInfo {
                name: table_metadata.name.clone(),
                description: Some(format!("A collection of {}", table)),
                collection_type: table_metadata.name.clone(),
                arguments: BTreeMap::from_iter([]),
                foreign_keys: BTreeMap::from_iter([]),
                uniqueness_constraints: BTreeMap::from_iter([]),
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
    Ok((object_types, collections))
}
// ANCHOR_END: collections
