use std::collections::{BTreeMap, HashMap};
use std::error::Error;

use ndc_models::{CollectionInfo, ObjectField, ObjectType, ScalarType, SchemaResponse};
use ndc_models::Type::{Named, Nullable};

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
    data_models: &HashMap<String, HashMap<String, String>>,
    scalar_types: &BTreeMap<String, ScalarType>,
) -> Result<
    (BTreeMap<String, ObjectType>, Vec<CollectionInfo>),
    Result<SchemaResponse, Box<dyn Error>>,
> {
    let mut object_types: BTreeMap<String, ObjectType> = BTreeMap::new();
    let mut collections: Vec<CollectionInfo> = Vec::new();
    for (table_name, columns) in data_models {
        let mut fields: BTreeMap<String, ObjectField> = BTreeMap::new();
        for (column_name, column_type) in columns {
            fields.insert(
                column_name.into(),
                ObjectField {
                    description: Some("".into()),
                    r#type: Nullable {
                        underlying_type: Box::new(Named {
                            name: column_type.into(),
                        }),
                    },
                    arguments: BTreeMap::new(),
                },
            );
        }
        if !scalar_types.contains_key(table_name) {
            object_types.insert(
                table_name.into(),
                ObjectType {
                    description: Some("".into()),
                    fields: fields.clone(),
                },
            );
            collections.push(CollectionInfo {
                name: table_name.into(),
                description: Some(format!("A collection of {}", table_name)),
                collection_type: table_name.into(),
                arguments: BTreeMap::from_iter([]),
                foreign_keys: BTreeMap::from_iter([]),
                uniqueness_constraints: BTreeMap::from_iter([]),
            })
        } else {
            return Err(Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Table names cannot be same as a scalar type name: {}",
                    table_name
                ),
            ))));
        }
    }
    Ok((object_types, collections))
}
// ANCHOR_END: collections
