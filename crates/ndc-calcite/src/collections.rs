use std::collections::{BTreeMap, HashMap};
use ndc_models::{CollectionInfo, ObjectField, ObjectType, ScalarType, SchemaResponse};
use std::error::Error;
use ndc_models::Type::{Named, Nullable};

// ANCHOR: collections
#[tracing::instrument]
pub fn collections(data_models: &HashMap<String, HashMap<String, String>>, scalar_types: &BTreeMap<String, ScalarType>) -> Result<(BTreeMap<String, ObjectType>, Vec<CollectionInfo>), Result<SchemaResponse, Box<dyn Error>>> {
    let mut object_types: BTreeMap<String, ObjectType> = BTreeMap::new();
    let mut collections: Vec<CollectionInfo> = Vec::new();
    for (table_name, columns) in data_models {
        let mut fields: BTreeMap<String, ObjectField> = BTreeMap::new();
        for (column_name, column_type) in columns {
            fields.insert(column_name.into(), ObjectField {
                description: Some("".into()),
                r#type: Nullable {
                    underlying_type: Box::new(Named { name: column_type.into() })
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
                arguments: BTreeMap::from_iter([]),
                foreign_keys: BTreeMap::from_iter([]),
                uniqueness_constraints: BTreeMap::from_iter([]),
            })
        } else {
            return Err(Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Table names cannot be same as a scalar type name: {}", table_name)))));
        }
    }
    Ok((object_types, collections))
}
// ANCHOR_END: collections
