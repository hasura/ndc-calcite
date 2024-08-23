use jni::objects::{GlobalRef, JObject, JString, JValueGen};
use std::collections::HashMap;
use jni::objects::JValueGen::Object;
use ndc_models::CollectionName;
use crate::calcite::TableMetadata;
use tracing::{event, Level};
use crate::jvm::get_jvm;

/// Retrieves models from Calcite.
///
/// # Arguments
///
/// * `calcite_ref` - A reference to the Calcite instance.
///
/// # Return
///
/// A `HashMap` containing the retrieved models. The outer `HashMap` maps model names
/// to inner `HashMap`s, where each inner `HashMap` represents a model with its properties.
#[tracing::instrument]
pub fn get_models(calcite_ref: GlobalRef) -> HashMap<CollectionName, TableMetadata> {
    let jvm = get_jvm().lock().unwrap();
    let env = jvm.attach_current_thread().unwrap();
    let calcite_query = env.new_local_ref(calcite_ref).unwrap();
    let mut env = jvm.attach_current_thread_as_daemon().unwrap();
    let args: &[JValueGen<&JObject<'_>>] = &[];
    let method_signature = "()Ljava/lang/String;";
    let result = env.call_method(calcite_query, "getModels", method_signature, args);
    let map= match result.unwrap() {
        Object(obj) => {
            let j_string = JString::from(obj);
            let json_string: String = env.get_string(&j_string).unwrap().into();
            let map: HashMap<CollectionName, TableMetadata> = serde_json::from_str(&json_string).unwrap();
            map
        }
        _ => todo!(),
    };
    event!(Level::INFO, "Retrieved models from Calcite");
    return map;
}
