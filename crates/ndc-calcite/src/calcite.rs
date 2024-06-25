use std::collections::HashMap;

use indexmap::IndexMap;
use jni::JNIEnv;
use jni::objects::{GlobalRef, JObject, JString, JValueGen, JValueOwned};
use jni::objects::JValueGen::Object;
use ndc_models as models;
use ndc_models::RowFieldValue;
use ndc_sdk::connector::{InitializationError, QueryError};
use serde_json::Value;
use tracing::{event, Level};

use crate::configuration::CalciteConfiguration;
use crate::jvm::get_jvm;

pub type Row = IndexMap<String, RowFieldValue>;

#[tracing::instrument]
fn create_calcite_connection<'a>(
    configuration: &CalciteConfiguration,
    calcite_query: &JObject<'a>,
    env: &'a mut JNIEnv<'a>,
) -> Result<JValueOwned<'a>, InitializationError> {
    let calcite_model = configuration.clone().model_file_path.unwrap_or_default();
    let arg0: JObject = env.new_string(calcite_model).unwrap().into();
    let args: &[JValueGen<&JObject<'_>>] = &[Object(&arg0)];
    let method_signature = "(Ljava/lang/String;)Ljava/sql/Connection;";
    let result = env.call_method(
        calcite_query,
        "createCalciteConnection",
        method_signature,
        args,
    );

    match result {
        Ok(val) => {
            event!(Level::INFO, "Connected to Calcite");
            Ok(val)
        }
        Err(e) => {
            event!(Level::ERROR, "Error while connecting to Calcite: {:?}", e);
            Err(InitializationError::Other(Box::new(e)))
        }
    }
}

#[tracing::instrument]
pub fn create_calcite_query_engine<'a>(configuration: &CalciteConfiguration, env: &'a mut JNIEnv<'a>) -> JObject<'a> {
    let class = env.find_class("org/kenstott/CalciteQuery").unwrap();
    let instance = env.new_object(class, "()V", &[]).unwrap();
    let _ = create_calcite_connection(configuration, &instance, env);
    event!(Level::INFO, "Instantiated Calcite Query Engine");
    return instance;
}

#[tracing::instrument]
pub fn get_models(calcite_ref: GlobalRef) -> HashMap<String, HashMap<String, String>> {
    let jvm = get_jvm().lock().unwrap();
    let env = jvm.attach_current_thread().unwrap();
    let calcite_query = env.new_local_ref(calcite_ref).unwrap();
    let mut env = jvm.attach_current_thread_as_daemon().unwrap();
    let args: &[JValueGen<&JObject<'_>>] = &[];
    let method_signature = "()Ljava/lang/String;";
    let result = env.call_method(calcite_query, "getModels", method_signature, args);
    let map: HashMap<String, HashMap<String, String>>;
    match result.unwrap() {
        Object(obj) => {
            let j_string = JString::from(obj);
            let json_string: String = env.get_string(&j_string).unwrap().into();
            map = serde_json::from_str(&json_string).unwrap();
        }
        _ => todo!(),
    }
    event!(Level::INFO, "Retrieved models from Calcite");
    return map;
}

//noinspection RsExternalLinter
//noinspection RsExternalLinter
// ANCHOR: calcite_query
#[tracing::instrument]
pub fn calcite_query(
    configuration: &CalciteConfiguration,
    calcite_ref: GlobalRef,
    query: &str,
    query_metadata: &models::Query,
) -> Result<Vec<Row>, QueryError> {
    event!(
        Level::INFO,
        message = format!("Attempting this query: {}", query)
    );
    let _jvm = get_jvm().lock().unwrap();
    let env = _jvm.attach_current_thread().unwrap();
    let calcite_query = env.new_local_ref(calcite_ref).unwrap();
    let arg0: JObject = env.new_string(query).unwrap().into();
    let args: &[JValueGen<&JObject<'_>>] = &[Object(&arg0)];
    let method_signature = "(Ljava/lang/String;)Ljava/lang/String;";
    let mut env = _jvm.attach_current_thread().unwrap();
    let result = env.call_method(calcite_query, "queryModels", method_signature, args);
    match result.unwrap() {
        Object(obj) => {
            let j_string = JString::from(obj);
            let json_string: String = env.get_string(&j_string).unwrap().into();
            let json_array: Result<Vec<Row>, serde_json::Error> =
                serde_json::from_str(&json_string);
            let fix = configuration.fixes.unwrap_or(false);
            match json_array {
                //  TODO: These are attempts to deal with 2 Calcite issues
                // related to Calcite adapters. I have found that
                // they may drop a null value from a row. But the
                // the reasons are not clear.
                // Plus, it is an error to send in a SQL command
                // with no fields - so I have to send in a dummy
                // field and then remove it.
                // It would be better to find a more performant
                // way then converting all to JSON and making these
                // fixes in memory.
                Ok(rows) => {
                    if fix {
                        let fields = query_metadata.clone().fields.unwrap_or_default();
                        let aggregates = query_metadata.clone().aggregates.unwrap_or_default();
                        let max_keys = fields.len() + aggregates.len();
                        let mut key_sample: Vec<String> = vec![];
                        for (key, _) in fields {
                            key_sample.push(key)
                        }
                        for (key, _) in aggregates {
                            key_sample.push(key)
                        }

                        let new_rows: Vec<Row> = rows
                            .into_iter()
                            .map(|mut row| {
                                if fix && max_keys > row.len() {
                                    for key in &key_sample {
                                        if !row.contains_key(key) {
                                            row.insert(key.into(), RowFieldValue(Value::Null));
                                        }
                                    }
                                }
                                for (_key, value) in &mut row {
                                    if let RowFieldValue(val) = value {
                                        if val == "null" {
                                            *value = RowFieldValue(Value::Null);
                                        }
                                    }
                                }
                                row.swap_remove("CONSTANT");
                                row
                            })
                            .collect();
                        event!(
                            Level::INFO,
                            message = format!(
                                "Completed Query. Retrieved {} rows. Result: {}",
                                new_rows.len().to_string(),
                                serde_json::to_string(&new_rows).unwrap()
                            )
                        );
                        Ok(new_rows)
                    } else {
                        event!(
                            Level::INFO,
                            message = format!(
                                "Completed Query. Retrieved {} rows. Result: {}",
                                rows.len().to_string(),
                                serde_json::to_string(&rows).unwrap()
                            )
                        );
                        Ok(rows)
                    }
                }
                Err(e) => {
                    eprintln!("An error occurred: {:?} / {}", e, query);
                    event!(
                        Level::ERROR,
                        message = format!("An error occurred: {:?}", e)
                    );
                    Err(QueryError::Other(Box::new(e)))
                }
            }
        }
        _ => todo!(),
    }
}
// ANCHOR_END: calcite_query
