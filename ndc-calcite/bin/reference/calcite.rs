use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::Json;
use jni::JavaVM;
use jni::JNIEnv;
use jni::objects::{GlobalRef, JObject, JString, JValueGen, JValueOwned};
use jni::objects::JValueGen::Object;
use serde_json::Value;
use tracing::{event, Level};

use ndc_models::{CapabilitiesResponse, ErrorResponse, LeafCapability, NestedFieldCapabilities, RelationshipCapabilities, RowFieldValue};
use ndc_models as models;

use crate::{Row};

#[tracing::instrument]
fn create_calcite_connection<'a>(calcite_query: &JObject<'a>, env: &'a mut JNIEnv<'a>) -> crate::Result<JValueOwned<'a>> {
    let calcite_model = env::var("CALCITE_MODEL").unwrap_or_default();
    let arg0: JObject = env.new_string(calcite_model).unwrap().into();
    let args: &[JValueGen<&JObject<'_>>] = &[Object(&arg0)];
    let method_signature = "(Ljava/lang/String;)Ljava/sql/Connection;";
    let result = env.call_method(
        calcite_query,
        "createCalciteConnection",
        method_signature,
        args).map_err(|jni_error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                message: jni_error.to_string(),
                details: Default::default(),
            }),
        )
    })?;
    event!(Level::INFO,"Connected to Calcite");
    return Ok(result);
}

#[tracing::instrument]
pub fn create_calcite_query_engine<'a>(env: &'a mut JNIEnv<'a>) -> JObject<'a> {
    let class = env.find_class("org/kenstott/CalciteQuery").unwrap();
    let instance = env.new_object(class, "()V", &[]).unwrap();
    let _ = create_calcite_connection(&instance, env);
    event!(Level::INFO,"Instantiated to Calcite Query Engine");
    return instance;
}

#[tracing::instrument]
pub fn get_models(calcite_query: JObject, jvm: &JavaVM) -> HashMap<String, HashMap<String, String>> {
    let mut env = jvm.attach_current_thread_as_daemon().unwrap();
    let args: &[JValueGen<&JObject<'_>>] = &[];
    let method_signature = "()Ljava/lang/String;";
    let result = env.call_method(calcite_query, "getModels", method_signature, args);
    let map: HashMap<String, HashMap<String, String>>;
    match result.unwrap() {
        JValueGen::Object(obj) => {
            let j_string = JString::from(obj);
            let json_string: String = env.get_string(&j_string).unwrap().into();
            map = serde_json::from_str(&json_string).unwrap();
        }
        _ => todo!()
    }
    event!(Level::INFO, "Retrieved models from Calcite");
    return map;
}

#[tracing::instrument]
pub fn get_query(calcite_ref: GlobalRef, jvm: Arc<JavaVM>, query: &str, query_metadata: &models::Query,) -> crate::Result<Vec<Row>> {
    event!(Level::INFO, message = format!("Attempting this query: {}", query));
    let env = jvm.attach_current_thread().unwrap();
    let calcite_query = env.new_local_ref(calcite_ref).unwrap();
    let arg0: JObject = env.new_string(query).unwrap().into();
    let args: &[JValueGen<&JObject<'_>>] = &[Object(&arg0)];
    let method_signature = "(Ljava/lang/String;)Ljava/lang/String;";
    let mut env = jvm.attach_current_thread().unwrap();
    let result = env.call_method(calcite_query, "queryModels", method_signature, args);
    match result.unwrap() {
        JValueGen::Object(obj) => {
            let j_string = JString::from(obj);
            let json_string: String = env.get_string(&j_string).unwrap().into();
            let json_array: crate::Result<Vec<Row>> = serde_json::from_str(&json_string).map_err(|serde_json_error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        message: serde_json_error.to_string(),
                        details: Default::default(),
                    })
                )
            });
            let fix = env::var("FILE_ADAPTER_FIXES").unwrap_or("false".into()) == "true";
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

                        let new_rows: Vec<Row> = rows.into_iter().map(|mut row| {
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
                            // Didn't seem to be a problem to return more than
                            // what was requested.
                            //row.swap_remove("CONSTANT");
                            row
                        }).collect();
                        event!(Level::INFO,
                        message = format!("Completed Query. Retrieved {} rows. Result: {}",
                                   new_rows.len().to_string(),
                                    serde_json::to_string(&new_rows).unwrap()));
                        Ok(new_rows)
                    } else {
                        event!(Level::INFO,
                        message = format!("Completed Query. Retrieved {} rows. Result: {}",
                                   rows.len().to_string(),
                                    serde_json::to_string(&rows).unwrap()));
                        Ok(rows)
                    }
                }
                Err(e) => {
                    eprintln!("An error occurred: {:?}", e);
                    event!(Level::ERROR,message = format!("An error occurred: {:?}", e));
                    Err(e)
                }
            }
        }
        _ => todo!()
    }
}

pub fn calcite_capabilities() -> CapabilitiesResponse {
    crate::models::CapabilitiesResponse {
        version: "0.1.3".into(),
        capabilities: models::Capabilities {
            query: models::QueryCapabilities {
                aggregates: Some(LeafCapability {}),
                variables: Some(LeafCapability {}),
                explain: None,
                nested_fields: NestedFieldCapabilities {
                    filter_by: None,
                    order_by: None,
                    aggregates: None,
                },
            },
            mutation: models::MutationCapabilities {
                transactional: None,
                explain: None,
            },
            relationships: Some(RelationshipCapabilities {
                order_by_aggregate: None,
                relation_comparisons: None,
            }),
        },
    }
}