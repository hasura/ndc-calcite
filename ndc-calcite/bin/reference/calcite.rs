use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::Json;
use jni::JavaVM;
use jni::JNIEnv;
use jni::objects::{GlobalRef, JObject, JString, JValueGen, JValueOwned};
use jni::objects::JValueGen::Object;
use tracing::{event, Level};

use ndc_models::{CapabilitiesResponse, ErrorResponse, LeafCapability, NestedFieldCapabilities, RelationshipCapabilities};

use crate::Row;
use ndc_models as models;

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
pub fn get_query(calcite_ref: GlobalRef, jvm: Arc<JavaVM>, query: &str) -> crate::Result<Vec<Row>> {
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
            match json_array {
                Ok(rows) => {
                    let new_rows: Vec<Row> = rows.into_iter().map(|mut row| {
                        row.swap_remove("CONSTANT");
                        row
                    }).collect();
                    let message = format!("Retrieved {} rows from", new_rows.len().to_string());
                    event!(Level::INFO, message);
                    event!(Level::DEBUG, message = serde_json::to_string(&new_rows).unwrap());
                    Ok(new_rows)
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