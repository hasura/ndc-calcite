//! # Calcite Query Handler
//!
//! Takes a Calcite query, passes it to Calcite Query Engine
//! and then transforms it into a Vec<Row>.
//!
use std::collections::HashMap;
use std::fmt;

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
use crate::configuration::TableMetadata;

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

/// Creates a Calcite query engine.
///
/// This function creates an instance of the `CalciteQuery` class and initializes
/// it with the given configuration. It also creates a Calcite connection using
/// the provided configuration.
///
/// # Arguments
///
/// * `configuration` - A reference to the `CalciteConfiguration` object containing the configuration details.
/// * `env` - A mutable reference to the Java environment.
///
/// # Returns
///
/// Returns a `JObject` representing the created `CalciteQuery` instance.
///
/// # Examples
///
/// ```rust,no_run
/// use jni::JNIEnv;
/// use jni::objects::JObject;
/// use tracing::Level;
///
/// # fn create_calcite_connection(configuration: &CalciteConfiguration, instance: &JObject, env: &mut JNIEnv) { unimplemented!() }
///
/// #[tracing::instrument]
/// pub fn create_calcite_query_engine<'a>(configuration: &CalciteConfiguration, env: &'a mut JNIEnv<'a>) -> JObject<'a> {
///     let class = env.find_class("org/kenstott/CalciteQuery").unwrap();
///     let instance = env.new_object(class, "()V", &[]).unwrap();
///     let _ = create_calcite_connection(configuration, &instance, env);
///     event!(Level::INFO, "Instantiated Calcite Query Engine");
///     return instance;
/// }
/// ```
#[tracing::instrument]
pub fn create_calcite_query_engine<'a>(configuration: &CalciteConfiguration, env: &'a mut JNIEnv<'a>) -> JObject<'a> {
    let class = env.find_class("org/kenstott/CalciteQuery").unwrap();
    let instance = env.new_object(class, "()V", &[]).unwrap();
    let _ = create_calcite_connection(configuration, &instance, env);
    event!(Level::INFO, "Instantiated Calcite Query Engine");
    return instance;
}

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
pub fn get_models(calcite_ref: GlobalRef) -> HashMap<String, TableMetadata> {
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
            let map: HashMap<String, TableMetadata> = serde_json::from_str(&json_string).unwrap();
            map
        }
        _ => todo!(),
    };
    event!(Level::INFO, "Retrieved models from Calcite");
    return map;
}

fn parse_to_row(data: Vec<String>) -> Vec<Row> {
    let mut rows: Vec<Row> = Vec::new();
    for item in data {
        let row: Row = serde_json::from_str(&item).unwrap();
        rows.push(row);
    }
    rows
}

/// Executes a query using the Calcite Java library.
///
/// # Arguments
///
/// * `configuration` - The configuration for the Calcite query.
/// * `calcite_ref` - The global reference to the Calcite instance.
/// * `query` - The query string to be executed.
/// * `query_metadata` - Metadata for the query.
///
/// # Returns
///
/// Returns a `Result` containing a vector of `Row` if successful, or a `QueryError` if an error occurred.
///
/// # Tracing
///
/// This function is instrumented with the `tracing` library, which logs an `INFO` level message with the query before executing it.
///
/// # Errors
///
/// The function may return a `QueryError` if an error occurs during the execution of the query.
/// This includes issues with the Calcite adapters, null value dropping, errors caused by sending a SQL command with no fields, and issues with JSON serialization or deserialization.
///
/// # Example
///
/// ```rust
/// use models::Query;
///
/// let configuration = CalciteConfiguration::default();
/// let calcite_ref = GlobalRef::new();
/// let query = "SELECT * FROM table";
/// let query_metadata = Query::new();
///
/// let result = calcite_query(&configuration, calcite_ref, query, &query_metadata);
/// match result {
///     Ok(rows) => {
///         for row in rows {
///             println!("{:?}", row);
///         }
///     }
///     Err(error) => {
///         eprintln!("An error occurred: {:?}", error);
///     }
/// }
/// ```
// ANCHOR: calcite_query
#[tracing::instrument]
pub fn calcite_query(
    config: &CalciteConfiguration,
    calcite_reference: GlobalRef,
    sql_query: &str,
    query_metadata: &models::Query,
) -> Result<Vec<Row>, QueryError> {
    log_event(Level::INFO, &format!("Attempting this query: {}", sql_query));
    let jvm = get_jvm().lock().unwrap();
    let mut java_env = jvm.attach_current_thread().unwrap();
    let calcite_query = java_env.new_local_ref(calcite_reference).unwrap();

    let temp_string = java_env.new_string(sql_query).unwrap().into();
    let query_args: &[JValueGen<&JObject<'_>>] = &[Object(&temp_string)];
    let result = java_env.call_method(calcite_query, "queryModels", "(Ljava/lang/String;)Ljava/lang/String;", query_args);

    match result.unwrap() {
        Object(obj) => {
            let json_string: String = java_env.get_string(&JString::from(obj)).unwrap().into();
            let json_rows: Vec<String> = serde_json::from_str(&json_string).unwrap();
            let rows = parse_to_row(json_rows);
            let rows = if config.fixes.unwrap_or(false) {
                fix_rows(rows, query_metadata)
            } else {
                rows
            };
            log_event(Level::INFO, &format!("Completed Query. Retrieved {} rows. Result: {}", rows.len().to_string(), serde_json::to_string(&rows).unwrap()));
            Ok(rows)
        },
        _ => Err(QueryError::Other(Box::new(CalciteError{message: String::from("Invalid response from Calcite.")})))
    }
}

fn log_event(level: Level, message: &str) {
    match level {
        Level::ERROR => tracing::error!("{}", message),
        Level::WARN => tracing::warn!("{}", message),
        Level::INFO => tracing::info!("{}", message),
        Level::DEBUG => tracing::debug!("{}", message),
        Level::TRACE => tracing::trace!("{}", message),
    }
}

fn fix_rows(rows: Vec<Row>, query_metadata: &models::Query) -> Vec<Row> {
    let fields = query_metadata.clone().fields.unwrap_or_default();
    let aggregates = query_metadata.clone().aggregates.unwrap_or_default();
    let max_keys = fields.len() + aggregates.len();
    let mut key_sample: Vec<String> = vec![];

    for (key, _) in fields {
        key_sample.push(key);
    }

    for (key, _) in aggregates {
        key_sample.push(key);
    }

    rows.into_iter().map(|mut row| {
        if max_keys > row.len() {
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
    }).collect()
}
// ANCHOR_END: calcite_query

#[derive(Debug)]
struct CalciteError {
    message: String,
}

impl fmt::Display for CalciteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for CalciteError {}