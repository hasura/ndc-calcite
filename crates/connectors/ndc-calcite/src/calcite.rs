//! # Calcite Query Handler
//!
//! Takes a Calcite query, passes it to Calcite Query Engine
//! and then transforms it into a Vec<Row>.
//!
use std::fmt;

use indexmap::IndexMap;
use jni::JNIEnv;
use jni::objects::{GlobalRef, JObject, JString, JValueGen};
use jni::objects::JValueGen::Object;
use ndc_models as models;
use ndc_models::{FieldName, RowFieldValue};
use ndc_sdk::connector::QueryError;
use opentelemetry::trace::{TraceContextExt};
use serde::de::DeserializeOwned;
use serde_json::Value;
use tracing::{event, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use ndc_calcite_schema::jvm::get_jvm;
use ndc_calcite_schema::version5::create_jvm_connection;
use ndc_calcite_schema::version5::ParsedConfiguration;

pub type Row = IndexMap<FieldName, RowFieldValue>;

fn is_json_string<T: DeserializeOwned>(s: &str) -> bool {
    serde_json::from_str::<T>(s).is_ok()
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
/// use ndc_calcite_schema::version5::{create_jvm_connection, ParsedConfiguration};
///
/// #[tracing::instrument(skip(configuration, env))]
/// pub fn create_query_engine<'a>(configuration: &ParsedConfiguration, env: &'a mut JNIEnv<'a>) -> JObject<'a> {
///     let class = env.find_class("org/kenstott/CalciteQuery").unwrap();
///     let instance = env.new_object(class, "()V", &[]).unwrap();
///     let _ = create_jvm_connection(configuration, &instance, env);
///     event!(Level::INFO, "Instantiated Calcite Query Engine");
///     return instance;
/// }
/// ```
#[tracing::instrument(skip(configuration, env), level = Level::INFO)]
pub fn create_query_engine<'a>(configuration: &'a ParsedConfiguration, env: &'a mut JNIEnv<'a>) -> JObject<'a> {
    let class = env.find_class("org/kenstott/CalciteQuery").unwrap();
    let instance = env.new_object(class, "()V", &[]).unwrap();
    let _ = create_jvm_connection(configuration, &instance, env);
    event!(Level::INFO, "Instantiated Calcite Query Engine");
    return instance;
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
/// let result = connector_query(&configuration, calcite_ref, query, &query_metadata);
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
#[tracing::instrument(
    fields(internal.visibility = "user"), skip(config, calcite_reference, query_metadata), level = Level::INFO
)]
pub fn connector_query(
    config: &ParsedConfiguration,
    calcite_reference: GlobalRef,
    sql_query: &str,
    query_metadata: &models::Query,
    explain: &bool,
) -> Result<Vec<Row>, QueryError> {

    // This method of retrieving current span context is not working!!!
    let span = tracing::Span::current();
    let otel_context = span.context();
    let span_id = otel_context.span().span_context().span_id();
    let trace_id = otel_context.span().span_context().trace_id();

    let jvm = get_jvm().lock().unwrap();
    let mut java_env = jvm.attach_current_thread().unwrap();
    let calcite_query = java_env.new_local_ref(calcite_reference).unwrap();

    let temp_string = java_env.new_string(sql_query).unwrap();
    let trace_id_jstring = java_env.new_string(trace_id.to_string()).unwrap();
    let span_id_jstring = java_env.new_string(span_id.to_string()).unwrap();
    let temp_obj = JObject::from(temp_string);
    let trace_id_obj = JObject::from(trace_id_jstring);
    let span_id_obj = JObject::from(span_id_jstring);

    let query_args: &[JValueGen<&JObject<'_>>] = &[Object(&temp_obj), Object(&trace_id_obj), Object(&span_id_obj)];
    let result = java_env.call_method(
        calcite_query,
        if *explain { "queryPlanModels" } else { "queryModels" },
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;",
        query_args);

    match result.unwrap() {
        Object(obj) => {
            let json_string: String = java_env.get_string(&JString::from(obj)).unwrap().into();
            let rows: Vec<Row> = match serde_json::from_str::<Vec<String>>(&json_string) {
                Ok(json_rows) => {
                    parse_to_row(json_rows)
                },
                Err(_) => match serde_json::from_str::<Vec<Row>>(&json_string) {
                    Ok(vec) => vec,
                    Err(error) => {
                        let err = CalciteError { message: format!("Failed to deserialize JSON: {}", error) };
                        return Err(QueryError::Other(Box::new(err), Value::Null));
                    }
                }
            };
            event!(Level::DEBUG, result = format!("Completed Query. Retrieved {} rows. Result: {:?}", rows.len().to_string(), serde_json::to_string_pretty(&rows)));
            Ok(rows)
        }
        _ => Err(QueryError::Other(Box::new(CalciteError { message: String::from("Invalid response from Calcite.") }), Value::Null))
    }
}

fn fix_rows(rows: Vec<Row>, query_metadata: &models::Query) -> Vec<Row> {
    let fields = query_metadata.clone().fields.unwrap_or_default();
    let aggregates = query_metadata.clone().aggregates.unwrap_or_default();
    let max_keys = fields.len() + aggregates.len();
    let mut key_sample: Vec<FieldName> = vec![];

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
                    row.insert(key.clone(), RowFieldValue(Value::Null));
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