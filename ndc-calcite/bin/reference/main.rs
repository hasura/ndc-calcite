use std::{collections::BTreeMap, env, net, sync::Arc};

use axum::{
    extract::State,
    http::StatusCode,
    Json,
    Router, routing::{get, post},
};
use dotenv;
use indexmap::IndexMap;
use jni::JavaVM;
use jni::objects::GlobalRef;
use opentelemetry::global;
use prometheus::{Encoder, IntCounter, IntGauge, Opts, Registry, TextEncoder};
use tokio::sync::Mutex;
use ndc_models::{ErrorResponse, RowFieldValue, SchemaResponse, self as models};
use crate::calcite::calcite_capabilities;

mod jvm;
mod calcite;
mod sql;
mod schema;
mod otel;

const DEFAULT_PORT: &str = "8080";

// ANCHOR: row-type
pub type Row = IndexMap<String, RowFieldValue>;

// ANCHOR_END: row-type

// ANCHOR: app-state
#[derive(Debug, Clone)]
pub struct AppState {
    pub schema: SchemaResponse,
    pub metrics: Metrics,
    pub java_vm: Arc<JavaVM>,
    pub calcite_ref: GlobalRef,
}
// ANCHOR_END: app-state

#[derive(Debug, Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub total_requests: IntCounter,
    pub active_requests: IntGauge,
}

// ANCHOR: metrics
impl Metrics {
    fn new() -> prometheus::Result<Metrics> {
        let total_requests =
            IntCounter::with_opts(Opts::new("total_requests", "number of total requests"))?;
        let active_requests =
            IntGauge::with_opts(Opts::new("active_requests", "number of active requests"))?;
        let registry = Registry::new();
        registry.register(Box::new(total_requests.clone()))?;
        registry.register(Box::new(active_requests.clone()))?;
        Ok(Metrics {
            registry,
            total_requests,
            active_requests,
        })
    }

    fn as_text(&self) -> Option<String> {
        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder.encode(&metric_families, &mut buffer).ok()?;
        String::from_utf8(buffer).ok()
    }
}
// ANCHOR_END: metrics

// ANCHOR: metrics_middleware
async fn metrics_middleware<T>(
    state: State<Arc<Mutex<AppState>>>,
    request: axum::http::Request<T>,
    next: axum::middleware::Next<T>,
) -> axum::response::Response {
    // Don't hold the lock to update metrics, since the
    // lock doesn't protect the metrics anyway.
    let metrics = {
        let state = state.lock().await;
        state.metrics.clone()
    };

    metrics.total_requests.inc();
    metrics.active_requests.inc();
    let response = next.run(request).await;
    metrics.active_requests.dec();
    response
}
// ANCHOR_END: metrics_middleware

// ANCHOR: init_app_state
async fn init_app_state() -> std::result::Result<AppState, (StatusCode, Json<ErrorResponse>)> {
    let metrics = Metrics::new().unwrap();
    let java_vm = Arc::new(jvm::create_jvm());
    let mut env = java_vm.attach_current_thread_as_daemon().unwrap();
    let calcite = calcite::create_calcite_query_engine(&mut env);
    let env = java_vm.attach_current_thread_as_daemon().unwrap();
    let calcite_ref = env.new_global_ref(calcite).unwrap();
    let schema = schema::get_schema(java_vm.clone(), calcite_ref.clone()).await;

    match schema {
        Ok(schema) => {
            Ok(AppState {
                metrics,
                schema,
                java_vm,
                calcite_ref,
            })
        }
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    message: e.to_string(),
                    details: serde_json::Value::Null,
                }),
            ));
        }
    }

}
// ANCHOR_END: init_app_state

// ANCHOR: result
pub type Result<A> = core::result::Result<A, (StatusCode, Json<ErrorResponse>)>;
// ANCHOR_END: result

// ANCHOR: main
#[tokio::main]
async fn main() -> std::result::Result<(), std::io::Error> {
    dotenv::dotenv().ok();
    otel::init_tracer();

    let app_state = match init_app_state().await {
        Ok(state) => Arc::new(Mutex::new(state)),
        Err((_, ErrorResponse)) => {
            eprintln!("Failed to initialize app state");
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to initialize app state: {}", ErrorResponse.message)));
        },
    };

    let app = Router::new()
        .route("/health", get(get_health))
        .route("/metrics", get(get_metrics))
        .route("/capabilities", get(get_capabilities))
        .route("/schema", get(get_schema))
        .route("/query", post(post_query))
        .route("/query/explain", post(post_query_explain))
        .route("/mutation", post(post_mutation))
        .route("/mutation/explain", post(post_mutation_explain))
        .layer(axum::middleware::from_fn_with_state(
            app_state.clone(),
            metrics_middleware,
        ))
        .with_state(app_state);

    // Start the server on `localhost:<PORT>`.
    // This says it's binding to an IPv6 address, but will actually listen to
    // any IPv4 or IPv6 address.
    let host = net::IpAddr::V6(net::Ipv6Addr::UNSPECIFIED);
    let port = env::var("PORT").unwrap_or(DEFAULT_PORT.into());
    let addr = net::SocketAddr::new(host, port.parse().unwrap());

    let server = axum::Server::bind(&addr).serve(app.into_make_service());
    println!("Serving on {}", server.local_addr());
    let _ = server.with_graceful_shutdown(shutdown_handler()).await;

    Ok(())
}

// ANCHOR_END: main

// ANCHOR: shutdown_handler
async fn shutdown_handler() {
    // Wait for a SIGINT, i.e. a Ctrl+C from the keyboard
    let sigint = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install signal handler");
    };
    // Wait for a SIGTERM, i.e. a normal `kill` command
    #[cfg(unix)]
        let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await
    };
    // Block until either of the above happens
    #[cfg(unix)]
    tokio::select! {
        () = sigint => (),
        _ = sigterm => (),
    }
    #[cfg(windows)]
    tokio::select! {
        _ = sigint => (),
    }
    // Shutdown trace pipeline
    global::shutdown_tracer_provider();
}
// ANCHOR_END: shutdown_handler

// ANCHOR: health
async fn get_health() -> StatusCode {
    StatusCode::OK
}
// ANCHOR_END: health

// ANCHOR: get_metrics
async fn get_metrics(State(state): State<Arc<Mutex<AppState>>>) -> Result<String> {
    let state = state.lock().await;
    state.metrics.as_text().ok_or((
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            message: "cannot encode metrics".into(),
            details: serde_json::Value::Null,
        }),
    ))
}
// ANCHOR_END: get_metrics

// ANCHOR: get_capabilities
async fn get_capabilities() -> Json<models::CapabilitiesResponse> {
    Json(calcite_capabilities())
}
// ANCHOR_END: get_capabilities

// ANCHOR: get_schema
async fn get_schema(state: State<Arc<Mutex<AppState>>>) -> Result<Json<SchemaResponse>> {
    let state = state.lock().await;
    let schema = schema::get_schema(state.clone().java_vm, state.clone().calcite_ref).await;
    match schema {
        Ok(_) => {
            Ok(Json(schema.unwrap()))
        }
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    message: "problem retrieving schema".into(),
                    details: serde_json::Value::Null,
                }),
            ));
        }
    }
}
// ANCHOR_END: get_schema

// ANCHOR: post_query
pub async fn post_query(
    State(state): State<Arc<Mutex<AppState>>>,
    Json(request): Json<models::QueryRequest>,
) -> Result<Json<models::QueryResponse>> {
    let state = state.lock().await;

    let variable_sets = request.variables.unwrap_or(vec![BTreeMap::new()]);
    let mut row_sets = vec![];
    for variables in &variable_sets {
        let row_set = execute_query_with_variables(
            &request.collection,
            &request.arguments,
            &request.collection_relationships,
            &request.query,
            variables,
            &state,
        )?;
        row_sets.push(row_set);
    }

    Ok(Json(models::QueryResponse(row_sets)))
}
// ANCHOR_END: post_query

// ANCHOR: execute_query_with_variables
#[tracing::instrument]
fn execute_query_with_variables(
    collection: &str,
    arguments: &BTreeMap<String, models::Argument>,
    _collection_relationships: &BTreeMap<String, models::Relationship>,
    query: &models::Query,
    variables: &BTreeMap<String, serde_json::Value>,
    state: &AppState,
) -> Result<models::RowSet> {
    sql::query_with_variables(collection, arguments, query, variables, state)
}

// ANCHOR_END: get_collection_by_name
/// ANCHOR: Root
#[derive(Clone, Copy)]
enum Root<'a> {
    /// References to the root collection actually
    /// refer to the current row, because the path to
    /// the nearest enclosing [`models::Query`] does not pass
    /// an [`models::Expression::Exists`] node.
    CurrentRow,
    /// References to the root collection refer to the
    /// explicitly-identified row, which is the row
    /// being evaluated in the context of the nearest enclosing
    /// [`models::Query`].
    ExplicitRow(&'a Row),
}

/// ANCHOR_END: Root

// // ANCHOR: eval_argument
fn eval_argument(
    variables: &BTreeMap<String, serde_json::Value>,
    argument: &models::Argument,
) -> Result<serde_json::Value> {
    match argument {
        models::Argument::Variable { name } => {
            let value = variables
                .get(name.as_str())
                .ok_or((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        message: "invalid variable name".into(),
                        details: serde_json::Value::Null,
                    }),
                ))
                .cloned()?;
            Ok(value)
        }
        models::Argument::Literal { value } => Ok(value.clone()),
    }
}
// ANCHOR_END: eval_argument


// ANCHOR: query_explain
async fn post_query_explain(
    Json(_request): Json<models::QueryRequest>,
) -> Result<Json<models::ExplainResponse>> {
    Err((
        StatusCode::NOT_IMPLEMENTED,
        Json(ErrorResponse {
            message: "explain is not supported".into(),
            details: serde_json::Value::Null,
        }),
    ))
}

// ANCHOR_END: query_explain

// ANCHOR: mutation_explain
async fn post_mutation_explain(
    Json(_request): Json<models::MutationRequest>,
) -> Result<Json<models::ExplainResponse>> {
    Err((
        StatusCode::NOT_IMPLEMENTED,
        Json(ErrorResponse {
            message: "explain is not supported".into(),
            details: serde_json::Value::Null,
        }),
    ))
}

// ANCHOR_END: mutation_explain
// ANCHOR: post_mutation_signature
async fn post_mutation(
    State(state): State<Arc<Mutex<AppState>>>,
    Json(request): Json<models::MutationRequest>,
) -> Result<Json<models::MutationResponse>> {
    // ANCHOR_END: post_mutation_signature
    // ANCHOR: post_mutation
    if request.operations.len() > 1 {
        Err((
            StatusCode::NOT_IMPLEMENTED,
            Json(ErrorResponse {
                message: "transactional mutations are not supported".into(),
                details: serde_json::Value::Null,
            }),
        ))
    } else {
        let mut state = state.lock().await;

        let mut operation_results = vec![];

        for operation in &request.operations {
            let operation_result = execute_mutation_operation(
                &mut state,
                &request.collection_relationships,
                operation,
            )?;
            operation_results.push(operation_result);
        }

        Ok(Json(models::MutationResponse { operation_results }))
    }
}

// ANCHOR_END: post_mutation
// ANCHOR: execute_mutation_operation
fn execute_mutation_operation(
    state: &mut AppState,
    collection_relationships: &BTreeMap<String, models::Relationship>,
    operation: &models::MutationOperation,
) -> Result<models::MutationOperationResults> {
    match operation {
        models::MutationOperation::Procedure {
            name,
            arguments,
            fields,
        } => execute_procedure(state, name, arguments, fields, collection_relationships),
    }
}

// ANCHOR_END: execute_mutation_operation
// ANCHOR: execute_procedure_signature
fn execute_procedure(
    _state: &mut AppState,
    name: &str,
    _arguments: &BTreeMap<String, serde_json::Value>,
    _fields: &Option<models::NestedField>,
    _collection_relationships: &BTreeMap<String, models::Relationship>,
) -> std::result::Result<models::MutationOperationResults, (StatusCode, Json<ErrorResponse>)>
// ANCHOR_END: execute_procedure_signature
// ANCHOR: execute_procedure_signature_impl
{
    match name {
        _ => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                message: "unknown procedure".into(),
                details: serde_json::Value::Null,
            }),
        )),
    }
}
// ANCHOR_END: execute_procedure_signature_impl

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, File},
        io::Write,
        path::PathBuf,
        sync::Arc,
    };

    use async_trait::async_trait;
    use axum::{extract::State, Json};
    use goldenfile::Mint;
    use tokio::sync::Mutex;

    use ndc_models as models;
    use ndc_test::{
        configuration::{TestConfiguration, TestGenerationConfiguration, TestOptions},
        connector::Connector,
        error::Error,
        reporter::TestResults,
        test_connector,
    };

    use crate::{get_capabilities, get_schema, init_app_state, post_mutation, post_query};

    #[test]
    fn test_capabilities() {
        tokio_test::block_on(async {
            let test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests");

            let mut mint = Mint::new(&test_dir);

            let expected_path = PathBuf::from_iter(["capabilities", "expected.json"]);

            let response = crate::get_capabilities().await;

            let mut expected = mint.new_goldenfile(expected_path).unwrap();

            let response_json = serde_json::to_string_pretty(&response.0).unwrap();

            write!(expected, "{response_json}").unwrap();

            // Test roundtrip
            assert_eq!(
                response.0,
                serde_json::from_str(response_json.as_str()).unwrap()
            );
        });
    }

    #[test]
    fn test_schema() {
        tokio_test::block_on(async {
            let test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests");

            let mut mint = Mint::new(&test_dir);

            let expected_path = PathBuf::from_iter(["schema", "expected.json"]);

            let response = crate::get_schema().await;

            let mut expected = mint.new_goldenfile(expected_path).unwrap();

            write!(
                expected,
                "{}",
                serde_json::to_string_pretty(&response.0).unwrap()
            )
                .unwrap();
        });
    }

    #[test]
    fn test_query() {
        tokio_test::block_on(async {
            let test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests");

            let mut mint = Mint::new(&test_dir);

            for input_file in fs::read_dir(test_dir.join("query")).unwrap() {
                let entry = input_file.unwrap();
                let request = {
                    let path = entry.path();
                    assert!(path.is_dir());
                    let req_path = path.join("request.json");
                    let req_file = File::open(req_path).unwrap();
                    serde_json::from_reader::<_, models::QueryRequest>(req_file).unwrap()
                };

                let expected_path = {
                    let path = entry.path();
                    let test_name = path.file_name().unwrap().to_str().unwrap();
                    PathBuf::from_iter(["query", test_name, "expected.json"])
                };

                let state = Arc::new(Mutex::new(crate::init_app_state()));
                let response = crate::post_query(State(state), Json(request))
                    .await
                    .unwrap();

                let mut expected = mint.new_goldenfile(expected_path).unwrap();

                write!(
                    expected,
                    "{}",
                    serde_json::to_string_pretty(&response.0).unwrap()
                )
                    .unwrap();
            }
        });
    }

    #[test]
    fn test_mutation() {
        tokio_test::block_on(async {
            let test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests");

            let mut mint = Mint::new(&test_dir);

            for input_file in fs::read_dir(test_dir.join("mutation")).unwrap() {
                let entry = input_file.unwrap();
                let request = {
                    let path = entry.path();
                    assert!(path.is_dir());
                    let req_path = path.join("request.json");
                    let req_file = File::open(req_path).unwrap();
                    serde_json::from_reader::<_, models::MutationRequest>(req_file).unwrap()
                };

                let expected_path = {
                    let path = entry.path();
                    let test_name = path.file_name().unwrap().to_str().unwrap();
                    PathBuf::from_iter(["mutation", test_name, "expected.json"])
                };

                let state = Arc::new(Mutex::new(crate::init_app_state()));
                let response = crate::post_mutation(State(state), Json(request))
                    .await
                    .unwrap();

                let mut expected = mint.new_goldenfile(expected_path).unwrap();

                write!(
                    expected,
                    "{}",
                    serde_json::to_string_pretty(&response.0).unwrap()
                )
                    .unwrap();
            }
        });
    }

    struct Reference {
        state: crate::AppState,
    }

    #[async_trait(? Send)]
    impl Connector for Reference {
        async fn get_capabilities(&self) -> Result<models::CapabilitiesResponse, Error> {
            Ok(get_capabilities().await.0)
        }

        async fn get_schema(&self) -> Result<models::SchemaResponse, Error> {
            Ok(get_schema().await.0)
        }

        async fn query(
            &self,
            request: models::QueryRequest,
        ) -> Result<models::QueryResponse, Error> {
            Ok(post_query(
                State(Arc::new(Mutex::new(self.state.clone()))),
                Json(request),
            )
                .await
                .map_err(|(_, Json(err))| Error::ConnectorError(err))?
                .0)
        }

        async fn mutation(
            &self,
            request: models::MutationRequest,
        ) -> Result<models::MutationResponse, Error> {
            Ok(post_mutation(
                State(Arc::new(Mutex::new(self.state.clone()))),
                Json(request),
            )
                .await
                .map_err(|(_, Json(err))| Error::ConnectorError(err))?
                .0)
        }
    }

    #[test]
    fn test_ndc_test() {
        tokio_test::block_on(async {
            let configuration = TestConfiguration {
                seed: None,
                snapshots_dir: None,
                gen_config: TestGenerationConfiguration::default(),
                options: TestOptions::default(),
            };
            let connector = Reference {
                state: init_app_state(),
            };
            let mut reporter = TestResults::default();
            test_connector(&configuration, &connector, &mut reporter).await;
            assert!(reporter.failures.is_empty());
        });
    }
}
