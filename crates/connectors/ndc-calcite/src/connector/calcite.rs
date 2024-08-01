//! # Connector Definition
//!
//! Provides HTTP server paths for required NDC functions. Connecting
//! the request to the underlying code and providing the result.
//!
use std::collections::BTreeMap;
use std::{env, fs};
use std::path::Path;

use async_trait::async_trait;
use dotenv;
use jni::objects::GlobalRef;
use ndc_models::{Argument, ExplainResponse, RelationshipArgument};
use ndc_sdk::connector::{
    Connector, ConnectorSetup, ExplainError, FetchMetricsError, HealthError, InitializationError,
    MutationError, ParseError, QueryError, SchemaError,
};
use ndc_sdk::json_response::JsonResponse;
use ndc_sdk::models;
use tracing::info_span;
use tracing::Instrument;

use crate::{calcite, jvm, query, schema};
use crate::capabilities::calcite_capabilities;
use crate::configuration::{CalciteConfiguration, Model};
use crate::jvm::init_jvm;
use crate::query::QueryParams;

pub const CONFIG_FILE_NAME: &str = "configuration.json";
pub const DEV_CONFIG_FILE_NAME: &str = "dev.local.configuration.json";

#[derive(Clone, Default, Debug)]
pub struct Calcite {}

#[derive(Clone, Debug)]
pub struct CalciteState {
    pub calcite_ref: GlobalRef,
}

/// Checks if the code is running inside a container.
///
/// This function checks for the existence of the `/.dockerenv` file in the filesystem,
/// which is commonly used to indicate that the code is running inside a Docker container.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// fn is_running_in_container() -> bool {
///     Path::new("/.dockerenv").exists()
/// }
///
/// assert_eq!(is_running_in_container(), false);
/// ```
///
/// # Returns
///
/// Returns `true` if the code is running inside a container, `false` otherwise.
#[tracing::instrument]
pub fn is_running_in_container() -> bool {
    Path::new("/.dockerenv").exists() || env::var("KUBERNETES_SERVICE_HOST").is_ok()
}

#[tracing::instrument]
fn execute_query_with_variables(
    config: &CalciteConfiguration,
    coll: &str,
    args: &BTreeMap<String, models::RelationshipArgument>,
    coll_rel: &BTreeMap<String, models::Relationship>,
    query: &models::Query,
    vars: &BTreeMap<String, serde_json::Value>,
    state: &CalciteState,
    explain: &bool
) -> Result<models::RowSet, QueryError> {
    query::orchestrate_query(QueryParams { config, coll, coll_rel, args, query, vars, state, explain})
}

#[async_trait]
impl ConnectorSetup for Calcite {
    type Connector = Self;

    async fn parse_configuration(
        &self,
        configuration_dir: impl AsRef<Path> + Send,
    ) -> Result<<Self as Connector>::Configuration, ParseError> {
        async {
            info_span!("inside parsing configuration");
        }
            .instrument(info_span!("parsing configuration"))
            .await;
        dotenv::dotenv().ok();
        let file_path = if is_running_in_container() {
            configuration_dir.as_ref().join(CONFIG_FILE_NAME)
        } else {
            configuration_dir.as_ref().join(DEV_CONFIG_FILE_NAME)
        };
        println!("Configuration file path: {:?}", configuration_dir.as_ref().display());
        match fs::read_to_string(file_path) {
            Ok(file_content) => {
                println!("Configuration file content: {:?}", file_content);
                let mut json_object: CalciteConfiguration = serde_json::from_str(&file_content)
                    .map_err(|err| ParseError::Other(Box::from(err.to_string())))?;
                match json_object.model_file_path {
                    None => {
                        match json_object.model {
                            None => {}
                            Some(_) => {}
                        }
                    }
                    Some(ref model_file_path) => {
                        match fs::read_to_string(model_file_path) {
                            Ok(models) => {
                                println!("Configuration model content: {:?}", models);
                                let model_object: Model = serde_json::from_str(&models)
                                    .map_err(|err| ParseError::Other(Box::from(err.to_string())))?;
                                json_object.model = Some(model_object)
                            },
                            Err(_err) => {}
                        }
                    }
                }
                match json_object.metadata {
                    None => {
                        let state = init_state(&json_object).expect("TODO: panic message");
                        json_object.metadata = Some(calcite::get_models(state.calcite_ref));
                        println!("metadata: {:?}",  serde_json::to_string_pretty(&json_object.metadata));
                    }
                    Some(_) => {}
                }
                Ok(json_object)
            }
            Err(err) => Err(ParseError::Other(Box::from(err.to_string()))),
        }
    }

    async fn try_init_state(
        &self,
        configuration: &<Self as Connector>::Configuration,
        _metrics: &mut prometheus::Registry,
    ) -> Result<<Self as Connector>::State, InitializationError> {
        init_state(configuration)
    }

}

#[async_trait]
impl Connector for Calcite {
    type Configuration = CalciteConfiguration;
    type State = CalciteState;

    fn fetch_metrics(
        _configuration: &Self::Configuration,
        _state: &Self::State,
    ) -> Result<(), FetchMetricsError> {
        Ok(())
    }

    async fn health_check(
        _configuration: &Self::Configuration,
        _state: &Self::State,
    ) -> Result<(), HealthError> {
        Ok(())
    }

    async fn get_capabilities() -> JsonResponse<models::CapabilitiesResponse> {
        calcite_capabilities().into()
    }

    async fn get_schema(
        configuration: &Self::Configuration,
    ) -> Result<JsonResponse<models::SchemaResponse>, SchemaError> {
        async {
            info_span!("inside tracing Calcite");
        }
            .instrument(info_span!("tracing Calcite"))
            .await;
        dotenv::dotenv().ok();
        let calcite;
        let calcite_ref;
        {
            let java_vm = jvm::get_jvm().lock().unwrap();
            let mut env = java_vm.attach_current_thread_as_daemon().unwrap();
            calcite = calcite::create_calcite_query_engine(configuration, &mut env);
            let env = java_vm.attach_current_thread_as_daemon().unwrap();
            calcite_ref = env.new_global_ref(calcite).unwrap();
        }

        let schema = schema::get_schema(configuration, calcite_ref.clone());
        match schema {
            Ok(schema) => Ok(schema.into()),
            Err(e) => Err(SchemaError::Other(e.to_string().into())),
        }
    }

    async fn query_explain(
        configuration: &Self::Configuration,
        state: &Self::State,
        request: models::QueryRequest,
    ) -> Result<JsonResponse<models::ExplainResponse>, ExplainError> {
        let variable_sets = request.variables.unwrap_or(vec![BTreeMap::new()]);
        let mut map: BTreeMap<String, String> = BTreeMap::new();
        let input_map: BTreeMap<String, Argument> = request.arguments.clone();
        let relationship_arguments : BTreeMap<String, models::RelationshipArgument> =
            input_map.iter()
                .map(|(key, value)|
                (key.clone(), convert_to_relationship_argument(value))
                )
                .collect();
        for variables in &variable_sets {
            let row_set = execute_query_with_variables(
                configuration,
                &request.collection,
                &relationship_arguments,
                &request.collection_relationships,
                &request.query,
                variables,
                &state,
                &true
            ).map_err(|error| ExplainError::Other(Box::new(error)))?;
            match row_set.aggregates {
                None => {}
                Some(map_index) => {
                    let map_btree: BTreeMap<String, String> = map_index.iter()
                        .map(|(key, value)| (key.clone(), value.to_string()))
                        .collect();
                    map.extend(map_btree);
                }
            };
            match row_set.rows {
                None => {}
                Some(r) => {
                    for map_index in r {
                        let map_btree: BTreeMap<String, String> = map_index.iter()
                            .map(|(key, value)| (key.clone(), value.0.to_string()))
                            .collect();
                        map.extend(map_btree);
                    }
                }
            }
        }
        let explain_response = ExplainResponse {
            details: map,
        };
        Ok(JsonResponse::from(explain_response))
    }

    async fn mutation_explain(
        _configuration: &Self::Configuration,
        _state: &Self::State,
        _request: models::MutationRequest,
    ) -> Result<JsonResponse<models::ExplainResponse>, ExplainError> {
        todo!()
    }

    async fn mutation(
        _configuration: &Self::Configuration,
        _state: &Self::State,
        _request: models::MutationRequest,
    ) -> Result<JsonResponse<models::MutationResponse>, MutationError> {
        todo!()
    }

    async fn query(
        configuration: &Self::Configuration,
        state: &Self::State,
        request: models::QueryRequest,
    ) -> Result<JsonResponse<models::QueryResponse>, QueryError> {
        let variable_sets = request.variables.unwrap_or(vec![BTreeMap::new()]);
        let mut row_sets = vec![];
        let input_map: BTreeMap<String, Argument> = request.arguments.clone();
        let relationship_arguments : BTreeMap<String, models::RelationshipArgument> =
            input_map.iter()
                .map(|(key, value)|
                // Assuming we have a function `convert_to_relationship_argument`
                // that takes an argument and returns a relationship argument
                (key.clone(), convert_to_relationship_argument(value))
                )
                .collect();
        for variables in &variable_sets {
            let row_set = execute_query_with_variables(
                configuration,
                &request.collection,
                &relationship_arguments,
                &request.collection_relationships,
                &request.query,
                variables,
                &state,
                &false
            )?;
            row_sets.push(row_set);
        }
        Ok(models::QueryResponse(row_sets).into())
    }
}

fn convert_to_relationship_argument(p0: &Argument) -> RelationshipArgument {
    match p0 {
        Argument::Variable { name } => RelationshipArgument::Variable { name: name.to_string() },
        Argument::Literal { value } => RelationshipArgument::Literal { value: value.clone() }
    }
}

fn init_state(
    configuration: &CalciteConfiguration,
) -> Result<CalciteState, InitializationError> {
    dotenv::dotenv().ok();
    init_jvm(configuration);
    let java_vm = jvm::get_jvm().lock().unwrap();
        let calcite;
        let calcite_ref;
        {
            let mut env = java_vm.attach_current_thread_as_daemon().unwrap();
            calcite = calcite::create_calcite_query_engine(configuration, &mut env);
            let env = java_vm.attach_current_thread_as_daemon().unwrap();
            calcite_ref = env.new_global_ref(calcite).unwrap();
        }
        Ok(CalciteState { calcite_ref })
}
#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::path::PathBuf;

    use axum_test_helper::TestClient;
    use http::StatusCode;

    use super::*;

    #[tokio::test]
    async fn capabilities_match_ndc_spec_version() -> Result<(), Box<dyn Error + Send + Sync>> {
        let state =
            ndc_sdk::default_main::init_server_state(Calcite::default(), PathBuf::new()).await?;
        let app = ndc_sdk::default_main::create_router::<Calcite>(state, None);

        let client = TestClient::new(app);
        let response = client.get("/capabilities").send().await;

        assert_eq!(response.status(), StatusCode::OK);

        let body: ndc_models::CapabilitiesResponse = response.json().await;
        // ideally we would get this version from `ndc_models::VERSION`
        assert_eq!(body.version, "0.1.4");
        Ok(())
    }
}
