use std::collections::{BTreeMap};
use std::{fs};
use std::fs::File;
use std::io::Write;
use std::path::Path;

use async_trait::async_trait;
use dotenv;
use jni::objects::GlobalRef;
use ndc_sdk::connector::{
    Connector, ConnectorSetup, ExplainError, FetchMetricsError, HealthError, InitializationError,
    MutationError, ParseError, QueryError, SchemaError,
};
use ndc_sdk::json_response::JsonResponse;
use ndc_sdk::models;
use serde_json::to_string_pretty;
use tracing::info_span;
use tracing::Instrument;

use crate::{calcite, jvm, schema, sql};
use crate::capabilities::calcite_capabilities;
use crate::configuration::CalciteConfiguration;
use crate::jvm::init_jvm;

pub const CONFIG_FILE_NAME: &str = "configuration.json";
pub const DEV_CONFIG_FILE_NAME: &str = "dev.local.configuration.json";

#[derive(Clone, Default)]
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
    Path::new("/.dockerenv").exists()
}

#[tracing::instrument]
fn execute_query_with_variables(
    configuration: &CalciteConfiguration,
    collection: &str,
    arguments: &BTreeMap<String, models::Argument>,
    _collection_relationships: &BTreeMap<String, models::Relationship>,
    query: &models::Query,
    variables: &BTreeMap<String, serde_json::Value>,
    state: &CalciteState,
) -> Result<models::RowSet, QueryError> {
    sql::query_with_variables(configuration, collection, arguments, query, variables, state)
}

#[async_trait]
impl ConnectorSetup for Calcite {
    type Connector = Self;

    async fn parse_configuration(
        &self,
        configuration_dir: impl AsRef<Path> + Send,
    ) -> Result<<Self as Connector>::Configuration, ParseError> {
        dotenv::dotenv().ok();
        let file_path = if is_running_in_container() {
            configuration_dir.as_ref().join(CONFIG_FILE_NAME)
        } else {
            configuration_dir.as_ref().join(DEV_CONFIG_FILE_NAME)
        };
        match fs::read_to_string(file_path) {
            Ok(file_content) => {
                let mut json_object: CalciteConfiguration = serde_json::from_str(&file_content)
                    .map_err(|err| ParseError::Other(Box::from(err.to_string())))?;
                let serialized_json = to_string_pretty(&json_object.model)
                    .map_err(|err| ParseError::Other(Box::from(err.to_string())))?;
                let mut file = File::create("model.json")
                    .map_err(|err| ParseError::Other(Box::from(err.to_string())))?;
                file.write_all(serialized_json.as_bytes())
                    .map_err(|err| ParseError::Other(Box::from(err.to_string())))?;
                json_object.model_file_path = Some("./model.json".to_string());
                match json_object.metadata {
                    None => {
                        init_state(&json_object).expect("TODO: panic message");
                        Self::get_schema(&json_object).await.expect("TODO: panic message");
                        return Self::parse_configuration(self, configuration_dir).await;
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
        _configuration: &Self::Configuration,
        _state: &Self::State,
        _request: models::QueryRequest,
    ) -> Result<JsonResponse<models::ExplainResponse>, ExplainError> {
        todo!()
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
        for variables in &variable_sets {
            let row_set = execute_query_with_variables(
                configuration,
                &request.collection,
                &request.arguments,
                &request.collection_relationships,
                &request.query,
                variables,
                &state,
            )?;
            row_sets.push(row_set);
        }

        Ok(models::QueryResponse(row_sets).into())
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
