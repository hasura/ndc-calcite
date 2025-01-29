//! # Connector Definition
//!
//! Provides HTTP server paths for required NDC functions. Connecting
//! the request to the underlying code and providing the result.
//!
use async_trait::async_trait;
use dotenv;
use http::status::StatusCode;
use jni::objects::GlobalRef;
use ndc_calcite_values::values::CONFIGURATION_FILENAME;
use ndc_models as models;
use ndc_models::{Capabilities, CollectionName, VariableName};
use ndc_sdk::connector::{Connector, ConnectorSetup, ErrorResponse, Result};
use ndc_sdk::json_response::JsonResponse;
use regex::Regex;
use serde_json::Value;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::{env, fs};

use tracing::Instrument;
use tracing::{event, info_span, span, Level, Span};

use crate::capabilities::calcite_capabilities;
use crate::query::{ExplainResponse, QueryPlan};
use ndc_calcite_schema::calcite::Model;
use ndc_calcite_schema::jvm::{get_jvm, init_jvm};
use ndc_calcite_schema::schema::get_schema as retrieve_schema;
use ndc_calcite_schema::version5::ParsedConfiguration;

use crate::calcite::CalciteError;
use crate::{calcite, query};

#[derive(Clone, Default, Debug)]
pub struct Calcite {}

#[derive(Clone, Debug)]
pub struct CalciteState {
    pub calcite_ref: GlobalRef,
}

#[tracing::instrument(skip(config, coll, query, vars, state), level=Level::INFO)]
fn handle_query(
    config: &ParsedConfiguration,
    coll: &CollectionName,
    query: &models::Query,
    vars: &Option<Vec<BTreeMap<VariableName, Value>>>,
    state: &CalciteState,
    explain: bool,
) -> Result<Vec<models::RowSet>> {
    let plan = query::generate_query_plan(config, coll, query, vars, state, explain)?;
    query::execute_query_plan(&state.calcite_ref, plan)
}

const CONFIG_ERROR_MSG: &str = "Could not find model file.";

fn has_yaml_extension(filename: &str) -> bool {
    let path = Path::new(filename);
    match path.extension().and_then(OsStr::to_str) {
        Some("yml") | Some("yaml") => true,
        _ => false,
    }
}

#[async_trait]
impl ConnectorSetup for Calcite {
    type Connector = Self;

    #[tracing::instrument(skip(configuration_dir))]
    async fn parse_configuration(
        &self,
        configuration_dir: impl AsRef<Path> + Send,
    ) -> Result<<Self as Connector>::Configuration> {
        let span = span!(Level::INFO, "parse_configuration");
        dotenv::dotenv().ok();

        fn get_config_file_path(configuration_dir: impl AsRef<Path> + Send) -> PathBuf {
            configuration_dir.as_ref().join(CONFIGURATION_FILENAME)
        }

        fn configure_path(span: Span, configuration_dir: &(impl AsRef<Path> + Send)) {
            span.record(
                "configuration_dir",
                format!("{:?}", configuration_dir.as_ref().display()),
            );
        }

        fn parse_json<T: Connector<Configuration = ParsedConfiguration>>(
            json_str: String,
        ) -> Result<T::Configuration> {
            let mut json_object: ParsedConfiguration =
                serde_json::from_str(&json_str).map_err(|err| ErrorResponse::from_error(err))?;

            update_model(&mut json_object)?;

            Ok(json_object)
        }

        fn update_model(json_object: &mut ParsedConfiguration) -> Result<()> {
            let model_file_path = json_object
                .model_file_path
                .clone()
                .or_else(|| env::var("MODEL_FILE").ok())
                .ok_or(ErrorResponse::new(
                    StatusCode::from_u16(500).unwrap(),
                    CONFIG_ERROR_MSG.to_string(),
                    Value::String(String::new()),
                ))?;

            let mut models = fs::read_to_string(model_file_path.clone()).unwrap();

            for (key, value) in std::env::vars() {
                let env_var_identifier = format!("${{{}}}", key);
                models = models.replace(&env_var_identifier, &value);
            }

            // Create a regex pattern to match `${*}`
            let re = Regex::new(r"\$\{.*?\}").unwrap();
            // check if there is any placeholder left in the model file, which means
            // there is an extra env var which is not allowed in the metadata or there is
            // a mismatch between the two files.
            let final_model_string = if re.is_match(&models) {
                Err(ErrorResponse::from_error(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Some environment variable placeholders are not updated in the model file",
                )))
            } else {
                Ok(&models)
            }?;

            if has_yaml_extension(&model_file_path.clone()) {
                let model_object: Model =
                    serde_yaml::from_str(final_model_string).map_err(ErrorResponse::from_error)?;
                json_object.model = Some(model_object);
            } else {
                let model_object: Model =
                    serde_json::from_str(final_model_string).map_err(ErrorResponse::from_error)?;
                json_object.model = Some(model_object);
            }

            Ok(())
        }

        configure_path(span, &configuration_dir);
        match fs::read_to_string(get_config_file_path(configuration_dir)) {
            Ok(file_content) => parse_json::<Self>(file_content),
            Err(err) => Err(ErrorResponse::from_error(err)),
        }
    }

    async fn try_init_state(
        &self,
        configuration: &<Self as Connector>::Configuration,
        _metrics: &mut prometheus::Registry,
    ) -> Result<<Self as Connector>::State> {
        init_state(configuration)
    }
}

#[async_trait]
impl Connector for Calcite {
    type Configuration = ParsedConfiguration;
    type State = CalciteState;

    fn fetch_metrics(_configuration: &Self::Configuration, _state: &Self::State) -> Result<()> {
        Ok(())
    }

    async fn get_health_readiness(
        _configuration: &Self::Configuration,
        _state: &Self::State,
    ) -> Result<()> {
        Ok(())
    }

    async fn get_capabilities() -> Capabilities {
        calcite_capabilities().capabilities
    }

    async fn get_schema(
        configuration: &Self::Configuration,
    ) -> Result<JsonResponse<models::SchemaResponse>> {
        async {
            info_span!("inside tracing Calcite");
        }
        .instrument(info_span!("tracing Calcite"))
        .await;
        dotenv::dotenv().ok();
        let calcite;
        let calcite_ref;
        {
            let java_vm = get_jvm(false).lock().unwrap();
            let mut env = java_vm.attach_current_thread_as_daemon().unwrap();
            calcite = calcite::create_query_engine(configuration, &mut env).or(Err(
                ErrorResponse::from_error(CalciteError {
                    message: String::from("Failed to lock JVM"),
                }),
            ))?;
            let env = java_vm.attach_current_thread_as_daemon().unwrap();
            calcite_ref = env.new_global_ref(calcite).unwrap();
        }

        let schema = retrieve_schema(configuration, calcite_ref);
        match schema {
            Ok(schema) => Ok(JsonResponse::from(schema)),
            Err(_) => Err(ErrorResponse::new(
                StatusCode::from_u16(500).unwrap(),
                "Problem getting schema.".to_string(),
                Value::String("".to_string()),
            )),
        }
    }

    async fn query_explain(
        configuration: &Self::Configuration,
        state: &Self::State,
        request: models::QueryRequest,
    ) -> Result<JsonResponse<models::ExplainResponse>> {
        let variable_sets = request.variables;
        let mut details: BTreeMap<String, String> = BTreeMap::new();
        let plan = query::generate_query_plan(
            &configuration,
            &request.collection,
            &request.query,
            &variable_sets,
            state,
            true,
        )?;
        let QueryPlan {
            aggregate_query,
            row_query,
            ..
        } = &plan;

        if let Some(aggregate) = aggregate_query {
            details.insert("aggregate_query".to_string(), aggregate.to_string());
        }

        if let Some(row) = row_query.as_ref() {
            details.insert("row_query".to_string(), row.clone());
        }

        let ExplainResponse {
            rows_explain,
            aggregates_explain,
        } = query::explain_query_plan(state.calcite_ref.clone(), plan)?;

        if let Some(aggregate) = aggregates_explain {
            details.insert("aggregates_explain".to_string(), aggregate);
        }

        if let Some(row) = rows_explain {
            details.insert("rows_explain".to_string(), row);
        }

        let explain_response = models::ExplainResponse { details };
        Ok(JsonResponse::from(explain_response))
    }

    async fn mutation_explain(
        _configuration: &Self::Configuration,
        _state: &Self::State,
        _request: models::MutationRequest,
    ) -> Result<JsonResponse<models::ExplainResponse>> {
        todo!()
    }

    async fn mutation(
        _configuration: &Self::Configuration,
        _state: &Self::State,
        _request: models::MutationRequest,
    ) -> Result<JsonResponse<models::MutationResponse>> {
        todo!()
    }

    async fn query(
        configuration: &Self::Configuration,
        state: &Self::State,
        request: models::QueryRequest,
    ) -> Result<JsonResponse<models::QueryResponse>> {
        let variable_sets = request.variables;

        let plan = query::generate_query_plan(
            &configuration,
            &request.collection,
            &request.query,
            &variable_sets,
            state,
            false,
        )?;
        let query_response = query::execute_query_plan(&state.calcite_ref, plan);

        let row_sets = match query_response {
            Ok(row_set) => {
                event!(Level::INFO, result = "execute_query_plan was successful");
                row_set
            }
            Err(e) => {
                event!(Level::ERROR, "Error executing query: {:?}", e);
                return Err(e.into());
            }
        };

        Ok(models::QueryResponse(row_sets).into())
    }
}

fn init_state(configuration: &ParsedConfiguration) -> Result<CalciteState> {
    dotenv::dotenv().ok();
    init_jvm(
        &ndc_calcite_schema::configuration::ParsedConfiguration::Version5(configuration.clone()),
        true,
    );
    let jvm = get_jvm(true);
    match jvm.lock() {
        Ok(java_vm) => {
            let calcite;
            let calcite_ref;
            {
                let mut env = java_vm.attach_current_thread_as_daemon().unwrap();
                calcite = calcite::create_query_engine(configuration, &mut env).or(Err(
                    ErrorResponse::from_error(CalciteError {
                        message: String::from("Failed to get Calcite engine."),
                    }),
                ))?;
                let env = java_vm.attach_current_thread_as_daemon().unwrap();
                calcite_ref = env.new_global_ref(calcite).unwrap();
            }
            Ok(CalciteState { calcite_ref })
        }
        Err(_) => Err(ErrorResponse::from_error(CalciteError {
            message: "Unable to get mutex lock on JVM.".to_string(),
        })),
    }
}
#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use axum_test_helper::TestClient;
    use http::StatusCode;

    use super::*;

    #[tokio::test]
    async fn capabilities_match_ndc_spec_version() -> Result<()> {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let config_dir = PathBuf::from(manifest_dir).join("test_configuration");

        let state =
            ndc_sdk::default_main::init_server_state(Calcite::default(), config_dir).await?;
        let app = ndc_sdk::default_main::create_router::<Calcite>(state, None);

        let client = TestClient::new(app);
        let response = client.get("/capabilities").send().await;

        assert_eq!(response.status().as_u16(), StatusCode::OK.as_u16());

        let body: ndc_models::CapabilitiesResponse = response.json().await;
        // ideally we would get this version from `ndc_models::VERSION`
        assert_eq!(body.version, "0.1.4");
        Ok(())
    }
}
