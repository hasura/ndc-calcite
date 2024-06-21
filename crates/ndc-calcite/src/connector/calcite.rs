use std::collections::BTreeMap;
use std::path::Path;
use async_trait::async_trait;
use dotenv;
use jni::objects::GlobalRef;
use ndc_sdk::connector::{Connector, ConnectorSetup, ExplainError, FetchMetricsError, HealthError, InitializationError, MutationError, ParseError, QueryError, SchemaError};
use ndc_sdk::json_response::JsonResponse;
use ndc_sdk::models;
use tracing::info_span;
use tracing::Instrument;

use crate::{calcite, jvm, schema, sql};
use crate::capabilities::calcite_capabilities;
use crate::jvm::init_jvm;

#[derive(Clone, Default)]
pub struct Calcite {}

#[derive(Clone, Debug)]
pub struct CalciteState {
    pub calcite_ref: GlobalRef,
}

#[tracing::instrument]
fn execute_query_with_variables(
    collection: &str,
    arguments: &BTreeMap<String, models::Argument>,
    _collection_relationships: &BTreeMap<String, models::Relationship>,
    query: &models::Query,
    variables: &BTreeMap<String, serde_json::Value>,
    state: &CalciteState,
) -> Result<models::RowSet, QueryError> {
    sql::query_with_variables(collection, arguments, query, variables, state)
}

#[async_trait]
impl ConnectorSetup for Calcite {
    type Connector = Self;

    async fn parse_configuration(
        &self,
        _configuration_dir: impl AsRef<Path> + Send,
    ) -> Result<<Self as Connector>::Configuration, ParseError> {
        dotenv::dotenv().ok();
        Ok(())
    }

    async fn try_init_state(
        &self,
        _configuration: &<Self as Connector>::Configuration,
        _metrics: &mut prometheus::Registry,
    ) -> Result<<Self as Connector>::State, InitializationError> {
        dotenv::dotenv().ok();
        init_jvm();
        let java_vm = jvm::get_jvm().lock().unwrap();
        let calcite;
        let calcite_ref;

        {
            let mut env = java_vm.attach_current_thread_as_daemon().unwrap();
            calcite = calcite::create_calcite_query_engine(&mut env);
            let env = java_vm.attach_current_thread_as_daemon().unwrap();
            calcite_ref = env.new_global_ref(calcite).unwrap();
        }
        Ok(CalciteState { calcite_ref })
    }
}

#[async_trait]
impl Connector for Calcite {
    type Configuration = ();
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
        _configuration: &Self::Configuration,
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
            calcite = calcite::create_calcite_query_engine(&mut env);
            let env = java_vm.attach_current_thread_as_daemon().unwrap();
            calcite_ref = env.new_global_ref(calcite).unwrap();
        }

        let schema = schema::get_schema(calcite_ref.clone());
        match schema {
            Ok(schema) => Ok(schema.into()),
            Err(e) => {
                Err(SchemaError::Other(e.to_string().into()))
            }
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