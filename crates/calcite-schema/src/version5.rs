//! Internal Configuration and state for our connector.

use std::collections::{HashMap};
use std::path::Path;
use jni::JNIEnv;
use jni::objects::{GlobalRef, JObject, JValueGen, JValueOwned};
use jni::objects::JValueGen::Object;
use ndc_models::CollectionName;
use ndc_sdk::connector::InitializationError;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tracing::{event, Level};
use ndc_calcite_values::values::{CONFIGURATION_FILENAME, CONFIGURATION_JSONSCHEMA_FILENAME};

use crate::calcite::{Model, TableMetadata};
use crate::environment::Environment;
use crate::error::{ParseConfigurationError, WriteParsedConfigurationError};
use crate::jvm::{get_jvm, init_jvm};
use crate::models::get_models;

/// Initial configuration, just enough to connect to a database and elaborate a full
/// 'Configuration'.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ParsedConfiguration {
    /// Hasura NDC version
    pub version: Version,
    /// JSON Schema file that defines a valid configuration
    #[serde(rename = "$schema")]
    pub _schema: Option<String>,
    /// The Calcite Model - somewhat dependent on type of calcite adapter being used.
    /// Better documentation can be found [here](https://calcite.apache.org/docs/model.html).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<Model>,
    /// Used internally
    pub model_file_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Certain fixes that will solve for missing field values, for non-existing fields.
    /// It's expensive and probably not necessary, but required to pass the NDC
    /// tests. You can set the value to false in order to improve performance.
    pub fixes: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "supportJsonObject")]
    pub supports_json_object: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Many common JDBC jars are included by default. Some are not you can
    /// create a directory with additional required JARS and point to that
    /// directory here.
    pub jars: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<CollectionName, TableMetadata>>
}

#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize, JsonSchema)]
pub enum Version {
    #[serde(rename = "5")]
    This,
}

impl ParsedConfiguration {
    pub fn empty() -> Self {
        Self {
            version: Version::This,
            _schema: Some(CONFIGURATION_JSONSCHEMA_FILENAME.to_string()),
            model: None,
            model_file_path: Some("/etc/connector/models/model.json".to_string()),
            fixes: Some(true),
            supports_json_object: None,
            jars: None,
            metadata: None,
        }
    }
}

pub fn create_calcite_connection<'a>(
    configuration: &ParsedConfiguration,
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
pub fn create_calcite_query_engine<'a>(configuration: &ParsedConfiguration, env: &'a mut JNIEnv<'a>) -> JObject<'a> {
    let class = env.find_class("org/kenstott/CalciteQuery").unwrap();
    let instance = env.new_object(class, "()V", &[]).unwrap();
    let _ = create_calcite_connection(configuration, &instance, env);
    event!(Level::INFO, "Instantiated Calcite Query Engine");
    return instance;
}
pub async fn introspect(
    args: ParsedConfiguration,
    _environment: impl Environment,
) -> anyhow::Result<ParsedConfiguration> {
    init_jvm(&crate::configuration::ParsedConfiguration::Version5(args.clone()));
    let calcite_ref: GlobalRef;
    {
        let java_vm = get_jvm().lock().unwrap();
        let mut env = java_vm.attach_current_thread_as_daemon().unwrap();
        let calcite = create_calcite_query_engine(&args, &mut env);
        let env = java_vm.attach_current_thread_as_daemon().unwrap();
        calcite_ref = env.new_global_ref(calcite).unwrap();
    }
    let metadata = get_models(calcite_ref);
    let introspected = ParsedConfiguration {
        version: Version::This,
        _schema: args._schema,
        model: args.model,
        model_file_path: args.model_file_path,
        fixes: args.fixes,
        supports_json_object: args.supports_json_object,
        jars: args.jars,
        metadata: Some(metadata),
    };
    Ok(introspected)
}

/// Parse the configuration format from a directory.
pub async fn parse_configuration(
    configuration_dir: impl AsRef<Path>,
) -> Result<ParsedConfiguration, ParseConfigurationError> {
    let configuration_file = configuration_dir.as_ref().join(CONFIGURATION_FILENAME);

    let configuration_file_contents =
        fs::read_to_string(&configuration_file)
            .await
            .map_err(|err| {
                ParseConfigurationError::IoErrorButStringified(format!(
                    "{}: {}",
                    &configuration_file.display(),
                    err
                ))
            })?;

    let parsed_config: ParsedConfiguration = serde_json::from_str(&configuration_file_contents)
        .map_err(|error| ParseConfigurationError::ParseError {
            file_path: configuration_file.clone(),
            line: error.line(),
            column: error.column(),
            message: error.to_string(),
        })?;

    Ok(parsed_config)
}

/// Write the parsed configuration into a directory on disk.
pub async fn write_parsed_configuration(
    parsed_config: ParsedConfiguration,
    out_dir: impl AsRef<Path>,
) -> Result<(), WriteParsedConfigurationError> {
    let configuration_file = out_dir.as_ref().to_owned().join(CONFIGURATION_FILENAME);
    fs::create_dir_all(out_dir.as_ref()).await?;

    // create the configuration file
    fs::write(
        configuration_file,
        serde_json::to_string_pretty(&parsed_config)
            .map_err(|e| WriteParsedConfigurationError::IoError(e.into()))?
            + "\n",
    )
        .await?;

    // create the jsonschema file
    let configuration_jsonschema_file_path = out_dir
        .as_ref()
        .to_owned()
        .join(CONFIGURATION_JSONSCHEMA_FILENAME);

    let output = schemars::schema_for!(ParsedConfiguration);
    fs::write(
        &configuration_jsonschema_file_path,
        serde_json::to_string_pretty(&output)
            .map_err(|e| WriteParsedConfigurationError::IoError(e.into()))?
            + "\n",
    )
        .await?;

    Ok(())
}
