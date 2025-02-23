//! Internal Configuration and state for our connector.

use jni::objects::JValueGen::Object;
use jni::objects::{GlobalRef, JObject, JValueGen, JValueOwned};
use jni::JNIEnv;
use ndc_models::CollectionName;
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::path::Path;
use std::{error, fmt};

use ndc_calcite_values::values::{
    CONFIGURATION_FILENAME, CONFIGURATION_JSONSCHEMA_FILENAME, DOCKER_CONNECTOR_DIR,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tracing::log::debug;
use tracing::{event, Level};

use crate::calcite::{Model, TableMetadata};
use crate::configuration::has_configuration;
use crate::environment::Environment;
use crate::error::{ParseConfigurationError, WriteParsedConfigurationError};
use crate::jvm::{get_jvm, init_jvm};
use crate::models::get_models;

#[derive(Debug)]
pub enum InitializationError {
    Other(Box<dyn error::Error>),
}

impl fmt::Display for InitializationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            InitializationError::Other(ref e) => write!(f, "Other error: {}", e),
        }
    }
}

impl error::Error for InitializationError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            InitializationError::Other(ref e) => Some(&**e),
        }
    }
}
pub struct CalciteRefSingleton {
    calcite_ref: OnceCell<GlobalRef>,
}

impl CalciteRefSingleton {
    pub fn new() -> Self {
        Self {
            calcite_ref: OnceCell::new(),
        }
    }

    #[tracing::instrument(skip(self, args), level=Level::INFO)]
    pub fn initialize(
        &self,
        args: &crate::configuration::ParsedConfiguration,
    ) -> Result<(), &'static str> {
        match args {
            crate::configuration::ParsedConfiguration::Version5(config) => {
                dotenv::dotenv().ok();
                let calcite;
                let calcite_ref;
                init_jvm(args, false);
                let java_vm = get_jvm(false).lock().unwrap();
                let mut env = java_vm.attach_current_thread_as_daemon().unwrap();
                calcite = create_query_engine(&config, &mut env);
                let new_env = java_vm.attach_current_thread_as_daemon().unwrap();
                calcite_ref = new_env.new_global_ref(calcite).unwrap();
                self.calcite_ref
                    .set(calcite_ref)
                    .map_err(|e| format!("Calcite Query Engine already initialized - {e:#?}"))
                    .unwrap();
                Ok(())
            }
        }
    }

    pub fn get(&self) -> Option<&GlobalRef> {
        self.calcite_ref.get()
    }
}

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
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    #[serde(rename = "supportJsonObject", default)]
    pub supports_json_object: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Many common JDBC jars are included by default. Some are not you can
    /// create a directory with additional required JARS and point to that
    /// directory here.
    pub jars: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<CollectionName, TableMetadata>>,
}

#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize, JsonSchema)]
pub enum Version {
    #[serde(rename = "5")]
    This,
}

#[tracing::instrument(skip(configuration, env), level = Level::INFO)]
pub fn create_query_engine<'a>(
    configuration: &'a ParsedConfiguration,
    env: &'a mut JNIEnv<'a>,
) -> JObject<'a> {
    let class = env.find_class("com/hasura/CalciteQuery").unwrap();
    let instance = env.new_object(class, "()V", &[]).unwrap();
    let _ = create_jvm_connection(configuration, &instance, env)
        .expect("Failed to create JVM connection");
    event!(Level::INFO, "Instantiated Calcite Query Engine");
    instance
}

impl ParsedConfiguration {
    pub fn empty() -> Self {
        debug!("Configuration is empty.");
        Self {
            version: Version::This,
            _schema: Some(CONFIGURATION_JSONSCHEMA_FILENAME.to_string()),
            model: None,
            model_file_path: Some(
                format!("{}/models/model.json", DOCKER_CONNECTOR_DIR).to_string(),
            ),
            fixes: Some(true),
            supports_json_object: false,
            jars: None,
            metadata: None,
        }
    }
}

#[tracing::instrument(skip(configuration, calcite_query, env), level=Level::INFO)]
pub fn create_jvm_connection<'a, 'b>(
    configuration: &'a ParsedConfiguration,
    calcite_query: &'b JObject,
    env: &'b mut JNIEnv<'a>,
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

#[tracing::instrument(skip(_environment, calcite_ref_singleton))]
pub async fn introspect(
    args: &ParsedConfiguration,
    _environment: impl Environment,
    calcite_ref_singleton: &CalciteRefSingleton,
) -> anyhow::Result<ParsedConfiguration> {
    if let Err(e) = calcite_ref_singleton.initialize(
        &crate::configuration::ParsedConfiguration::Version5(args.clone()),
    ) {
        println!("Error initializing CalciteRef: {}", e);
    }
    let calcite_ref = calcite_ref_singleton.get().unwrap();
    let metadata = get_models(calcite_ref);
    let introspected = ParsedConfiguration {
        version: Version::This,
        _schema: args._schema.clone(),
        model: args.model.clone(),
        model_file_path: args.model_file_path.clone(),
        fixes: args.fixes,
        supports_json_object: args.supports_json_object,
        jars: args.jars.clone(),
        metadata: Some(metadata),
    };
    Ok(introspected)
}

/// Parse the configuration format from a directory.
#[tracing::instrument(skip(configuration_dir))]
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
#[tracing::instrument(skip(out_dir))]
pub async fn write_parsed_configuration(
    parsed_config: ParsedConfiguration,
    out_dir: impl AsRef<Path> + Send,
) -> Result<(), WriteParsedConfigurationError> {
    debug!("has_configuration: {}", has_configuration(out_dir.as_ref()));
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
