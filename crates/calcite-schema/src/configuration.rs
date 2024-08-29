//! Configuration for the connector.

use std::path::{Path, PathBuf};

use schemars::{gen::SchemaSettings, schema::RootSchema};
use tracing::Level;

use crate::environment::Environment;
use crate::error::{
    MakeRuntimeConfigurationError, ParseConfigurationError,
    WriteParsedConfigurationError,
};
use crate::list_files::list_files_in_directory;
use crate::version5;
use crate::version5::CalciteRefSingleton;
use crate::version::VersionTag;

#[tracing::instrument(skip(), level = Level::INFO)]
pub fn generate_latest_schema() -> RootSchema {
    SchemaSettings::openapi3()
        .into_generator()
        .into_root_schema_for::<version5::ParsedConfiguration>()
}

/// The 'ParsedConfiguration' type models the various concrete configuration formats that are
/// currently supported.
///
/// Introducing a breaking configuration format change involves adding a new case to this type.
///
/// 'ParsedConfiguration' is used to support serialization and deserialization of an NDC
/// configuration. It retains all the salient information that constitues an instance of an NDC
/// deployment, such that 'c = parse_configuration(dir) => { write_parsed_configuration(c, dir2) ;
/// assert(c == parse_configuration(dir2))}'.
///
/// Upgrades between different configuration format versions are version-specific functions on
/// 'ParsedConfiguration' as well.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ParsedConfiguration {
    Version5(version5::ParsedConfiguration),
}

type Configuration = ParsedConfiguration;

impl ParsedConfiguration {
    #[tracing::instrument(skip(), level = Level::INFO)]
    pub fn initial() -> Self {
        ParsedConfiguration::Version5(version5::ParsedConfiguration::empty())
    }
    #[tracing::instrument(skip_all, level = Level::INFO)]
    pub fn version(&self) -> VersionTag {
        match self {
            ParsedConfiguration::Version5(_) => VersionTag::Version5,
        }
    }
}

/// The 'Configuration' type collects all the information necessary to serve queries at runtime.
///
/// 'ParsedConfiguration' deals with a multitude of different concrete version formats, and each
/// version is responsible for interpreting its serialized format into the current 'Configuration'.
/// Values of this type are produced from a 'ParsedConfiguration' using
/// 'make_runtime_configuration'.
///
/// Separating 'ParsedConfiguration' and 'Configuration' simplifies the main query translation
/// logic by placing the responsibility of dealing with configuration format evolution in
/// 'ParsedConfiguration'.
///


pub async fn introspect(
    input: ParsedConfiguration,
    _context_path: &PathBuf,
    environment: impl Environment,
    calcite_ref: &CalciteRefSingleton,
) -> anyhow::Result<ParsedConfiguration> {
    match input {
        ParsedConfiguration::Version5(config) => Ok(ParsedConfiguration::Version5(
            version5::introspect(&config, environment, calcite_ref).await?,
        )),
    }
}

#[tracing::instrument(skip(configuration_dir))]
pub fn has_configuration(
    configuration_dir: impl AsRef<Path> + Send,
) -> bool {
    !list_files_in_directory(configuration_dir.as_ref()).unwrap().is_empty()
}

pub async fn parse_configuration(
    configuration_dir: impl AsRef<Path> + Send,
) -> Result<ParsedConfiguration, ParseConfigurationError> {
    // Try parsing each supported version in turn
    match version5::parse_configuration(configuration_dir.as_ref()).await {
        Err(v5_err) => Err(v5_err),
        Ok(config) => Ok(ParsedConfiguration::Version5(config)),
    }
}

/// Turn a 'ParsedConfiguration' into a 'Configuration', such that it may be used in main
/// NDC business logic.
///
/// Each concrete supported version implementation is responsible for interpretation its format
/// into the runtime configuration.
#[tracing::instrument(skip(parsed_config, _environment), level = Level::INFO)]
pub fn make_runtime_configuration(
    parsed_config: ParsedConfiguration,
    _environment: impl Environment,
) -> Result<Configuration, MakeRuntimeConfigurationError> {
    match parsed_config {
        ParsedConfiguration::Version5(c) => Ok(ParsedConfiguration::Version5(c)),
    }
}

/// Write out a parsed configuration to a directory.
pub async fn write_parsed_configuration(
    parsed_config: ParsedConfiguration,
    out_dir: impl AsRef<Path> + Send,
) -> Result<(), WriteParsedConfigurationError> {
    match parsed_config {
        ParsedConfiguration::Version5(c) => version5::write_parsed_configuration(c, out_dir).await,
    }
}

/// Produce an equivalent version of a parsed configuration in the latest supported version.
///
/// This is part of the configuration crate API to enable users to upgrade their configurations
/// mechanically, using the ndc-postgres cli, when new versions are released.
#[tracing::instrument(skip(parsed_config), level = Level::INFO)]
pub fn upgrade_to_latest_version(parsed_config: ParsedConfiguration) -> ParsedConfiguration {
    match parsed_config {
        ParsedConfiguration::Version5(_) => parsed_config,
    }
}
