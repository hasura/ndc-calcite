//! Configuration for the connector.

use std::path::Path;

// use query_engine_metadata::metadata;

use crate::environment::Environment;
use crate::error::{
    MakeRuntimeConfigurationError, MultiError, ParseConfigurationError,
    WriteParsedConfigurationError,
};


pub const DEFAULT_CONNECTION_URI_VARIABLE: &str = "CONNECTION_URI";

/// The 'ParsedConfiguration' type models the various concrete configuration formats that are
/// currently supported.
///
/// Introducing a breaking configuration format change involves adding a new case to this type.
///
/// 'ParsedConfiguration' is used to support serialization and deserialization of an NDC
/// configuration. It retains all the salient information that constitutes an instance of an NDC
/// deployment, such that 'c = parse_configuration(dir) => { write_parsed_configuration(c, dir2) ;
/// assert(c == parse_configuration(dir2))}'.
///
/// Upgrades between different configuration format versions are version-specific functions on
/// 'ParsedConfiguration' as well.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ParsedConfiguration {
    Version1
}

#[derive(Debug, Copy, Clone)]
pub enum VersionTag {
    Version1
}

impl ParsedConfiguration {
    pub fn initial() -> Self {
        ParsedConfiguration::Version1
    }
    pub fn version(&self) -> VersionTag {
        VersionTag::Version1
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
/// 'ParsedConfiguration.
///
#[derive(Debug)]
pub struct Configuration {
    fake: String
}
pub async fn introspect(
    input: ParsedConfiguration,
    environment: impl Environment,
) -> anyhow::Result<ParsedConfiguration> {
    Ok(ParsedConfiguration::Version1)
}

pub async fn parse_configuration(
    configuration_dir: impl AsRef<Path> + Send,
) -> Result<ParsedConfiguration, ParseConfigurationError> {
    Ok(ParsedConfiguration::Version1)
}

/// Turn a 'ParsedConfiguration' into a 'Configuration', such that it may be used in main
/// NDC business logic.
///
/// Each concrete supported version implementation is responsible for interpretation its format
/// into the runtime configuration.
pub fn make_runtime_configuration(
    parsed_config: ParsedConfiguration,
    environment: impl Environment,
) -> Result<Configuration, MakeRuntimeConfigurationError> {
    Ok(Configuration{fake: "".to_string()})
}

/// Write out a parsed configuration to a directory.
pub async fn write_parsed_configuration(
    parsed_config: ParsedConfiguration,
    out_dir: impl AsRef<Path>,
) -> Result<(), WriteParsedConfigurationError> {
    Ok(())
}

/// Produce an equivalent version of a parsed configuration in the latest supported version.
///
/// This is part of the configuration crate API to enable users to upgrade their configurations
/// mechanically, using the ndc-calcite cli, when new versions are released..
pub fn upgrade_to_latest_version(parsed_config: ParsedConfiguration) -> ParsedConfiguration {
    ParsedConfiguration::Version1
}
