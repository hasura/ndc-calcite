//! The interpretation of the commands that the CLI can handle.
//!
//! The CLI can do a few things. This provides a central point where those things are routed and
//! then done, making it easier to test this crate deterministically.

pub mod metadata;
pub mod environment;
pub mod configuration;
pub mod error;


use std::{env, io};
use std::path::{Path, PathBuf};

use clap::Subcommand;
use tokio::fs;
use crate::environment::Environment;


const UPDATE_ATTEMPTS: u8 = 3;

/// The various contextual bits and bobs we need to run.
pub struct Context<Env: Environment> {
    pub context_path: PathBuf,
    pub environment: Env,
    pub release_version: Option<&'static str>,
}

/// The command invoked by the user.
#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// Initialize a configuration in the current (empty) directory.
    Initialize {
        #[arg(long)]
        /// Whether to create the hasura ndc-calcite metadata.
        with_metadata: bool,
    },
    /// Update the configuration by introspecting the database, using the configuration options.
    Update,
    /// Upgrade the configuration to the latest version. This does not involve the database.
    Upgrade {
        #[arg(long)]
        dir_from: PathBuf,
        #[arg(long)]
        dir_to: PathBuf,
    }
}

/// The set of errors that can go wrong _in addition to_ generic I/O or parsing errors.
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum Error {
    #[error("directory is not empty")]
    DirectoryIsNotEmpty,
}

/// Run a command in a given directory.
pub async fn run(command: Command, context: Context<impl Environment>) -> anyhow::Result<()> {
    match command {
        Command::Initialize { with_metadata } => initialize(with_metadata, context).await?,
        Command::Update => update(context).await?,
        Command::Upgrade { dir_from, dir_to } => upgrade(dir_from, dir_to).await?,
    };
    Ok(())
}

fn copy_files(input_dir: &str, output_dir: &str) -> io::Result<()> {
    let input_path = Path::new(input_dir);
    let output_path = Path::new(output_dir);

    if !output_path.exists() {
        std::fs::create_dir_all(&output_path)?;
    }

    if input_path.is_dir() {
        for entry in std::fs::read_dir(input_path)? {
            let entry = entry?;
            let entry_path = entry.path();
            let output_file_path = output_path.join(entry_path.file_name().unwrap());
            if entry_path.is_dir() {
                copy_files(entry_path.to_str().unwrap(), output_file_path.to_str().unwrap())?;
            } else if entry_path.is_file() {
                std::fs::copy(entry_path, output_file_path)?;
            }
        }
    }
    Ok(())
}

fn is_running_in_container() -> bool {
    Path::new("/.dockerenv").exists() || env::var("KUBERNETES_SERVICE_HOST").is_ok()
}

/// Initialize an empty directory with an empty ndc-calcite configuration.
///
/// An empty configuration contains default settings and options, and is expected to be filled with
/// information such as the database connection string by the user, and later on metadata
/// information via introspection.
///
/// Optionally, this can also create the ndc-calcite metadata, which is used by the Hasura CLI to
/// automatically work with this CLI as a plugin.
async fn initialize(with_metadata: bool, context: Context<impl Environment>) -> anyhow::Result<()> {
    // refuse to initialize the directory unless it is empty
    let mut items_in_dir = fs::read_dir(&context.context_path).await?;
    if items_in_dir.next_entry().await?.is_some() {
        Err(Error::DirectoryIsNotEmpty)?;
    }

    let config_path = if is_running_in_container() {
        Path::new("/config-templates")
    } else {
        Path::new("../config-templates")
    };
    let context_path_str = context.context_path.to_str().ok_or(anyhow::anyhow!("Failed to convert PathBuf to &str"))?;
    let config_path_str = config_path.to_str().ok_or(anyhow::anyhow!("Failed to convert PathBuf to &str"))?;
    let _ = copy_files(config_path_str, context_path_str);

    configuration::write_parsed_configuration(
        configuration::ParsedConfiguration::initial(),
        &context.context_path,
    )
    .await?;

    // if requested, create the metadata
    if with_metadata {
        let metadata_dir = context.context_path.join(".hasura-ndc-calcite");
        fs::create_dir(&metadata_dir).await?;
        let metadata_file = metadata_dir.join("ndc-calcite-metadata.yaml");
        let metadata = metadata::ConnectorMetadataDefinition {
            packaging_definition: metadata::PackagingDefinition::PrebuiltDockerImage(
                metadata::PrebuiltDockerImagePackaging {
                    docker_image: format!(
                        "docker.io/kstott/meta-connector:{}",
                        context.release_version.unwrap_or("latest")
                    ),
                },
            ),
            supported_environment_variables: vec![metadata::EnvironmentVariableDefinition {
                name: "MODEL_FILE".to_string(),
                description: "The Calcite connection model".to_string(),
                default_value: Some("/etc/connection/models/model.json".to_string()),
            }],
            commands: metadata::Commands {
                update: Some("hasura-ndc-calcite update".to_string()),
                watch: None,
            },
            cli_plugin: Some(metadata::CliPluginDefinition {
                name: "ndc-calcite".to_string(),
                version: context.release_version.unwrap_or("latest").to_string(),
            }),
            docker_compose_watch: vec![metadata::DockerComposeWatchItem {
                path: "./".to_string(),
                target: Some(".".to_string()),
                action: metadata::DockerComposeWatchAction::SyncAndRestart,
                ignore: vec![],
            }],
        };

        fs::write(metadata_file, serde_yaml::to_string(&metadata)?).await?;
    }

    Ok(())
}

/// Update the configuration in the current directory by introspecting the database.
///
/// This expects a configuration with a valid connection URI.
async fn update(context: Context<impl Environment>) -> anyhow::Result<()> {
    // It is possible to change the file in the middle of introspection.
    // We want to detect this scenario and retry, or fail if we are unable to.
    // We do that with a few attempts.
    for _attempt in 1..=UPDATE_ATTEMPTS {
        let existing_configuration =
            configuration::parse_configuration(&context.context_path).await?;
        let output =
            configuration::introspect(existing_configuration.clone(), &context.environment).await?;

        // Check that the input file did not change since we started introspecting,
        let input_again_before_write =
            configuration::parse_configuration(&context.context_path).await?;

        // and skip this attempt if it has.
        if input_again_before_write == existing_configuration {
            // In order to be sure to capture default values absent in the initial input we have to
            // always write out the updated configuration.
            configuration::write_parsed_configuration(output, &context.context_path).await?;
            return Ok(());
        }

        // If we have reached here, the input file changed before writing.
    }

    // We ran out of attempts.
    Err(anyhow::anyhow!(
        "Cannot override configuration: input changed before write."
    ))
}

/// Upgrade the configuration in a directory by trying to read it and then write it back
/// out to a different directory.
///
async fn upgrade(dir_from: PathBuf, dir_to: PathBuf) -> anyhow::Result<()> {
    let old_configuration = configuration::parse_configuration(dir_from).await?;
    let upgraded_configuration = configuration::upgrade_to_latest_version(old_configuration);
    configuration::write_parsed_configuration(upgraded_configuration, dir_to).await?;

    eprintln!("Upgrade completed successfully. You may need to also run 'update'.");

    Ok(())
}
