use std::process::ExitCode;

use ndc_sdk::default_main::default_main;

use ndc_calcite::connector::calcite::Calcite;

/// Run the [`Calcite`] connector using the [`default_main`]
/// function, which accepts standard configuration options
/// via the command line, configures metrics and trace
/// collection, and starts a server.
#[tokio::main]
pub async fn main() -> ExitCode {
    match default_main::<Calcite>().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}