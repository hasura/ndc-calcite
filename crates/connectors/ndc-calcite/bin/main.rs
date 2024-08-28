use std::process::ExitCode;

use ndc_sdk::default_main::default_main;

use calcite::connector::calcite::Calcite;

/// Run the [`Calcite`] ndc-calcite using the [`default_main`]
/// function, which accepts standard configuration options
/// via the command line, configures metrics and trace
/// collection, and starts a server.
#[tokio::main]
pub async fn main() -> ExitCode {
    #[cfg(debug_assertions)]
    {
        env_logger::init();
    }
    match default_main::<Calcite>().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}
