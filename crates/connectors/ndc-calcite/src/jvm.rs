//! # JVM interface
//!
//! Uses a singleton to initialize and re-use.
//!
//! TODO: Add logic to determine that JVM died and reinitialize.
use std::{env, fs};
use std::sync::Mutex;

use jni::{InitArgsBuilder, JavaVM, JNIVersion};
use once_cell::sync::OnceCell;
use tracing::{event, Level};

use crate::configuration::CalciteConfiguration;

static JVM: OnceCell<Mutex<JavaVM>> = OnceCell::new();
static CONFIG: OnceCell<Mutex<CalciteConfiguration>> = OnceCell::new();

/// Returns a reference to the global JVM instance.
///
/// # Panics
///
/// This function will panic if the JVM instance is not set up.
///
/// # Examples
///
/// ```
/// use std::sync::Mutex;
///
/// static JVM: once_cell::sync::OnceCell<Mutex<JavaVM>> = once_cell::sync::OnceCell::new();
///
/// pub fn get_jvm() -> &'static Mutex<JavaVM> {
///     JVM.get().expect("JVM is not set up.")
/// }
/// ```
// ANCHOR: get_jvm
pub fn get_jvm() -> &'static Mutex<JavaVM> {
    let jvm = JVM.get().expect("JVM is not set up.");
    let binding = jvm.lock().unwrap();
    let mut env = binding.attach_current_thread().unwrap();
    let _ = env.call_static_method("org/kenstott/CalciteQuery", "noOpMethod", "()V", &[]);
    if let Err(_) = env.exception_occurred() {
        env.exception_describe().expect("TODO: panic message");
        env.exception_clear().expect("TODO: panic message");
        init_jvm(&CONFIG.get().as_ref().unwrap().lock().unwrap());
        JVM.get().expect("JVM problem.")
    } else {
        JVM.get().expect("JVM is not set up.")
    }
}
// ANCHOR_END: get_jvm

/// This function initializes the Java Virtual Machine (JVM) with the given Calcite configuration.
/// It sets up the necessary JAR dependencies and JVM options based on environment variables.
///
/// # Arguments
///
/// * `calcite_configuration` - A reference to the `CalciteConfiguration` struct containing
///                             the configuration options for Calcite.
///
/// # Example
///
/// ```rust
/// use crate::CalciteConfiguration;
///
/// let config = CalciteConfiguration { ... };
/// init_jvm(&config);
/// ```
// ANCHOR: init_jvm
#[tracing::instrument]
pub fn init_jvm(calcite_configuration: &CalciteConfiguration) {
    let state_inited =  env::var("STATE_INITED").unwrap_or("false".to_string());
    if state_inited == "false" {
        let folder_path = env::var("JAR_DEPENDENCY_FOLDER").unwrap_or("/calcite-rs-jni/target/dependency".into());
        let mut jar_paths: Vec<String> = Vec::new();
        let jar_name = env::var("CALCITE_JAR").unwrap_or("/calcite-rs-jni/target/calcite-rs-jni-1.0-SNAPSHOT.jar".into());
        if !jar_name.is_empty() {
            jar_paths.push(jar_name);
        }
        if !folder_path.is_empty() {
            if let Ok(entries) = fs::read_dir(folder_path) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        let path = entry.path();
                        if path.is_file() && path.extension().map(|ext| ext == "jar").unwrap_or(false) {
                            jar_paths.push(path.to_string_lossy().to_string());
                        }
                    }
                }
            }
        }
        match &calcite_configuration.jars {
            Some(jars) => {
                if let Ok(entries) = fs::read_dir(jars) {
                    for entry in entries {
                        if let Ok(entry) = entry {
                            let path = entry.path();
                            if path.is_file() && path.extension().map(|ext| ext == "jar").unwrap_or(false) {
                                jar_paths.push(path.to_string_lossy().to_string());
                            }
                        }
                    }
                }
            },
            None => { /* handle None case if necessary */ }
        }

        let otel_exporter_otlp_endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or("http://local.hasura.dev:4317".to_string());
        let otel_service_name = env::var("OTEL_SERVICE_NAME").unwrap_or("".to_string());
        let otel_logs_exported = env::var("OTEL_LOGS_EXPORTER").unwrap_or("".to_string());
        let otel_log_level = env::var("OTEL_LOG_LEVEL").unwrap_or("".to_string());
        let log4j_configuration_file = env::var("LOG4J_CONFIGURATION_FILE").unwrap_or("/calcite-rs-jni/target/classes/log4j2.xml".to_string());
        let expanded_paths: String = jar_paths.join(":");
        let mut jvm_args = InitArgsBuilder::new()
            .version(JNIVersion::V8)
            .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
            .option("-Dotel.java.global-autoconfigure.enabled=true")
            .option(format!("-Dlog4j.configurationFile={}", log4j_configuration_file));
        if !otel_exporter_otlp_endpoint.is_empty() {
            jvm_args = jvm_args.option(
                format!("-DOTEL_EXPORTER_OTLP_ENDPOINT={}", otel_exporter_otlp_endpoint)
            );
            println!("Added {} to JVM", format!("-DOTEL_EXPORTER_OTLP_ENDPOINT={}", otel_exporter_otlp_endpoint))
        }
        if !otel_service_name.is_empty() {
            jvm_args = jvm_args.option(
                format!("-DOTEL_SERVICE_NAME={}", otel_service_name)
            );
            println!("Added {} to JVM", format!("-DOTEL_SERVICE_NAME={}", otel_service_name));
        }
        if !otel_logs_exported.is_empty() {
            jvm_args = jvm_args.option(
                format!("-DOTEL_LOGS_EXPORTED={}", otel_logs_exported)
            );
            println!("Added {} to JVM", format!("-DOTEL_LOGS_EXPORTED={}", otel_logs_exported));
        }
        if !otel_log_level.is_empty() {
            jvm_args = jvm_args.option(
                format!("-DOTEL_LOG_LEVEL={}", otel_log_level)
            );
            println!("Added {} to JVM", format!("-DOTEL_LOG_LEVEL={}", otel_log_level));
        }
        if !expanded_paths.is_empty() {
            jvm_args = jvm_args.option(
                format!("-Djava.class.path={}", &expanded_paths)
            );
            println!("Added {} to JVM", format!("-Djava.class.path={}", &expanded_paths));
        }
        let jvm_args = jvm_args.build().unwrap();
        let jvm = JavaVM::new(jvm_args).unwrap();
        JVM.set(Mutex::new(jvm)).unwrap();
        CONFIG.set(Mutex::new(calcite_configuration.clone())).unwrap();
        event!(Level::INFO, "JVM Instantiated");
        env::set_var("STATE_INITED", "true");
    }
}
// ANCHOR_END: init_jvm
