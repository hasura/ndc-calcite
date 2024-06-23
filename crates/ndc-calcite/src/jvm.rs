use std::{env, fs};
use std::sync::Mutex;

use jni::{InitArgsBuilder, JavaVM, JNIVersion};
use once_cell::sync::OnceCell;
use tracing::{event, Level};

use crate::configuration::CalciteConfiguration;

static JVM: OnceCell<Mutex<JavaVM>> = OnceCell::new();

// ANCHOR: get_jvm
pub fn get_jvm() -> &'static Mutex<JavaVM> {
    JVM.get().expect("JVM is not set up.")
}
// ANCHOR_END: get_jvm

// ANCHOR: init_jvm
#[tracing::instrument]
pub fn init_jvm(_calcite_configuration: &CalciteConfiguration) {
    let folder_path = env::var("JAR_DEPENDENCY_FOLDER").unwrap_or("./calcite-rs-jni/target/dependency".into());
    let mut jar_paths: Vec<String> = Vec::new();
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
    let jar_name = env::var("CALCITE_JAR").unwrap_or("./calcite-rs-jni/target/calcite-rs-jni-1.0-SNAPSHOT.jar".into());
    let otel_exporter_otlp_traces_endpoint = env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").unwrap_or("".to_string());
    let otel_service_name = env::var("OTEL_SERVICE_NAME").unwrap_or("".to_string());
    let otel_logs_exported = env::var("OTEL_LOGS_EXPORTER").unwrap_or("".to_string());
    let otel_log_level = env::var("OTEL_LOG_LEVEL").unwrap_or("".to_string());
    if !jar_name.is_empty() {
        jar_paths.push(jar_name);
    }
    let expanded_paths: String = jar_paths.join(":");
    let mut jvm_args = InitArgsBuilder::new()
        .version(JNIVersion::V8)
        .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .option("-Dotel.java.global-autoconfigure.enabled=true")
        .option("-Dlog4j.configurationFile=./calcite-rs-jni/target/classes/log4j2.xml");
    if !otel_exporter_otlp_traces_endpoint.is_empty() {
        jvm_args = jvm_args.option(
            format!("-DOTEL_EXPORTER_OTLP_TRACES_ENDPOINT={}", otel_exporter_otlp_traces_endpoint)
        );
    }
    if !otel_service_name.is_empty() {
        jvm_args = jvm_args.option(
            format!("-DOTEL_SERVICE_NAME={}", otel_service_name)
        );
    }
    if !otel_logs_exported.is_empty() {
        jvm_args = jvm_args.option(
            format!("-DOTEL_LOGS_EXPORTED={}", otel_logs_exported)
        );
    }
    if !otel_log_level.is_empty() {
        jvm_args = jvm_args.option(
            format!("-DOTEL_LOG_LEVEL={}", otel_log_level)
        );
    }
    if !expanded_paths.is_empty() {
        jvm_args = jvm_args.option(["-Djava.class.path=", &expanded_paths].join(""))
    }
    let jvm_args = jvm_args.build().unwrap();
    let jvm = JavaVM::new(jvm_args).unwrap();
    JVM.set(Mutex::new(jvm)).unwrap();
    event!(Level::INFO, "JVM Instantiated")
}
// ANCHOR_END: init_jvm
