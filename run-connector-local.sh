cd adapters/$1
export LOG_LEVEL=debug
export OTEL_LOG_LEVEL=debug
export OTEL_LOGS_EXPORTER=console
export OTEL_METRICS_EXPORTER=none
export OTEL_TRACES_EXPORTER=console
export RUST_LOG=debug
export JAR_DEPENDENCY_FOLDER=../../calcite-rs-jni/jni/target/dependency
export CALCITE_JAR=../../calcite-rs-jni/jni/target/calcite-rs-jni-1.0-SNAPSHOT.jar
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 OTEL_SERVICE_NAME=app_calcite LOG_LEVEL=debug OTEL_METRICS_EXPORTER=console OTEL_TRACES_EXPORTER=console OTEL_LOG_EXPORTER=console RUST_LOG=info cargo run --package ndc-calcite --bin ndc-calcite -- serve --configuration=.
