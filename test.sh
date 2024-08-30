export LOG_LEVEL=debug
export OTEL_LOG_LEVEL=debug
export OTEL_LOGS_EXPORTER=console
export OTEL_METRICS_EXPORTER=none
export OTEL_TRACES_EXPORTER=console
export RUST_LOG=debug
JAR_DEPENDENCY_FOLDER=../../calcite-rs-jni/target/dependency
CALCITE_JAR=../../calcite-rs-jni/target/calcite-rs-jni-1.0-SNAPSHOT.jar
cd adapters/file
cargo run --package ndc-calcite --bin ndc-calcite -- test --configuration=.
