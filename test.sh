export LOG_LEVEL=debug
export OTEL_LOG_LEVEL=debug
export OTEL_LOGS_EXPORTER=console
export OTEL_METRICS_EXPORTER=none
export OTEL_TRACES_EXPORTER=console
export RUST_LOG=debug
export JAR_DEPENDENCY_FOLDER=../../calcite-rs-jni/jni/target/dependency
export CALCITE_JAR=../../calcite-rs-jni/jni/target/calcite-rs-jni-1.0-SNAPSHOT.jar
cd adapters/"$1" || exit 1
cargo run --package ndc-calcite --bin ndc-calcite -- test --configuration=.
