cd adapters/$1
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 OTEL_SERVICE_NAME=app_calcite LOG_LEVEL=debug OTEL_METRICS_EXPORTER=console OTEL_TRACES_EXPORTER=console OTEL_LOG_EXPORTER=console RUST_LOG=info cargo run --package ndc-calcite --bin ndc-calcite -- serve --configuration=.
