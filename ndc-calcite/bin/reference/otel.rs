use std::env;
use opentelemetry::global;
use opentelemetry_sdk::trace::TracerProvider;
use tracing_subscriber::EnvFilter;

pub fn init_tracer() {

    let default_level = "info"; // default level if env variable is not set or invalid
    let level_str = env::var("OTEL_LOG_LEVEL").unwrap_or_else(|_| default_level.to_string());

    let provider = TracerProvider::builder()
        .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
        .build();

    use tracing_subscriber::{fmt, Registry};
    use tracing_subscriber::prelude::*;

    let filter = EnvFilter::new(level_str);

    let subscriber = Registry::default()
        .with(fmt::Layer::default())
        .with(filter);

    tracing::subscriber::set_global_default(subscriber).unwrap();

    global::set_tracer_provider(provider);
}