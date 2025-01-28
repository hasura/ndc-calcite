# Build stage for cargo-chef
FROM rust:1.78.0-slim AS chef
WORKDIR /app
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo install cargo-chef

# Planning stage
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates/
RUN cargo chef prepare --recipe-path recipe.json

# Caching stage
FROM chef AS cacher
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo chef cook --release --recipe-path recipe.json

# Final Rust build stage
FROM chef AS builder
COPY . .
COPY --from=cacher /app/target target
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --bin ndc-calcite --bin ndc-calcite-cli

# Java build stage
FROM eclipse-temurin:21-jdk-jammy AS java-build
WORKDIR /calcite-rs-jni

# Install required packages
RUN apt-get update && apt-get install -y \
    maven \
    gradle \
    python3 \
    python3-venv \
    python3-pip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY calcite-rs-jni/pom.xml ./
COPY calcite-rs-jni/jni ./jni/
COPY calcite-rs-jni/jni-arrow ./jni-arrow/
COPY calcite-rs-jni/bigquery ./bigquery/
COPY calcite-rs-jni/calcite ./calcite/
COPY calcite-rs-jni/jdbc ./jdbc/
COPY calcite-rs-jni/sqlengine ./sqlengine/
COPY calcite-rs-jni/py_graphql_sql ./py_graphql_sql/
COPY calcite-rs-jni/build.sh ./

# Make build script executable and run it with DOCKER_BUILD flag
RUN chmod +x build.sh && \
    DOCKER_BUILD=1 ./build.sh

# Runtime stage
FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /app

# Create required directories with proper permissions
RUN mkdir -p \
    /calcite-rs-jni/jni/target \
    /etc/ndc-calcite \
    /app/connector && \
    chmod -R 666 /app/connector

# Copy binaries and JARs
COPY --from=builder /app/target/release/ndc-calcite /usr/local/bin/
COPY --from=builder /app/target/release/ndc-calcite-cli /usr/local/bin/
COPY --from=java-build /calcite-rs-jni/jni/target/ /calcite-rs-jni/jni/target/

# Set environment variables
ENV HASURA_CONFIGURATION_DIRECTORY=/etc/connector \
    CONNECTOR_CONTEXT_PATH=/etc/connector \
    RUST_BACKTRACE=full

ENTRYPOINT ["ndc-calcite"]
CMD ["serve"]
