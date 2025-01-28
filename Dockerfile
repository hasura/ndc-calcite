# Build stage for cargo-chef
FROM rust:1.78.0-slim AS chef
WORKDIR /app
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef

# Planning stage
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates/
RUN cargo chef prepare --recipe-path recipe.json

# Caching stage
FROM chef AS cacher
COPY --from=planner /app/recipe.json recipe.json
RUN mkdir -p /app/target
RUN cargo chef cook --release --recipe-path recipe.json

# Final Rust build stage
FROM chef AS builder
WORKDIR /app
COPY . .

# Debug the directory structure before build
RUN ls -la

# Build with verbose output and explicit binary checking
RUN RUST_BACKTRACE=1 cargo build -vv --release --bin ndc-calcite --bin ndc-calcite-cli && \
    ls -la target/release/ && \
    test -f target/release/ndc-calcite && \
    test -f target/release/ndc-calcite-cli

# Java build stage
FROM eclipse-temurin:21-jdk-jammy AS java-build
WORKDIR /calcite-rs-jni

# Install required packages
RUN apt-get update && apt-get install -y \
    maven \
    python3 \
    python3-venv \
    python3-pip \
    ca-certificates \
    git \
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
    chmod +x calcite/gradlew
RUN DOCKER_BUILD=1 ./build.sh

# Runtime stage
FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /app

# Create required directories with proper permissions
RUN mkdir -p \
    /calcite-rs-jni/jni/target \
    /etc/ndc-calcite \
    /app/connector && \
    chmod -R 666 /app/connector

# First verify the files exist before copying
RUN mkdir -p /usr/local/bin

# Copy binaries and JARs with explicit error checking
COPY --from=builder /app/target/release/ndc-calcite /usr/local/bin/ndc-calcite
RUN test -f /usr/local/bin/ndc-calcite && chmod +x /usr/local/bin/ndc-calcite

COPY --from=builder /app/target/release/ndc-calcite-cli /usr/local/bin/ndc-calcite-cli
RUN test -f /usr/local/bin/ndc-calcite-cli && chmod +x /usr/local/bin/ndc-calcite-cli

COPY --from=java-build /calcite-rs-jni/jni/target/ /calcite-rs-jni/jni/target/

# Set environment variables
ENV HASURA_CONFIGURATION_DIRECTORY=/etc/connector \
    CONNECTOR_CONTEXT_PATH=/etc/connector \
    RUST_BACKTRACE=full

ENTRYPOINT ["ndc-calcite"]
CMD ["serve"]
