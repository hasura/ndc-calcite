# Build stage for cargo-chef
FROM rust:1.78.0-slim AS chef
WORKDIR /app
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

# Copy build script first for better layer caching
COPY calcite-rs-jni/build.sh ./
RUN chmod +x build.sh

# Copy project files
COPY calcite-rs-jni/pom.xml ./
COPY calcite-rs-jni/calcite ./calcite/
COPY calcite-rs-jni/src ./src/
COPY calcite-rs-jni/py_graphql_sql ./py_graphql_sql/

# Set environment variable to skip local-only steps
ENV DOCKER_BUILD=1

# Run build script with caching
RUN --mount=type=cache,target=/root/.m2 \
    --mount=type=cache,target=/root/.gradle \
    --mount=type=cache,target=/root/.cache/pip \
    ./build.sh 2>&1 | tee build.log || \
    (echo "=== Build failed. Last 50 lines of build.log: ===" && \
     tail -n 50 build.log && exit 1)

# Copy dependencies
RUN --mount=type=cache,target=/root/.m2 \
    mvn dependency:copy-dependencies

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