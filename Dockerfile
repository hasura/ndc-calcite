# Java build stage - moved earlier since it changes less frequently
FROM debian:trixie-slim AS java-build
COPY scripts/java_env_jdk.sh ./scripts/

# Install Java development tools
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    apt-get update && \
    apt-get install -y \
    openjdk-21-jdk \
    maven \
    ca-certificates \
    python3.13-venv \
    gradle && \
    rm -rf /var/lib/apt/lists/*

RUN . /scripts/java_env_jdk.sh
COPY calcite-rs-jni/ /calcite-rs-jni/

# Build Java artifacts with separate cache mounts
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    --mount=type=cache,target=/root/.gradle,sharing=locked \
    cd /calcite-rs-jni && \
    chmod +x build.sh && \
    sh -x build.sh && \
    mvn dependency:copy-dependencies

# Create a separate stage just for the Java artifacts
FROM scratch AS java-artifacts
COPY --from=java-build /calcite-rs-jni/jni/target/ /java-artifacts/

# build stage for cargo-chef
FROM rust:1.78.0 AS chef
WORKDIR /app
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    cargo install cargo-chef

# planning stage
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

# caching stage
FROM chef AS cacher
COPY --from=planner /app/recipe.json recipe.json
# Create the target directory
RUN mkdir -p /app/target
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo chef cook --recipe-path recipe.json

# final build stage
FROM chef AS builder
WORKDIR /app
COPY . .
# Create output directory
RUN mkdir -p /output/release
# Build with cache mounts and copy artifacts in the same layer
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo build --release --bin ndc-calcite --bin ndc-calcite-cli && \
    cp target/release/ndc-calcite /output/release/ && \
    cp target/release/ndc-calcite-cli /output/release/

# Create a stage for Rust artifacts
FROM scratch AS rust-artifacts
COPY --from=builder /output/release/ /rust-artifacts/

# runtime stage
FROM debian:trixie-slim AS runtime
COPY scripts/java_env_jre.sh ./scripts/

# Install Java runtime
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    apt-get update && \
    apt-get install -y openjdk-21-jre-headless && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

RUN . /scripts/java_env_jre.sh && \
    mkdir -p /calcite-rs-jni/jni/target && \
    mkdir -p /etc/ndc-calcite && \
    mkdir -p /app/connector && \
    chmod -R 666 /app/connector

# Copy binaries from rust-artifacts stage
COPY --from=rust-artifacts /rust-artifacts/ndc-calcite /usr/local/bin/
COPY --from=rust-artifacts /rust-artifacts/ndc-calcite-cli /usr/local/bin/

# Copy Java artifacts
COPY --from=java-artifacts /java-artifacts/ /calcite-rs-jni/jni/target/

ENV HASURA_CONFIGURATION_DIRECTORY=/etc/connector
ENV CONNECTOR_CONTEXT_PATH=/etc/connector
ENV RUST_BACKTRACE=full

WORKDIR /app

ENTRYPOINT ["ndc-calcite"]
CMD ["serve"]
