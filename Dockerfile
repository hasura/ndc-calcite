# build stage for cargo-chef
FROM rust:1.78.0 AS chef
WORKDIR /app
RUN cargo install cargo-chef

# planning stage
FROM chef AS planner
COPY Cargo.toml Cargo.lock crates ./app/
RUN cargo chef prepare --recipe-path recipe.json

# caching stage
FROM chef AS cacher
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --recipe-path recipe.json

# final build stage
FROM chef AS builder
COPY . .
RUN cargo build --release --bin ndc-calcite --bin ndc-calcite-cli

# java-build stage
FROM debian:trixie-slim AS java-build
COPY scripts/java_env_jdk.sh ./scripts/
RUN apt-get update && apt-get install -y openjdk-21-jdk maven ca-certificates
RUN . /scripts/java_env_jdk.sh
RUN java -version && mvn --version
COPY calcite-rs-jni/ /calcite-rs-jni/
RUN mkdir -p /root/.m2 /root/.gradle
VOLUME /root/.m2 /root/.gradle

WORKDIR /calcite-rs-jni
RUN apt-get update && apt-get install -y gradle
RUN chmod +x build.sh

# Run build.sh with detailed logging
RUN sh -x build.sh 2>&1 | tee build.log || (echo "=== Build failed. Last 50 lines of build.log: ===" && tail -n 50 build.log && exit 1)

# Put all the jars into target/dependency folder
RUN mvn dependency:copy-dependencies

# runtime stage
FROM debian:trixie-slim AS runtime
COPY scripts/java_env_jre.sh ./scripts/

RUN apt-get update &&  \
    apt-get install -y openjdk-21-jre-headless &&  \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

RUN . /scripts/java_env_jre.sh && \
    mkdir -p /calcite-rs-jni/jni/target && \
    mkdir -p /etc/ndc-calcite && \
    mkdir -p /app/connector && \
    chmod -R 666 /app/connector

COPY --from=builder /app/target/release/ndc-calcite /usr/local/bin
COPY --from=builder /app/target/release/ndc-calcite-cli /usr/local/bin
COPY --from=java-build /calcite-rs-jni/jni/target/ /calcite-rs-jni/jni/target/

ENV HASURA_CONFIGURATION_DIRECTORY=/etc/connector
ENV CONNECTOR_CONTEXT_PATH=/etc/connector
ENV RUST_BACKTRACE=full

WORKDIR /app

ENTRYPOINT ["ndc-calcite"]
CMD ["serve"]
