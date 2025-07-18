# build stage for cargo-chef - USE ALPINE VERSION
FROM rust:1.78.0-alpine AS chef
WORKDIR /app
# Install build dependencies for Alpine including OpenSSL static libraries
RUN apk add --no-cache musl-dev gcc openssl-dev openssl-libs-static pkgconfig
# Configure OpenSSL for static linking
ENV OPENSSL_STATIC=1
ENV OPENSSL_LIB_DIR=/usr/lib
ENV OPENSSL_INCLUDE_DIR=/usr/include
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

# java-build stage - Use Amazon Corretto Alpine (no Debian repos)
FROM amazoncorretto:21-alpine AS java-build
COPY scripts/java_env_jdk.sh ./scripts/

# Install Maven and Gradle using Alpine package manager (not Debian)
RUN apk add --no-cache maven gradle

# Set up Java environment and verify installation
RUN JAVA_PATH=$(find /usr/lib/jvm -name "*corretto*" -o -name "*amazon*" -o -name "*21*" | head -1) && \
    if [ -z "$JAVA_PATH" ]; then JAVA_PATH=$(find /usr -name "java" -type f -executable | head -1 | xargs dirname | xargs dirname); fi && \
    export JAVA_HOME=$JAVA_PATH && \
    export PATH=$JAVA_HOME/bin:$PATH && \
    echo "JAVA_HOME set to: $JAVA_HOME" && \
    java -version && mvn --version

COPY calcite-rs-jni/ /calcite-rs-jni/
RUN mkdir -p /root/.m2 /root/.gradle
VOLUME /root/.m2 /root/.gradle

WORKDIR /calcite-rs-jni

RUN chmod +x build.sh

# Run build.sh with detailed logging
RUN sh -x build.sh 2>&1 | tee build.log || (echo "=== Build failed. Last 50 lines of build.log: ===" && tail -n 50 build.log && exit 1)

# Put all the jars into target/dependency folder
RUN mvn dependency:copy-dependencies

# runtime stage - Use Amazon Corretto Alpine (no Debian repos)
FROM amazoncorretto:21-alpine AS runtime
COPY scripts/java_env_jre.sh ./scripts/

# Clean Java runtime - Alpine, no Debian repositories involved
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