# https://github.com/LukeMathWalker/cargo-chef
FROM rust:1.75.0 AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY crates crates
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG RUSTFLAGS
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --profile release --recipe-path recipe.json
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY crates crates
RUN cargo build --locked --profile release --package ndc-calcite


FROM ubuntu:latest AS runtime
RUN apt-get update && apt-get install -y ca-certificates openjdk-21-jdk maven
# Set JAVA_HOME based on the platform
ENV JAVA_HOME_ARM64=/usr/lib/jvm/java-21-openjdk-arm64 \
    JAVA_HOME_AMD64=/usr/lib/jvm/java-21-openjdk-amd64

RUN if [ "${TARGETPLATFORM}" = "linux/arm64" ] || [ "$(uname -m)" = "aarch64" ]; then \
        echo "Setting JAVA_HOME for ARM64"; \
        ln -s ${JAVA_HOME_ARM64} /usr/local/java_home; \
    else \
        echo "Setting JAVA_HOME for AMD64"; \
        ln -s ${JAVA_HOME_AMD64} /usr/local/java_home; \
    fi

ENV JAVA_HOME=/usr/local/java_home
ENV MAVEN_HOME=/usr/share/maven
ENV PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$PATH

# Verify Java installation
RUN java -version && echo $JAVA_HOME

# Your other instructions here (e.g., copying your Rust code)

WORKDIR /app
COPY --from=builder /app/target/release/ndc-calcite /usr/local/bin

RUN mkdir -p /etc/ndc-calcite
ENV HASURA_CONFIGURATION_DIRECTORY=/etc/connector
ENV RUST_BACKTRACE=full

COPY calcite-rs-jni/ /calcite-rs-jni/
COPY config-templates/ /config-templates/

WORKDIR /calcite-rs-jni/calcite
RUN ./gradlew assemble

WORKDIR /calcite-rs-jni
RUN mvn -version
RUN mvn clean
RUN mvn install -e -X
RUN mvn dependency:copy-dependencies

WORKDIR /app
ENTRYPOINT ["ndc-calcite"]
CMD ["serve"]

