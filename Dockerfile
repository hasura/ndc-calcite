# https://github.com/LukeMathWalker/cargo-chef
FROM rust:1.75.0 as chef
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
RUN apt-get update && apt-get install -y ca-certificates

# Install Java (OpenJDK) first
RUN apt-get update && apt-get install -y openjdk-21-jdk
ENV JAVA_HOME /usr/lib/jvm/java-21-openjdk-arm64

# Install Maven
RUN apt-get update && apt-get install -y maven
ENV MAVEN_HOME /usr/share/maven
ENV PATH $JAVA_HOME/bin:$MAVEN_HOME/bin:$PATH

# Your other instructions here (e.g., copying your Rust code)

WORKDIR /app
COPY --from=builder /app/target/release/ndc-calcite /usr/local/bin

RUN mkdir -p /etc/connector
ENV HASURA_CONFIGURATION_DIRECTORY=/etc/connector
ENV RUST_BACKTRACE=full

COPY calcite-rs-jni/ /calcite-rs-jni/

WORKDIR /calcite-rs-jni
RUN echo "The current working directory: $PWD"
RUN mvn -version
RUN mvn clean install
RUN mvn dependency:copy-dependencies

WORKDIR /app
#ENTRYPOINT ["mvn", "-version"]
#ENTRYPOINT ["tail", "-f", "/dev/null"]
ENTRYPOINT ["ndc-calcite"]
CMD ["serve"]

