# build stage for cargo-chef
FROM rust:1.75.0 AS chef
WORKDIR /app
RUN cargo install cargo-chef

# planning stage
FROM chef AS planner
COPY Cargo.toml .
COPY Cargo.lock .
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

# caching stage
FROM chef AS cacher
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --recipe-path recipe.json

# final build stage
FROM chef AS builder
COPY . .
RUN cargo build --release --bin ndc-calcite --bin ndc-calcite-cli

# runtime stage
FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y openjdk-21-jdk maven ca-certificates &&  \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*
# set JAVA_HOME based on architecture
ENV JAVA_HOME_ARM64=/usr/lib/jvm/java-21-openjdk-arm64
ENV JAVA_HOME_AMD64=/usr/lib/jvm/java-21-openjdk-amd64
RUN if [ "$(uname -m)" = "aarch64" ]; then \
        echo "Setting JAVA_HOME for ARM64"; \
        ln -s ${JAVA_HOME_ARM64} /usr/local/java_home; \
    else \
        echo "Setting JAVA_HOME for AMD64"; \
        ln -s ${JAVA_HOME_AMD64} /usr/local/java_home; \
    fi

ENV JAVA_HOME=/usr/local/java_home
ENV MAVEN_HOME=/usr/share/maven
ENV PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$PATH

# Verify Java and Maven installations
RUN java -version && echo $JAVA_HOME
RUN mvn --version

COPY --from=builder /app/target/release/ndc-calcite /usr/local/bin
COPY --from=builder /app/target/release/ndc-calcite-cli /usr/local/bin
RUN mkdir -p /etc/ndc-calcite

ENV HASURA_CONFIGURATION_DIRECTORY=/etc/connector
ENV RUST_BACKTRACE=full
COPY calcite-rs-jni/ /calcite-rs-jni/
WORKDIR /calcite-rs-jni/calcite

RUN ./gradlew assemble --no-daemon
WORKDIR /calcite-rs-jni
RUN mvn clean install -DskipTests &&  \
    mvn dependency:copy-dependencies &&  \
    rm -rf /root/.m2 &&  \
    rm -rf /usr/share/maven-repo &&  \
    rm -rf /calcite-rs-jni/calcite /calcite-rs-jni/src

WORKDIR /app

ENTRYPOINT ["ndc-calcite"]
CMD ["serve"]