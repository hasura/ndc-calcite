export HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH="$(cd "$(dirname "$1")"; pwd -P)/$(basename "$1")"
filepath=$1
connector=$(basename "$filepath")
subgraph=$(echo $filepath | cut -d'/' -f2)
SUBGRAPH=$(echo $subgraph | tr '[:lower:]' '[:upper:]')
CONNECTOR=$(echo $connector | tr '[:lower:]' '[:upper:]')
content=$(cat compose.yaml)

echo $HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH
mkdir -p $HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH
#rm -rf ${HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH}
#mkdir -p ${HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH}
docker run --entrypoint ndc-calcite-cli -e "OTEL_LOG_LEVEL=trace" -e "OTEL_LOGS_EXPORTER=console" -e "OTEL_TRACES_EXPORTER=console" -e "RUST_LOG=debug" -e "LOG_LEVEL=all" -e HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH -v "${HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH}":/app/output -v "${HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH}":/etc/connector:ro ghcr.io/hasura/ndc-calcite:latest update

echo "include:\n  - path: ${filepath}/compose.yaml" > temp.yml
echo "$content" >> temp.yml
mv temp.yml compose.yaml

global_env=$(cat << EOF

${SUBGRAPH}_${CONNECTOR}_HASURA_SERVICE_TOKEN_SECRET=$3
${SUBGRAPH}_${CONNECTOR}_OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="http://local.hasura.dev:4317"
${SUBGRAPH}_${CONNECTOR}_OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="http://local.hasura.dev:4317"
${SUBGRAPH}_${CONNECTOR}_OTEL_EXPORTER_OTLP_ENDPOINT="http://local.hasura.dev:4317"
EOF
)
echo "$global_env" >> .env

rm -rf $filepath/compose.yaml
connector_compose_yaml=$(cat << EOF
services:
  ${subgraph}_${connector}:
    build:
      context: .
      dockerfile_inline: |-
        FROM ghcr.io/hasura/ndc-calcite:latest
        COPY ./ /etc/connector
    develop:
      watch:
        - path: ./
          action: sync+restart
          target: /etc/connector
    environment:
      HASURA_SERVICE_TOKEN_SECRET: \$${SUBGRAPH}_${CONNECTOR}_HASURA_SERVICE_TOKEN_SECRET
      OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: \$${SUBGRAPH}_${CONNECTOR}_OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
      OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: \$${SUBGRAPH}_${CONNECTOR}_OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
      OTEL_EXPORTER_OTLP_ENDPOINT: \$${SUBGRAPH}_${CONNECTOR}_OTEL_EXPORTER_OTLP_ENDPOINT
      OTEL_SERVICE_NAME: \$${SUBGRAPH}_${CONNECTOR}_OTEL_SERVICE_NAME
    extra_hosts:
      - local.hasura.dev=host-gateway
    ports:
      - mode: ingress
        target: 8080
        published: "$2"
        protocol: tcp
EOF
)
echo "$connector_compose_yaml" > $filepath/compose.yaml

rm -rf $filepath/connector.yaml
connector_yaml=$(cat << EOF
kind: Connector
version: v2
definition:
  name: ${connector}
  subgraph: ${subgraph}
  source: hasura/postgres:v1.1.0
  context: .
  envMapping:
    HASURA_SERVICE_TOKEN_SECRET:
      fromEnv: ${SUBGRAPH}_${CONNECTOR}_HASURA_SERVICE_TOKEN_SECRET
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT:
      fromEnv: ${SUBGRAPH}_${CONNECTOR}_OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT:
      fromEnv: ${SUBGRAPH}_${CONNECTOR}_OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
    OTEL_EXPORTER_OTLP_ENDPOINT:
      fromEnv: ${SUBGRAPH}_${CONNECTOR}_OTEL_EXPORTER_OTLP_ENDPOINT
    OTEL_SERVICE_NAME:
      fromEnv: ${SUBGRAPH}_${CONNECTOR}_OTEL_SERVICE_NAME
EOF
)
echo "$connector_yaml" > $filepath/connector.yaml


