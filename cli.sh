export HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH=/Users/kennethstott/test3/app/connector/calcite
#rm -rf ${HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH}
#mkdir -p ${HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH}
docker run --entrypoint ndc-calcite-cli -e "OTEL_LOG_LEVEL=trace" -e "OTEL_LOGS_EXPORTER=console" -e "OTEL_TRACES_EXPORTER=console" -e "RUST_LOG=debug" -e "LOG_LEVEL=all" -e HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH -v ${HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH}:/app/output -v ${HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH}:/etc/connector:ro docker.io/kstott/meta_connector:latest update
