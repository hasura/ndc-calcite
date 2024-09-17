export LOCAL_PATH=$(PWD)/test3/app/connector/calcite
docker run -it --entrypoint /bin/bash -e "OTEL_LOG_LEVEL=trace" -e "OTEL_LOGS_EXPORTER=console" -e "OTEL_TRACES_EXPORTER=console" -e "RUST_LOG=debug" -e "LOG_LEVEL=all" -e "HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH=${HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH}" -v ${LOCAL_PATH}:/etc/connector ghcr.io/hasura/meta_connector:latest
