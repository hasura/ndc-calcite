export HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH=/Users/kennethstott/Documents/GitHub/ndc-calcite/config-test
rm -rf ${HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH}
mkdir -p ${HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH}
echo docker run --entrypoint ndc-calcite-cli -e HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH -v ${HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH}:/etc/connector docker.io/kstott/meta_connector:latest update
docker run --entrypoint ndc-calcite-cli -e HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH -v ${HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH}:/etc/connector docker.io/kstott/meta_connector:latest update
