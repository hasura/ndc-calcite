export LOCAL_PATH=./config-test
export HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH=/etc/connector
rm -rf ${LOCAL_PATH}
mkdir -p ${LOCAL_PATH}
echo docker run --entrypoint ndc-calcite-cli -e HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH -v ${LOCAL_PATH}:/etc/connector docker.io/kstott/meta_connector:latest update
docker run --entrypoint ndc-calcite-cli -e HASURA_PLUGIN_CONNECTOR_CONTEXT_PATH -v ${LOCAL_PATH}:/etc/connector docker.io/kstott/meta_connector:latest update
