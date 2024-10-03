ddn supergraph init test-connector
cp ./grafana/supergraph-compose.yaml ./test-connector/compose.yaml
cd test-connector
ddn connector-link add calcite --configure-host http://local.hasura.dev:8080
sed -i.bak -e '11,13d' ./app/metadata/calcite.hml
ddn run docker-start
ddn connector-link update calcite --add-all-resources
ddn supergraph build local
docker compose down
ddn run docker-start
open -a "Microsoft Edge" --args --url "https://console.hasura.io/local/graphql?url=http://localhost:3000"
