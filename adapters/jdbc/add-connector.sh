ddn connector-link add "$1" --subgraph "$2" --supergraph "$3"/supergraph.yaml
mkdir "$3"/"$2"/connector
mkdir "$3"/"$2"/connector/"$1"
cp configuration.json "$3"/"$2"/connector/"$1"
cp schema.json "$3"/"$2"/connector/"$1"
cp .env.local "$3"/"$2"/connector/"$1"
export CONNECTOR="$1"
export SUBGRAPH="$2"
export UPPER_CONNECTOR=$(echo "$1" | awk '{print toupper($0)}')
export UPPER_SUBGRAPH=$(echo "$2" | awk '{print toupper($0)}')
export SUPERGRAPH="$3"
export DATA=$(readlink -f "$4")
export PORT="$5"
cat docker-compose.jdbc.yaml.template | envsubst > "$3"/"$2"/connector/"$1"/docker-compose.jdbc.yaml
cat env.template | envsubst >> "$3"/"$2"/.env."$2"
cat supergraph.template | envsubst >> "$3"/docker-compose.hasura.yaml

