# NDC Calcite Connector

The Calcite connector implements a generic connector for ALL Calcite data sources. You modify the configuration.json/model to
change the underlying Calcite data source.

## Getting Started

### With Cargo

```sh
(cd ndc-calcite; cargo run)
```

### With Docker

```sh
docker build -t calcite_connector .
docker run -it --rm -p 8080:8080 calcite_connector
```

## Using the reference connector

The calcite_connector connector runs on http://localhost:8080 by default:

```sh
curl http://localhost:8080/schema | jq .
```
