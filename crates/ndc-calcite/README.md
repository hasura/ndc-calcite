# NDC Calcite Connector

The Calcite connector implements a generic connector for ALL Calcite data sources. You modify the configuration.json/model to
change the underlying Calcite data source.

## Getting Started

We can build a file connector (creates schemas for one or directories of JSON and CSV files). Of course, you can build all types of connectors for data sources that Calcite supports.

### With Cargo

```sh
(cd crates/ndc-calcite; cargo build; cd ..; cd ..; cd adapters/file; cargo run --package ndc-calcite --bin ndc-calcite -- serve --configuration .)
```

### With Docker

```sh
cd adapters/file
docker build -t file_connector .
docker run -it --rm -p 8080:8080 file_connector
```

## Using the reference connector

The file_connector connector runs on http://localhost:8080 by default:

```sh
curl http://localhost:8080/schema | jq .
```
