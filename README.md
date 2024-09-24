# NDC Calcite

This repository contains an adapter that is metadata configurable to support approximately 40 data sources.

Approximately 15 files-based data sources, and 25 JDBC based data sources.

## Temporary Instructions - For Getting Started as a Developer with this repo.

### Clone the repo && the subrepo

This adapter is based on a forked version of Calcite (the sub-repo)

```shell
 git clone --recurse-submodules https://github.com/hasura/ndc-calcite.git calcite-connector
 cd calcite-connector
 git checkout main
```

Note - this is somewhat simplified - because everything is in the "main" branch. I'll let you research how to manage
the primary and sub-branch on your own!

### Build the Java project

The project will require jdk 21 and maven. You need to have those installed first.

This is the JNI for calcite. It handles the Calcite to Rust handoff.

You can build it like this.
```shell
cd calcite-rs-jni
chmod +x build.sh
./build.sh
```

This will build the Java jars that the Rust project (at the root of this mono-repo) requires.

### Build the Connector and CLI Plugin

```shell
cd ..
cargo build --bin ndc-calcite --bin ndc-calcite-cli
```

### Test the file adapter

Note - the test requires "metadata" to be added to configuration.json.
It should do it on first pass, but for now - you may to run it once to populate.
Then run it again to actually perform the test.

```shell
chmod +x test.sh
./test.sh # populate metadata
./test.sh # run the tests
```

## Instruction for Testing with a Supergraph using Docker
### Build the docker image

```shell
chmod +x build-local.sh
./build-local.sh
```

### Create a Supergraph

```shell
ddn supergraph init test-connector
cd test-connector
```

### Create a connector under default subgraph "app"
```shell
mkdir ./app/connector
ddn connector-link add calcite --configure-connector-token secret --configure-host http://local.hasura.dev:8081 --subgraph app/subgraph.yaml --target-env-file .env
```

### Add metadata to the connector

This script is one-and-done, you can't redo without resetting back to prior state.
You might consider, committing before running this, to facilitate a rollback.
```shell
chmod +x ../cli.sh
../cli.sh ./app/connector/calcite 8081 secret
```

### Optional Revise Calcite Adapter

This will setup a SQLite connector. If you want to change the connector DO IT NOW. Go to `app/connector/calcite/models/model.json` and revise the schema(s).
Look at the sample models for ideas, or, get more details from [Apache Calcite](https://calcite.apache.org/docs/adapter.html).

```shell
chmod +x ../cli-update-model.sh
../cli-update-model.sh ./app/connector/calcite
```

### Start supergraph

This is to facilitate the introspection. Introspection will not work offline
with `ddn connect-link add-all`, without the connector being in connector hub.
(That's a guess, since I can't prove it.)

```shell
HASURA_DDN_PAT=$(ddn auth print-pat) docker compose --env-file .env up --build --watch
```

### Introspect

```shell
ddn connector-link update calcite --add-all-resources --subgraph app/subgraph.yaml
```

### Build supergraph

```shell
ddn supergraph build local
```

### View in console

[Click here to launch Console View](https://console.hasura.io/local/graphql?url=http://localhost:3000)

### Execute a query

```graphql
query MyQuery {
  albums(limit: 10) {
    title
    artist {
      name
    }
  }
}
```

And you should see this:

```json
{
  "data": {
    "albums": [
      {
        "title": "For Those About To Rock We Salute You",
        "artist": {
          "name": "AC/DC"
        }
      },
      {
        "title": "Balls to the Wall",
        "artist": {
          "name": "Accept"
        }
      },
      {
        "title": "Restless and Wild",
        "artist": {
          "name": "Accept"
        }
      },
      {
        "title": "Let There Be Rock",
        "artist": {
          "name": "AC/DC"
        }
      },
      {
        "title": "Big Ones",
        "artist": {
          "name": "Aerosmith"
        }
      },
      {
        "title": "Jagged Little Pill",
        "artist": {
          "name": "Alanis Morissette"
        }
      },
      {
        "title": "Facelift",
        "artist": {
          "name": "Alice In Chains"
        }
      },
      {
        "title": "Warner 25 Anos",
        "artist": {
          "name": "Antônio Carlos Jobim"
        }
      },
      {
        "title": "Plays Metallica By Four Cellos",
        "artist": {
          "name": "Apocalyptica"
        }
      },
      {
        "title": "Audioslave",
        "artist": {
          "name": "Audioslave"
        }
      }
    ]
  }
}
```

## Instructions for Testing with Supergraph using a standalone connector instance

### Start the standalone instance

```shell
chmod +x run-connector-local.sh
./run-connector-local.sh file
```
You can start any adapter by using the names of the adapter with the `./adapters` directory.

### Create a Supergraph

```shell
ddn supergraph init test-connector
cd test-connector
```

### Create the connector HML file

```shell
ddn connector-link add calcite --configure-host http://local.hasura.dev:8080
sed -i.bak -e '11,13d' ./app/metadata/calcite.hml
```
### Start the Supergraph
```shell
ddn run docker-start
```
### Introspect and add all resources
```shell
ddn connector-link update calcite --add-all-resources
```

### Build the Supergraph
```shell
ddn supergraph build local
```

### Restart the Supergraph
```shell
docker compose down
ddn run docker-start
```

### View in console

[Click here to launch Console View](https://console.hasura.io/local/graphql?url=http://localhost:3000)

# Data File Formats

| **Fact**                     | **Arrow** | **CSV** | **JSON** | **XLSX** |
|------------------------------|-----------|---------|----------|----------|
| **Current Status**           | Growing   | Stable  | Stable   | Stable   |
| **Market Position**          | Niche     | Mainstream | Mainstream | Mainstream |
| **Primary Use Case**         | In-Memory Analytics | Data Exchange | Data Exchange | Data Exchange |
| **Notable Features**         | High Performance | Simple, Widely Supported | Flexible, Human-Readable | Spreadsheet Format |
| **Company**                  | Apache    | N/A     | N/A      | Microsoft |
| **Initial Release**          | 2016      | 1970s   | 2000s    | 2007     |
| **Latest Major Update**      | 2023      | N/A     | N/A      | 2023     |
| **Community Support**        | High      | High    | High     | High     |
| **Commercial Support**       | High      | High    | High     | High     |

# Databases

| **Fact**                     | **Cassandra** | **Druid** | **Geode** | **InnoDB** | **Redis** | **Solr** | **Spark** | **Splunk** | **Kafka** | **SQLite** | **Netezza** | **Redshift** | **Infobright** | **TeraData** | **Vertica** | **Sybase** | **StarRocks** | **Snowflake** | **Databricks** | **Presto** | **Pig** | **Trino** | **InterBase** | **Ingres** | **Informix** | **HSQLDB** | **HIVE** | **H2** | **DB2** | **Access** | **Exasol** | **Firebolt** | **SQLStream** | **Jethro** | **Firebird** | **BigQuery** | **Clickhouse** |
|------------------------------|---------------|-----------|-----------|------------|-----------|----------|-----------|------------|-----------|-------------|--------------|---------------|----------------|--------------|-------------|-------------|----------------|---------------|----------------|-------------|---------|-----------|----------------|------------|---------------|-------------|----------|---------|---------|-----------|-------------|--------------|----------------|-----------|--------------|---------------|---------------|
| **Current Status**           | Growing       | Growing   | Growing   | Stable     | Growing   | Stable   | Growing   | Growing    | Growing   | Stable      | Declining    | Growing       | Abandoned      | Stable       | Growing     | Stable      | Growing        | Growing       | Growing        | Growing     | Declining | Growing   | Stable         | Declining  | Stable        | Declining   | Stable   | Stable  | Stable  | Stable    | Growing     | Growing      | Growing        | Declining | Stable       | Growing       | Growing       |
| **Market Position**          | Mainstream    | Niche     | Niche     | Mainstream | Mainstream| Niche    | Mainstream| Mainstream | Mainstream| Mainstream | Niche        | Mainstream    | Niche          | Mainstream   | Mainstream  | Mainstream | Niche          | Mainstream    | Mainstream     | Mainstream  | Niche    | Mainstream| Niche          | Niche      | Niche         | Niche       | Mainstream| Niche   | Mainstream| Mainstream| Mainstream  | Mainstream   | Mainstream     | Niche     | Niche        | Mainstream    | Mainstream    |
| **Primary Use Case**         | NoSQL         | Real-time Analytics | In-Memory Data Grid | OLTP       | In-Memory Data Store | Search    | Big Data Processing | Log Management | Stream Processing | Embedded Database | Data Warehousing | Data Warehousing | Analytics       | Data Warehousing | Analytics    | OLTP        | Data Warehousing | Data Warehousing | Data Warehousing | SQL Query Engine | HDFS Map-Reduce | SQL Query Engine | OLTP           | OLTP       | OLTP          | OLTP        | SQL Query Engine | OLTP     | OLTP     | OLTP      | Analytics    | Analytics    | Stream Processing | Analytics | OLTP         | Analytics      | Analytics      |
| **Notable Features**         | High Scalability | Real-time Data Ingestion | Distributed | Transactional | High Performance | Full-Text Search | Distributed Processing | Real-time Insights | High Throughput | Lightweight | High Performance | Scalable | Columnar Storage | High Scalability | Columnar Storage | Cross-Platform | High Performance | Serverless | Unified Analytics | SQL on Hadoop | Map-Reduce | SQL on Hadoop | Cross-Platform | Open Source | High Availability | Lightweight | JDBC | Lightweight | High Performance | User-friendly | High Performance | Real-time Analytics | High Performance | Open Source | Serverless | Columnar Storage |
| **Company**                  | Apache        | Apache    | Apache    | Oracle     | Redis Labs| Apache   | Apache    | Splunk     | Apache    | SQLite Consortium | IBM          | Amazon        | Infobright     | Teradata    | Micro Focus | SAP         | StarRocks      | Snowflake    | Databricks     | PrestoDB    | Apache   | PrestoDB  | Embarcadero    | Actian     | IBM           | HSQLDB      | Apache   | H2       | IBM      | Microsoft | Exasol      | Firebolt     | SQLstream      | JethroData | Firebird Foundation | Google         | Yandex         |
| **Initial Release**          | 2008          | 2015      | 2002      | 2000       | 2009      | 2004     | 2014      | 2003       | 2011      | 2000        | 2000s        | 2012          | 2005           | 1979        | 2005        | 1980s       | 2020           | 2014         | 2013           | 2013        | 2006      | 2013      | 1980s          | 1980s      | 1980s         | 2001        | 2010     | 2004     | 1983     | 1992       | 2000        | 2020         | 2009           | 2015      | 2000         | 2010           | 2016           |
| **Latest Major Update**      | 2023          | 2023      | 2023      | 2023       | 2023      | 2023     | 2023      | 2023       | 2023      | 2023        | 2022         | 2023          | 2014           | 2023        | 2023        | 2023        | 2023           | 2023         | 2023           | 2023        | 2023      | 2023      | 2023           | 2022       | 2023          | 2023        | 2023     | 2023     | 2023     | 2023       | 2023        | 2023         | 2020           | 2023      | 2023         | 2023           | 2023           |
| **Community Support**        | High          | High      | High      | High       | High      | High     | High      | High       | High      | High        | Moderate     | High          | Low            | High        | High        | Moderate    | High           | High         | High           | High        | High      | High      | Moderate       | Low        | Moderate       | Low         | High      | High      | High      | High       | High         | High          | Moderate       | Low        | High          | High           | High           |
| **Commercial Support**       | High          | High      | High      | High       | High      | High     | High      | High       | High      | High        | Moderate     | High          | None           | High        | High        | High        | High           | High         | High           | High        | High      | High      | High           | Moderate   | High           | Moderate    | High      | High      | High      | High       | High         | High          | Moderate       | Moderate   | High          | High           | High           |
