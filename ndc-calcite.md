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
chmod +x *.sh
./build.sh
```

This will build the Java jars that the Rust project (at the root of this mono-repo) requires.

### Build the Connector and CLI Plugin

```shell
cd ..
cargo build --bin ndc-calcite --bin ndc-calcite-cli
```

### Test the file adapter

**Note** - Run it to perform the test.

```shell
./test.sh file # run the tests
```

## Instruction for Testing with a Supergraph using Docker

**NOTE Perform all of these operations from the root of the repo!!!**

### Build the docker image

```shell
./build-local.sh v0.1.0
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
../cli.sh ./app/connector/calcite 8081 secret
```

### Optional Revise Calcite Adapter

This will setup a SQLite connector. If you want to change the connector DO IT NOW. Go
to `app/connector/calcite/models/model.json` and revise the schema(s).
Look at the sample models for ideas, or, get more details
from [Apache Calcite](https://calcite.apache.org/docs/adapter.html).

```shell
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
ddn connector-link update calcite --add-all-resources --subgraph app/subgraph.yaml --timeout 100000
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

**NOTE Perform all of these operations from the root of the repo!!!**

### Start the standalone instance

```shell
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

| **Format**  | **Adapter Status** | **Notes**                           | **Current Status** | **Market Position** | **Primary Use Case** | **Notable Features**      | **Company** | **Initial Release** | **Latest Major Update** | **Community Support** | **Commercial Support** |
|-------------|--------------------|-------------------------------------|--------------------|---------------------|----------------------|---------------------------|-------------|---------------------|-------------------------|-----------------------|------------------------|
| **Arrow**   | Tested             | file mount                          | Growing            | Niche               | In-Memory Analytics  | High Performance          | Apache      | 2016                | 2023                    | High                  | High                   |
| **CSV**     | Tested             | s3, http, file mount, redis caching | Stable             | Mainstream          | Data Exchange        | Simple, Widely Supported  | N/A         | 1970s               | N/A                     | High                  | High                   |
| **JSON**    | Tested             | s3, http, file mount, redis caching | Stable             | Mainstream          | Data Exchange        | Flexible, Human-Readable  | N/A         | 2000s               | N/A                     | High                  | High                   |
| **XLSX**    | Tested             | s3, http, file mount, redis caching | Stable             | Mainstream          | Data Exchange        | Spreadsheet Format        | Microsoft   | 2007                | 2023                    | High                  | High                   |
| **AVRO**    | Not Interested     |                                     | Stable             | Niche               | Data Serialization   | Schema Evolution, Compact | Apache      | 2009                | 2023                    | Moderate              | Moderate               |
| **Parquet** | Tested             | file mount (s3 could be added)      | Growing            | Growing             | Big Data Analytics   | Columnar, Compression     | Apache      | 2013                | 2023                    | High                  | High                   |

## Additional Notes

All projection, filtering and sorting are handled in memory.
This means that the entire file is read into memory, and then operated on as a table scan.
Wide tables - with narrow projections may not perform as well as expected.
Large tables may not perform well.

# Databases

| **Database**   | Adapter Status | **Current Status** | **Market Position** | **Primary Use Case** | **Notable Features**     | **Company**         | **Initial Release** | **Latest Major Update** | **Community Support** | **Commercial Support** |
|----------------|----------------|--------------------|---------------------|----------------------|--------------------------|---------------------|---------------------|-------------------------|-----------------------|------------------------|
| **Cassandra**  | Tested         | Growing            | Mainstream          | NoSQL                | High Scalability         | Apache              | 2008                | 2023                    | High                  | High                   |
| **Druid**      |                | Growing            | Niche               | Real-time Analytics  | Real-time Data Ingestion | Apache              | 2015                | 2023                    | High                  | High                   |
| **Geode**      |                | Growing            | Niche               | In-Memory Data Grid  | Distributed              | Apache              | 2002                | 2023                    | High                  | High                   |
| **InnoDB**     |                | Stable             | Mainstream          | OLTP                 | Transactional            | Oracle              | 2000                | 2023                    | High                  | High                   |
| **Redis**      |                | Growing            | Mainstream          | In-Memory Data Store | High Performance         | Redis Labs          | 2009                | 2023                    | High                  | High                   |
| **Solr**       |                | Stable             | Niche               | Search               | Full-Text Search         | Apache              | 2004                | 2023                    | High                  | High                   |
| **Spark**      | Interesting    | Growing            | Mainstream          | Big Data Processing  | Distributed Processing   | Apache              | 2014                | 2023                    | High                  | High                   |
| **Splunk**     | Tested         | Growing            | Mainstream          | Log Management       | Real-time Insights       | Splunk              | 2003                | 2023                    | High                  | High                   |
| **Kafka**      |                | Growing            | Mainstream          | Stream Processing    | High Throughput          | Apache              | 2011                | 2023                    | High                  | High                   |
| **SQLite**     | Tested         | Stable             | Mainstream          | Embedded Database    | Lightweight              | SQLite Consortium   | 2000                | 2023                    | High                  | High                   |
| **Netezza**    | Not Interested | Declining          | Niche               | Data Warehousing     | High Performance         | IBM                 | 2000s               | 2022                    | Moderate              | Moderate               |
| **Redshift**   | Tested         | Growing            | Mainstream          | Data Warehousing     | Scalable                 | Amazon              | 2012                | 2023                    | High                  | High                   |
| **Infobright** | Not Interested | Abandoned          | Niche               | Analytics            | Columnar Storage         | Infobright          | 2005                | 2014                    | Low                   | None                   |
| **TeraData**   | Interesting    | Stable             | Mainstream          | Data Warehousing     | High Scalability         | Teradata            | 1979                | 2023                    | High                  | High                   |
| **Vertica**    | Interesting    | Growing            | Mainstream          | Analytics            | Columnar Storage         | Micro Focus         | 2005                | 2023                    | High                  | High                   |
| **Sybase**     | Tested         | Stable             | Mainstream          | OLTP                 | Cross-Platform           | SAP                 | 1980s               | 2023                    | Moderate              | High                   |
| **StarRocks**  | Interesting    | Growing            | Niche               | Data Warehousing     | High Performance         | StarRocks           | 2020                | 2023                    | High                  | High                   |
| **Snowflake**  | Dup            | Growing            | Mainstream          | Data Warehousing     | Serverless               | Snowflake           | 2014                | 2023                    | High                  | High                   |
| **Databricks** | Tested         | Growing            | Mainstream          | Data Warehousing     | Unified Analytics        | Databricks          | 2013                | 2023                    | High                  | High                   |
| **Presto**     |                | Growing            | Mainstream          | SQL Query Engine     | SQL on Hadoop            | PrestoDB            | 2013                | 2023                    | High                  | High                   |
| **Pig**        | Not Interested | Declining          | Niche               | HDFS Map-Reduce      | Map-Reduce               | Apache              | 2006                | 2023                    | High                  | High                   |
| **Trino**      | Tested         | Growing            | Mainstream          | SQL Query Engine     | SQL on Hadoop            | PrestoDB            | 2013                | 2023                    | High                  | High                   |
| **InterBase**  |                | Stable             | Niche               | OLTP                 | Cross-Platform           | Embarcadero         | 1980s               | 2023                    | Moderate              | High                   |
| **Ingres**     | Not Interested | Declining          | Niche               | OLTP                 | Open Source              | Actian              | 1980s               | 2022                    | Low                   | Moderate               |
| **Informix**   |                | Stable             | Niche               | OLTP                 | High Availability        | IBM                 | 1980s               | 2023                    | Moderate              | High                   |
| **HSQLDB**     | Not Interested | Declining          | Niche               | OLTP                 | Lightweight              | HSQLDB              | 2001                | 2023                    | Low                   | Moderate               |
| **HIVE**       | Tested         | Stable             | Mainstream          | SQL Query Engine     | JDBC                     | Apache              | 2010                | 2023                    | High                  | High                   |
| **H2**         | Tested         | Stable             | Niche               | OLTP                 | Lightweight              | H2                  | 2004                | 2023                    | High                  | High                   |
| **DB2**        | Tested         | Stable             | Mainstream          | OLTP                 | High Performance         | IBM                 | 1983                | 2023                    | High                  | High                   |
| **Access**     | Interesting    | Stable             | Mainstream          | OLTP                 | User-friendly            | Microsoft           | 1992                | 2023                    | High                  | High                   |
| **Exasol**     |                | Growing            | Mainstream          | Analytics            | High Performance         | Exasol              | 2000                | 2023                    | High                  | High                   |
| **Firebolt**   |                | Growing            | Mainstream          | Analytics            | High Performance         | Firebolt            | 2020                | 2023                    | High                  | High                   |
| **SQLStream**  |                | Growing            | Mainstream          | Stream Processing    | Real-time Analytics      | SQLstream           | 2009                | 2023                    | Moderate              | Moderate               |
| **Jethro**     | Not Interested | Declining          | Niche               | Analytics            | High Performance         | JethroData          | 2015                | 2020                    | Low                   | Moderate               |
| **Firebird**   |                | Stable             | Niche               | OLTP                 | Open Source              | Firebird Foundation | 2000                | 2023                    | High                  | High                   |
| **BigQuery**   | Dup/Tested     | Growing            | Mainstream          | Analytics            | Serverless               | Google              | 2010                | 2023                    | High                  | High                   |
| **Clickhouse** | Dup            | Growing            | Mainstream          | Analytics            | Columnar Storage         | Yandex              | 2016                | 2023                    | High                  | High                   |
| **Oracle**     | Dup            | Stable             | Mainstream          | Database             | High Performance         | Oracle              | 1979                | 2023                    | High                  | High                   |
| **PostgreSQL** | Dup/Tested     | Growing            | Mainstream          | Database             | Open Source              | PostgreSQL          | 1996                | 2023                    | High                  | High                   |
| **MySQL**      | Dup?           | Growing            | Mainstream          | Database             | Open Source              | Oracle              | 1995                | 2023                    | High                  | High                   |
| **MS SQL**     | Dup?           | Stable             | Mainstream          | Database             | High Performance         | Microsoft           | 1989                | 2023                    | High                  | High                   |
