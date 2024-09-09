//! # Hasura Calcite Native Data Connector
//!
//! Fast and easy method to support around 40 datasource types from a single Hasura NDC.
//!
//! Uses [`Apache Calcite`](https://calcite.apache.org/) as an-line query engine.
//!
//! Calcite has 2 personalities - it supports !~15 file data sources, particulary
//! big data file types but also some NoSQL, queues, and caches. But it also has the sql dialects for
//! ~25 data sources including things like DB2, Teradata etc.
//!
//! The full list is here:
//! - Arrow (tested)
//! - Cassandra (tested)
//! - CSV (tested)
//! - JSON (tested)
//! - XLSX (tested)
//! - Druid
//! - ElasticSearch
//! - Geode
//! - InnoDB (MySQL)
//! - MongoDB
//! - Redis
//! - Solr
//! - Spark
//! - Splunk
//! - Kafka  (tested - with caveats)
//! - SQLite (tested)
//! - MSSql
//! - MySql
//! - Oracle
//! - Netezza
//! - Redshift (tested)
//! - Infobright
//! - TeraData
//! - Vertica
//! - Sybase
//! - StarRocks
//! - Snowflake
//! - Presto
//! - Trino
//! - Phoenix
//! - Parracel
//! - NeoView
//! - LucidDB
//! - InterBase
//! - Ingres
//! - Informix
//! - HSQLDB
//! - HIVE (JDBC, tested)
//! - H2 (tested)
//! - DB2 (tested)
//! - PostreSQL (tested)
//!
//! This NDC cannot run independently. Calcite is a JVM-based library. There is a companion java
//! project that generates a JAR to interface between this Rust-based NDC and Apache Calcite. The Java project
//! `calcite-rs-jni` is embedded in the code repo for this project.
//!
//! There is also a Dockerfile which will compile the rust binaries, the java jars and package them
//! in a docker container.
//!
//! ## Benefits
//!
//! - Include multiple data sources in a connection
//! - Define cross-data source views in the connection configuration
//! - Define star-schema aggregate materialized views - to accelerate aggregates
//! - Query planner and optimizer
//!
//! ## Limitations
//!
//! - Mutations are not supported (possible - but not yet implemented)
//! - Path'ed where-predicates (you can only use the root in List arguments)
//! - Nested objects are not supported

pub mod calcite;
pub mod capabilities;
pub mod sql;
pub mod query;

pub mod connector {
    pub mod calcite;
}

