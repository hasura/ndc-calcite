//! Fast and easy method to support many around 40 datasource types from a single Hasura NDC.
//!
//! Uses [`Apache Calcite`](https://calcite.apache.org/) as an-line query engine.
//!
//! Calcite has 2 personalities - it supports !~15 file data sources, particulary
//! big data file types but also some NoSQL, queues, and caches. But it also has the sql dialects for
//! ~25 data sources including things like DB2, Teradata etc.
//!
//! The full list is here:
//! - Arrow
//! - Cassandra
//! - CSV
//! - JSON
//! - Druid
//! - ElasticSearch
//! - Geode
//! - InnoDB (MySQL)
//! - MongoDB
//! - Redis
//! - Hive Files
//! - Solr
//! - Spark
//! - Splunk
//! - Kafka
//! - SQLite
//! - MSSql
//! - MySql
//! - Oracle
//! - Netezza
//! - Redshift
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
//! - HIVE (JDBC)
//! - H2
//! - PostreSQL
//!
//! This NDC cannot run independently. Calcite is a JVM-based library. There is a companion java
//! project that generates a JAR to interface between this Rust-based NDC and Apache Calcite. The Java project
//! `calcite-rs-jni` is embedded in the code repo for this project.
//!
//! There is also a Dockerfile which will compile the rust binaries, the java jars and package them
//! in a docker container.

pub mod aggregates;
pub mod calcite;
pub mod capabilities;
pub mod collections;
pub mod comparators;
pub mod jvm;
pub mod scalars;
pub mod schema;
pub mod sql;
pub mod configuration;

pub mod connector {
    pub mod calcite;
}
