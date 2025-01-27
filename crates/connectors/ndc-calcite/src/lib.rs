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
//! - Parquet (testes)
//! - Cassandra (tested)
//! - CSV (tested, file://, http(s):// s3://, w/in-memory caching)
//! - JSON (tested, file://, http(s):// s3://, w/in-memory caching)
//! - XLSX (tested, file://, http(s):// s3://, w/in-memory caching)
//! - Druid (growing, niche, high interest)
//! - ElasticSearch (dup)
//! - Geode (growing, niche, high interest)
//! - InnoDB (MySQL)
//! - MongoDB (dup)
//! - Redis (todo)
//! - Solr
//! - Spark (todo)
//! - Splunk (todo)
//! - Kafka  (tested - with caveats)
//! - SQLite (tested)
//! - MSSql (dup)
//! - MySql (dup)
//! - Oracle (dup)
//! - Netezza (declining, not interesting)
//! - Redshift (tested)
//! - Infobright (abandoned, not interesting)
//! - TeraData (very commercially viable, todo)
//! - Vertica (very commercially viable, todo)
//! - Sybase (tested)
//! - StarRocks (todo)
//! - Snowflake (tested)
//! - Databricks (tested)
//! - Presto
//! - OS (interesting)
//! - Pig (HDFS Map-Reduce, interesting)
//! - Trino (tested)
//! - Phoenix (dup)
//! - Paraccel (abandoned)
//! - NeoView (abandoned, not interesting)
//! - LucidDB (abandoned, not interesting)
//! - InterBase (interesting)
//! - Ingres (declining, not interesting)
//! - Informix (niche, interesting)
//! - HSQLDB (not interesting)
//! - HIVE (JDBC, tested)
//! - H2 (tested)
//! - DB2 (tested)
//! - PostreSQL (dup, tested)
//! - Access (interesting)
//! - Exasol (growing, interesting)
//! - Firebolt (growing, interesting)
//! - SQLStream (growing, interesting)
//! - Jethro (declining, no interest)
//! - Firebird (interesting)
//! - Big Query (tested, dup)
//! - Clickhouse (dup)
//!
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
pub mod error;
pub mod query;
pub mod sql;

pub mod connector {
    pub mod calcite;
}
