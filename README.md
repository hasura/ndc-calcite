# Hasura Integration Suite

Tools for integrating with Hasura Data Delivery Network (DDN) in two key ways:
1. Native Data Connector (NDC) toolkit for ingesting data sources into Hasura DDN
2. SQL-based tools for querying Hasura DDN endpoints

## Native Data Connector Development

### [NDC Calcite](ndc-calcite.md)
A toolkit for creating Hasura DDN Native Data Connectors using Apache Calcite:
- Support for developing connectors to 15+ file-based data sources
- Support for developing connectors to 25+ JDBC-based data sources
- Metadata-configurable adapter framework
- Built-in testing and validation tools
- Comprehensive connector development documentation

#### Supported Data Sources for NDC Development

File Formats:
| Format    | Status  | Features |
|-----------|---------|----------|
| Arrow     | Tested  | File mount, High Performance |
| CSV       | Tested  | S3, HTTP, file mount, Redis caching |
| JSON      | Tested  | S3, HTTP, file mount, Redis caching |
| XLSX      | Tested  | S3, HTTP, file mount, Redis caching |
| Parquet   | Tested  | File mount (S3 support possible) |

Databases:
- PostgreSQL (Tested)
- Redshift (Tested)
- Databricks (Tested)
- Trino (Tested)
- HIVE (Tested)
- DB2 (Tested)
- SQLite (Tested)
- H2 (Tested)
- Cassandra (Tested)

[View full database support matrix](ndc-calcite.md#databases)

## SQL Access Tools for Hasura DDN

### [Hasura GraphQL SQL Adapter](calcite-rs-jni/calcite/graphql/README.md)
Apache Calcite adapter optimized for querying Hasura DDN endpoints using SQL:
- SQL:2003 compliance with extensive feature support
- Window functions and common table expressions
- Flexible caching system (in-memory and Redis)
- Advanced query optimization
- Comprehensive type system

### SQL Access Interfaces

#### JDBC Connector
JDBC driver for SQL access to Hasura DDN endpoints:
- Standard JDBC interface
- Authentication via headers (Bearer tokens, API keys)
- Role-based access control
- Query result caching
- Integration with standard JDBC tooling

#### Python DB-API
Python DB-API 2.0 implementation for SQL access to Hasura DDN endpoints:
- Python 3.9+ support
- Context manager support
- Type hints for better IDE integration
- Clear error handling
- Thread-safe design

#### SQL HTTP Server
Lightweight HTTP server for executing SQL queries against Hasura DDN endpoints via REST:
- JSON-based request/response format
- Read-only query execution
- Authentication and role-based access
- Connection pooling
- Environment-based configuration

## Getting Started

Each component has its own setup and configuration requirements. Please refer to the individual component documentation linked above for detailed instructions.

## Common Features

- Authentication and authorization support
- Query result caching
- Clear error handling
- Comprehensive documentation
- Production-ready implementations

## Development

For development setup and contribution guidelines, please refer to each component's documentation:
- [NDC Calcite Development](ndc-calcite.md#temporary-instructions---for-getting-started-as-a-developer-with-this-repo)
- [JDBC Connector Development Guide](calcite-rs-jni/jdbc/README.md#building-from-source)
- [Python DB-API Development](calcite-rs-jni/py_graphql_sql/README.md#prerequisites)
- [SQL HTTP Server Development](calcite-rs-jni/sqlengine/README.md#building-and-running)

## License

Different components are released under different licenses:
- Apache Calcite Adapter: Apache License 2.0
- GraphQL JDBC Connector: Apache License 2.0
- Python DB-API: MIT License
- SQL HTTP Server: MIT License
- NDC Calcite: License information not specified

## Support

For support options:
- File issues on the respective GitHub repositories
- Contact Hasura support at support@hasura.io for commercial support
- Join the community discussions on calcite.apache.org/community/