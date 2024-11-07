# Hasura SQL Integration Suite

A comprehensive collection of tools and adapters for enabling SQL access to GraphQL and other data sources through Apache Calcite.

## Components

### 1. [Hasura GraphQL SQL Adapter](calcite-rs-jni/calcite/graphql/README.md)
SQL:2003 compliant system that enables Apache Calcite to query Hasura GraphQL endpoints using SQL. Features include:
- SQL:2003 compliance with extensive feature support
- Window functions and common table expressions
- Flexible caching system (in-memory and Redis)
- Advanced query optimization
- Comprehensive type system

### 2. [GraphQL JDBC Connector](calcite-rs-jni/jdbc/README.md)
JDBC driver for SQL access to GraphQL endpoints, offering:
- Standard JDBC interface for GraphQL data
- Authentication via headers (Bearer tokens, API keys)
- Role-based access control
- Query result caching
- Integration with standard JDBC tooling

### 3. [Python DB-API](calcite-rs-jni/py_graphql_sql/README.md)
Python DB-API 2.0 implementation for SQL access to Hasura DDN endpoints:
- Python 3.9+ support
- Context manager support
- Type hints for better IDE integration
- Clear error handling
- Thread-safe design

### 4. [SQL HTTP Server](calcite-rs-jni/sqlengine/README.md)
Lightweight HTTP server for executing SQL queries via REST:
- JSON-based request/response format
- Read-only query execution
- Authentication and role-based access
- Connection pooling
- Environment-based configuration

### 5. [NDC Calcite](ndc-calcite.md)
Metadata-configurable adapter supporting ~40 data sources:
- 15+ file-based data sources
- 25+ JDBC-based data sources
- Support for various databases and file formats

## Supported Data Sources

### File Formats
| Format    | Status  | Features |
|-----------|---------|----------|
| Arrow     | Tested  | File mount, High Performance |
| CSV       | Tested  | S3, HTTP, file mount, Redis caching |
| JSON      | Tested  | S3, HTTP, file mount, Redis caching |
| XLSX      | Tested  | S3, HTTP, file mount, Redis caching |
| Parquet   | Tested  | File mount (S3 support possible) |

### Databases
Notable supported databases include:
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

## Getting Started

Each component has its own setup and configuration requirements. Please refer to the individual component documentation linked above for detailed instructions.

## Common Features Across Components

- SQL:2003 compliance
- Authentication and authorization support
- Query result caching
- Clear error handling
- Comprehensive documentation
- Production-ready implementations

## Development

For development setup and contribution guidelines, please refer to each component's documentation:
- [Calcite Adapter Contributing Guide](ndc-calcite.md#contributing)
- [JDBC Connector Development Guide](calcite-rs-jni/jdbc/README.md#building-from-source)
- [Python DB-API Development](calcite-rs-jni/py_graphql_sql/README.md#prerequisites)
- [SQL HTTP Server Development](calcite-rs-jni/sqlengine/README.md#building-and-running)
- [NDC Calcite Development](ndc-calcite.md#temporary-instructions---for-getting-started-as-a-developer-with-this-repo)

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