# GraphQL JDBC Connector

A JDBC driver that enables SQL access to GraphQL endpoints using Apache Calcite. This connector allows you to query GraphQL APIs using standard SQL syntax, making it easy to integrate GraphQL data sources with existing SQL-based tools and applications.

## Features

- Execute SQL queries against GraphQL endpoints
- Support for authentication via headers (Bearer tokens, API keys)
- Role-based access control support
- Query result caching (in-memory or Redis)
- Connection URL configuration options
- Integration with standard JDBC tooling
- Built on Apache Calcite for robust SQL support

## Installation

Add the following dependency to your project:

```xml
<dependency>
    <groupId>com.hasura</groupId>
    <artifactId>graphql-jdbc-driver</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Usage

### JDBC URL Format

The basic JDBC URL format is:

```
jdbc:graphql:<graphql-endpoint>[;<option>=<value>...]
```

Example URLs:
```
jdbc:graphql:http://localhost:8080/v1/graphql
jdbc:graphql:https://api.example.com/graphql;role=admin;auth=Bearer123
```

### Connection Options

Options can be specified in the JDBC URL after the endpoint, separated by semicolons:

#### Special Options
- `user` - GraphQL user identifier
- `role` - User role for role-based access control
- `auth` - Authentication token/header

#### Cache Options
The connector supports query result caching to improve performance. Configure caching using these operand options:

```
jdbc:graphql:http://localhost:8080/v1/graphql;operand.cache.type=memory;operand.cache.ttl=300
```

Available cache options:
- `operand.cache.type` - Cache implementation to use:
    - `memory` - In-memory cache using Guava Cache
    - `redis` - Redis-based distributed cache
    - If not specified, caching is disabled
- `operand.cache.ttl` - Cache time-to-live in seconds (defaults to 300)
- `operand.cache.url` - Redis connection URL (required if cache.type is "redis")

Example configurations:
```
# In-memory cache with 5 minute TTL
jdbc:graphql:http://localhost:8080/v1/graphql;operand.cache.type=memory;operand.cache.ttl=300

# Redis cache with 10 minute TTL
jdbc:graphql:http://localhost:8080/v1/graphql;operand.cache.type=redis;operand.cache.ttl=600;operand.cache.url=redis://localhost:6379

# Disable caching
jdbc:graphql:http://localhost:8080/v1/graphql
```

The cache is implemented as a singleton per JVM, meaning:
- For in-memory caching, the cache is shared across all connections in the same JVM
- For Redis caching, the cache can be shared across multiple JVMs using the same Redis instance

#### Operand Options
Prefix with `operand.` to pass custom options to the GraphQL adapter:
```
jdbc:graphql:http://localhost:8080/v1/graphql;operand.timeout=30;operand.maxRows=1000
```

#### Calcite Options
Prefix with `calcite.` to configure underlying Calcite behavior:
```
jdbc:graphql:http://localhost:8080/v1/graphql;calcite.caseSensitive=false
```

### Java Example

```java
// Basic connection
String url = "jdbc:graphql:http://localhost:8080/v1/graphql";
Connection conn = DriverManager.getConnection(url);

// Connection with options
String url = "jdbc:graphql:http://localhost:8080/v1/graphql;role=admin;auth=Bearer123";
Connection conn = DriverManager.getConnection(url);

// Connection with caching
String url = "jdbc:graphql:http://localhost:8080/v1/graphql;operand.cache.type=memory;operand.cache.ttl=300";
Connection conn = DriverManager.getConnection(url);

// Execute SQL query
try (Statement stmt = conn.createStatement()) {
    ResultSet rs = stmt.executeQuery("SELECT id, name, email FROM users WHERE age > 21");
    while (rs.next()) {
        System.out.println(rs.getString("name"));
    }
}
```

### Properties File Example

```properties
jdbc.driver=com.hasura.GraphQLDriver
jdbc.url=jdbc:graphql:http://localhost:8080/v1/graphql
jdbc.user=admin
jdbc.auth=Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

## Configuration Properties

| Property | Description | Default |
|----------|-------------|---------|
| `user` | GraphQL user identifier | None |
| `role` | User role for RBAC | None |
| `auth` | Authentication token/header | None |
| `operand.timeout` | Query timeout in seconds | 30 |
| `operand.maxRows` | Maximum rows to return | 1000 |
| `operand.cache.type` | Cache implementation (`memory` or `redis`) | None |
| `operand.cache.ttl` | Cache time-to-live in seconds | 300 |
| `operand.cache.url` | Redis connection URL | redis://localhost:6379 |
| `calcite.caseSensitive` | Case sensitivity for identifiers | true |
| `calcite.unquotedCasing` | Unquoted identifier casing | UNCHANGED |
| `calcite.quotedCasing` | Quoted identifier casing | UNCHANGED |

## SQL Support

The connector supports standard SQL2003 operations including:
- SELECT queries with filtering and joins
- Aggregations (COUNT, SUM, etc.)
- ORDER BY and GROUP BY clauses
- LIMIT and OFFSET

The actual SQL capabilities depend on the underlying GraphQL schema and endpoint capabilities.

You can read more about the advanced capabilities, [here](../calcite/graphql/docs/features.md).

## Building from Source

```bash
git clone https://github.com/hasura/graphql-jdbc-driver.git
cd graphql-jdbc-driver
mvn clean package
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

For issues and feature requests, please file an issue on the GitHub repository.

For commercial support, please contact support@hasura.io.