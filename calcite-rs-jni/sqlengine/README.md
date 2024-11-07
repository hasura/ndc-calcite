# SQL HTTP Server

A lightweight Java-based HTTP server that provides a REST API for executing SQL queries. This server allows clients to send read-only SQL queries via HTTP POST requests and receive results in JSON format.

## Features

- HTTP endpoint for executing read-only SQL queries
- JSON request and response format
- Authentication and role-based access control through headers
- Connection pooling and per-request database connections
- Environment-based configuration

## Prerequisites

- Java 8 or higher
- A compatible JDBC driver
- Access to a SQL database

## Environment Variables

The server requires the following environment variables:

- `JDBC_URL` (required): The JDBC connection URL for your database
- `PORT` (optional): The port number for the HTTP server (defaults to 8080)

## API Endpoints

The server exposes two identical endpoints:
- `/sql`
- `/v1/sql`

### Request Format

```json
{
    "sql": "SELECT * FROM users",
    "disallowMutations": true  // Must always be true as mutations are not supported
}
```

### Headers

The following headers are supported for authentication and authorization:

- `X-Hasura-User`: Database user
- `X-Hasura-Role`: User role
- `Authorization`: Authorization token
- `Password`: Database password

### Response Format

Successful queries return a JSON array of objects, where each object represents a row:

```json
[
    {
        "column1": "value1",
        "column2": "value2"
    },
    {
        "column1": "value3",
        "column2": "value4"
    }
]
```

## Security Features

1. **Read-Only Operations**: The JDBC driver only supports SELECT operations
2. **Per-Request Connections**: Each request uses a separate database connection
3. **Configurable Authentication**: Support for various authentication mechanisms through headers

## Error Handling

The server returns appropriate HTTP status codes and error messages:

- 200: Successful query execution
- 400: Invalid request or attempted mutation
- 405: Method not allowed (only POST is supported)
- 500: Internal server error or database error

## Building and Running

1. Compile the Java code:
```bash
javac -cp ".:path/to/dependencies/*" com/hasura/SQLHttpServer.java
```

2. Set the required environment variables:
```bash
export JDBC_URL="your-jdbc-url"
export PORT=8080  # optional
```

3. Run the server:
```bash
java -cp ".:path/to/dependencies/*" com.hasura.SQLHttpServer
```

## Example Usage

```bash
curl -X POST \
  http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -H "X-Hasura-User: myuser" \
  -H "X-Hasura-Role: admin" \
  -d '{
    "sql": "SELECT * FROM users LIMIT 5",
    "disallowMutations": true
  }'
```

## Dependencies

- `com.sun.net.httpserver`: Java's built-in HTTP server
- `org.json`: JSON parsing and generation
- `com.hasura.GraphQLDriver`: Custom JDBC driver for read-only SQL operations

## Notes

- The server uses the `com.hasura.GraphQLDriver` JDBC driver which only supports read operations
- All database operations are executed synchronously
- The server runs on a single thread (no executor is set)
- Responses are always in JSON format with UTF-8 encoding

## Error Messages

- "Method not allowed": Returned when using HTTP methods other than POST
- "Mutations not allowed": Returned when attempting mutations
- "Database Error": Returned when SQL execution fails
- "Internal Server Error": Returned for unexpected server errors

## Best Practices

1. Always set appropriate authentication headers
2. Always set `disallowMutations` to `true` in requests
3. Set proper connection timeouts in your JDBC URL
4. Monitor server logs for errors and warnings
5. Use read-only queries (SELECT statements only)

## License

This project is licensed under the MIT License.