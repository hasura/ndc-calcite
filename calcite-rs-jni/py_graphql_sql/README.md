# Python DB-API for Hasura DDN

This is a Python DB-API 2.0 compliant implementation for connecting to Hasura DDN endpoints using SQL through the Hasura GraphQL JDBC driver. It allows you to query Hasura DDN endpoints using SQL:2003 syntax through a JDBC bridge.

## Installation

```bash
# Using poetry (recommended)
poetry add python-db-api

# Or using pip
pip install python-db-api
```

## Prerequisites

1. Python 3.9 or higher
2. Java JDK 11 or higher installed and accessible in your system path
3. `graphql-jdbc-driver-1.0.0-jar-with-dependencies.jar` - this single JAR file contains all required dependencies

## Basic Usage

Here's a simple example of how to use the DB-API:

```python
from python_db_api import connect
import os

# Connection parameters
host = "http://localhost:3000/graphql"  # Your Hasura DDN endpoint
jdbc_args = {"role": "admin"}  # Connection properties

# Path to directory containing the all-in-one driver JAR
driver_paths = ["/path/to/jdbc/target"]  # Directory containing graphql-jdbc-driver-1.0.0-jar-with-dependencies.jar

# Create connection using context manager
with connect(host, jdbc_args, driver_paths) as conn:
    with conn.cursor() as cur:
        # Execute SQL:2003 query
        cur.execute("SELECT * FROM Albums")
        
        # Fetch results
        for row in cur.fetchall():
            print(f"Result: {row}")
```

## Connection Parameters

### Required Parameters

- `host`: The Hasura DDN endpoint URL (e.g., "http://localhost:3000/graphql")
- `driver_paths`: List containing the directory path where `graphql-jdbc-driver-1.0.0-jar-with-dependencies.jar` is located

### Optional Parameters

- `jdbc_args`: Dictionary of connection properties
  - Supported properties: "role", "user", "auth"
  - Example: `{"role": "admin", "auth": "bearer token"}`

## Connection Properties

You can pass various connection properties through the `jdbc_args` parameter:

```python
jdbc_args = {
    "role": "admin",      # Hasura role
    "user": "username",   # Optional username
    "auth": "token"      # Optional auth token
}
```

## Directory Structure

The driver requires a single JAR file. Example structure:

```
/path/to/jdbc/
└── target/
    └── graphql-jdbc-driver-1.0.0-jar-with-dependencies.jar
```

## Error Handling

The implementation provides clear error messages for common issues:

```python
try:
    with connect(host, jdbc_args, driver_paths) as conn:
        # ... your code ...
except DatabaseError as e:
    print(f"Database error occurred: {e}")
```

Common errors:
- Missing driver JAR file
- Invalid driver path
- Connection failures
- Invalid SQL:2003 queries

## Context Manager Support

The implementation supports both context manager and traditional connection patterns:

```python
# Using context manager (recommended)
with connect(host, jdbc_args, driver_paths) as conn:
    # ... your code ...

# Traditional approach
conn = connect(host, jdbc_args, driver_paths)
try:
    # ... your code ...
finally:
    conn.close()
```

## Type Hints

The implementation includes type hints for better IDE support and code completion:

```python
from python_db_api import connect
from typing import List

def get_connection(
    host: str,
    properties: dict[str, str],
    paths: List[str]
) -> None:
    with connect(host, properties, paths) as conn:
        # Your code here
        pass
```

## Thread Safety

The connection is not thread-safe. Each thread should create its own connection instance.

## Dependencies

- `jaydebeapi`: Java Database Connectivity (JDBC) bridge
- `jpype1`: Java to Python integration
- Java JDK 11+
- `graphql-jdbc-driver-1.0.0-jar-with-dependencies.jar`

## Limitations

- One JVM per Python process
- Cannot modify classpath after JVM starts

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.
