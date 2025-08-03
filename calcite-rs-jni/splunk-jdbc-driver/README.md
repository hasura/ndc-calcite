# Splunk JDBC Driver

A JDBC driver for Splunk that uses Apache Calcite to provide SQL query capabilities with PostgreSQL-compatible syntax.

## Features

- Standard JDBC interface for Splunk
- PostgreSQL-compatible SQL syntax
- Support for time-based queries
- Lightweight (~20MB with dependencies)

## Usage

### Connection URL Format

```
jdbc:splunk://host:port[/schema]?param1=value1&param2=value2
```

### Schema Specification

The driver supports PostgreSQL-like schema specification:

```java
// Schema in path component (like PostgreSQL database)
jdbc:splunk://host:port/schema_name

// Schema in query parameter  
jdbc:splunk://host:port?schema=schema_name

// Schema in properties
props.setProperty("schema", "schema_name");
```

**Precedence**: Query parameter > Path component > Properties

### Examples

```java
// Basic connection with schema in path
String url = "jdbc:splunk://localhost:8089/splunk";
Properties props = new Properties();
props.setProperty("user", "admin");
props.setProperty("password", "changeme");
Connection conn = DriverManager.getConnection(url, props);

// Connection with schema in query parameter
String url = "jdbc:splunk://localhost:8089?schema=splunk&cimModels=web,authentication";
Connection conn = DriverManager.getConnection(url, props);

// Query with unqualified table names (schema set as default)
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery(
    "SELECT host, source, COUNT(*) as event_count " +
    "FROM web " +  // No need for "splunk.web" - uses default schema
    "WHERE _time > CURRENT_TIMESTAMP - INTERVAL '1 hour' " +
    "GROUP BY host, source " +
    "ORDER BY event_count DESC " +
    "LIMIT 10"
);

// Or use fully qualified names
ResultSet rs2 = stmt.executeQuery(
    "SELECT COUNT(*) FROM splunk.authentication"
);
```

### Connection Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `user` or `username` | Splunk username | `admin` |
| `password` | Splunk password | Required |
| `schema` | Default schema for unqualified table names | `splunk` |
| `cimModels` | Comma-separated list of CIM models to expose as tables | All 24 models |
| `earliest` | Earliest time for queries | `-24h` |
| `latest` | Latest time for queries | `now` |
| `ssl` | Use HTTPS connection | `false` |
| `disableSslValidation` | Disable SSL certificate validation | `false` |
| `connectTimeout` | Connection timeout (ms) | `30000` |
| `socketTimeout` | Socket timeout (ms) | `60000` |

### PostgreSQL-Compatible Features

#### SQL Syntax Compatibility
- **Identifiers**: Double quotes for identifiers, e.g., `"my column"`
- **Cast operator**: `::` syntax, e.g., `_time::date`
- **Case handling**: Unquoted identifiers converted to lowercase
- **CTEs**: `WITH` clause support
- **Window functions**: `OVER` clause support
- **Array operators**: PostgreSQL array syntax
- **LIMIT/OFFSET**: Standard PostgreSQL pagination
- **String functions**: `LENGTH()`, `UPPER()`, `LOWER()`, `COALESCE()`
- **Date/time functions**: `CURRENT_TIMESTAMP`, `CURRENT_DATE`

#### Metadata Schema Compatibility
- **information_schema**: Standard SQL information schema views
  - `information_schema.tables` - Table metadata
  - `information_schema.columns` - Column metadata
  - `information_schema.schemata` - Schema information
  - `information_schema.views` - View definitions
  - `information_schema.table_constraints` - Constraint information
  - `information_schema.key_column_usage` - Key relationships

- **pg_catalog**: PostgreSQL system catalog compatibility
  - `pg_catalog.pg_tables` - Table information in PostgreSQL format
  - `pg_catalog.pg_namespace` - Schema/namespace information
  - `pg_catalog.pg_class` - Relation (table/view/index) metadata
  - `pg_catalog.pg_attribute` - Column attribute information
  - `pg_catalog.pg_type` - Data type information

#### Advanced SQL Features
- **Complex joins**: Cross-schema joins between metadata views
- **Subqueries**: IN, EXISTS, scalar subqueries
- **Aggregations**: GROUP BY, HAVING, window functions
- **Common Table Expressions**: Recursive and non-recursive CTEs
- **CASE statements**: Conditional logic expressions

### PostgreSQL Compatibility Examples

#### Metadata Discovery
```sql
-- Discover all tables (with default schema, no need to specify 'splunk.')
SELECT table_name, table_type 
FROM information_schema.tables 
WHERE table_schema = 'splunk';

-- Get column information for a specific table
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns 
WHERE table_schema = 'splunk' AND table_name = 'web'
ORDER BY ordinal_position;

-- PostgreSQL-style table listing
SELECT schemaname, tablename, tableowner 
FROM pg_catalog.pg_tables 
WHERE schemaname = 'splunk';

-- Complex metadata query with joins
SELECT t.table_name, COUNT(c.column_name) as column_count
FROM information_schema.tables t
LEFT JOIN information_schema.columns c 
  ON t.table_schema = c.table_schema 
  AND t.table_name = c.table_name
WHERE t.table_schema = 'splunk'
GROUP BY t.table_name;
```

#### Advanced SQL with PostgreSQL Syntax
```sql
-- Using CTEs and unqualified table names (default schema)
WITH recent_events AS (
  SELECT host, source, _time::date as event_date, COUNT(*) as event_count
  FROM web  -- No need for "splunk.web" with default schema
  WHERE _time::date >= CURRENT_DATE - INTERVAL '7 days'
  GROUP BY host, source, event_date
)
SELECT host, SUM(event_count) as total_events
FROM recent_events
GROUP BY host
ORDER BY total_events DESC;

-- PostgreSQL-style string operations and case handling
SELECT 
  "Host Name" as host_name,  -- Double-quoted identifier
  LENGTH(source) as source_length,
  UPPER(sourcetype) as sourcetype_upper,
  COALESCE(user, 'unknown') as user_name
FROM authentication  -- Unqualified table name
WHERE source LIKE '%access%'
LIMIT 100;

-- Complex aggregation with window functions
SELECT 
  host,
  source,
  COUNT(*) as event_count,
  ROW_NUMBER() OVER (PARTITION BY host ORDER BY COUNT(*) DESC) as host_rank,
  PERCENT_RANK() OVER (ORDER BY COUNT(*)) as percentile_rank
FROM web  -- Default schema applied automatically
GROUP BY host, source
HAVING COUNT(*) > 10;

-- Cross-table queries with unqualified names
SELECT 
  w.host,
  COUNT(w.*) as web_events,
  COUNT(a.*) as auth_events
FROM web w
FULL OUTER JOIN authentication a ON w.host = a.host
GROUP BY w.host;
```

## Building

```bash
# Ensure Calcite is built first
cd ../calcite
./gradlew assemble

# Build the driver
cd ../splunk-jdbc-driver
mvn clean package

# The JAR with dependencies will be at:
# target/splunk-jdbc-driver-1.0.0-jar-with-dependencies.jar
```

## Testing

### Unit Tests
```bash
mvn test
```

### Integration Tests
The project includes comprehensive integration tests for PostgreSQL compatibility:

- **PostgreSQLCompatibilityTest**: Tests PostgreSQL syntax compatibility, metadata access, and JDBC features
- **PostgreSQLCatalogIntegrationTest**: Tests pg_catalog schema compatibility  
- **InformationSchemaIntegrationTest**: Tests information_schema views
- **JdbcMetaDataIntegrationTest**: Tests JDBC DatabaseMetaData APIs

#### Running Specific Test Suites
```bash
# Test PostgreSQL compatibility features
mvn test -Dtest=PostgreSQLCompatibilityTest

# Test PostgreSQL catalog compatibility
mvn test -Dtest=PostgreSQLCatalogIntegrationTest

# Test information schema compatibility  
mvn test -Dtest=InformationSchemaIntegrationTest

# Test JDBC metadata APIs
mvn test -Dtest=JdbcMetaDataIntegrationTest
```

#### Integration Tests with Real Splunk
For integration tests with a real Splunk instance:
1. Update `local-properties.settings` with your Splunk connection details
2. Ensure Splunk is accessible and has data
3. Run the tests - they will automatically detect and use the real connection

If no Splunk instance is available, tests will fall back to basic Calcite connections to test PostgreSQL compatibility features.

## License

Same as the parent project.