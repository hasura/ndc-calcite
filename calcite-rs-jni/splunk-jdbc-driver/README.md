# Splunk JDBC Driver

A JDBC driver for Splunk that uses Apache Calcite to provide SQL query capabilities with PostgreSQL-compatible syntax. This driver supports dynamic data model discovery, custom table definitions, and enhanced authentication options including token-based authentication.

## Features

- **Dynamic Data Model Discovery**: Automatically discovers and exposes Splunk data models as tables
- **Custom Table Definitions**: Define custom tables with specific schemas via JSON configuration
- **Multiple Authentication Methods**: Support for username/password and token-based authentication
- **SSL/TLS Support**: Configurable SSL with optional certificate validation
- **PostgreSQL Compatibility**: Full support for PostgreSQL SQL syntax and metadata schemas
- **Standard JDBC Interface**: Works with any JDBC-compatible tool or application
- **Lightweight**: ~20MB with dependencies

## Installation

### Maven Central

Add the dependency to your project:

#### Maven
```xml
<dependency>
    <groupId>com.kenstott.components</groupId>
    <artifactId>splunk-jdbc-driver</artifactId>
    <version>1.1.0</version>
</dependency>
```

#### Gradle
```groovy
implementation 'com.kenstott.components:splunk-jdbc-driver:1.1.0'
```

#### SBT
```scala
libraryDependencies += "com.kenstott.components" % "splunk-jdbc-driver" % "1.1.0"
```

### Direct JAR Download

Download from Maven Central:
- [Fat JAR with dependencies](https://repo1.maven.org/maven2/com/kenstott/components/splunk-jdbc-driver/1.1.0/splunk-jdbc-driver-1.1.0-jar-with-dependencies.jar) (Recommended for tools like DataGrip)
- [Main JAR](https://repo1.maven.org/maven2/com/kenstott/components/splunk-jdbc-driver/1.1.0/splunk-jdbc-driver-1.1.0.jar) (Use with Maven/Gradle)

### Driver Class

```java
// Register the driver
Class.forName("com.hasura.SplunkDriver");

// Or let DriverManager auto-discover it
Connection conn = DriverManager.getConnection("jdbc:splunk://host:port", props);
```

### DataGrip Setup

**Download DataGrip**: [JetBrains DataGrip](https://www.jetbrains.com/datagrip/)

1. **Download the Fat JAR**: Use the jar-with-dependencies JAR from Maven Central
2. **Add Driver**: In DataGrip, go to Database → New → Driver
3. **Configure Driver**:
   - **Name**: Splunk
   - **Driver Class**: `com.hasura.SplunkDriver`
   - **Driver Files**: Add the downloaded JAR
4. **Java Compatibility**: If you get "SequencedCollection not found" error:
   - The driver requires Java 21 for DataGrip
   - Configure DataGrip to use Java 21 runtime following the DataGrip documentation
5. **Connection URL**: `jdbc:splunk://your-splunk-host:8089?schema=splunk&disableSslValidation=true`

### DBeaver Setup

**Download DBeaver**: [DBeaver Community Edition](https://dbeaver.io/download/) (Free) or [DBeaver PRO](https://dbeaver.com/download/) (Commercial)

1. **Download the Fat JAR**: Use the jar-with-dependencies JAR from Maven Central
2. **Java Compatibility**: DBeaver 23.0+ includes Java 21 runtime by default, which is compatible with this driver
3. **Add Driver**: 
   - Go to Database → Driver Manager
   - Click "New" to create a new driver
4. **Configure Driver**:
   - **Driver Name**: Splunk
   - **Class Name**: `com.hasura.SplunkDriver`
   - **URL Template**: `jdbc:splunk://{host}[:{port}][/{database}]`
   - **Default Port**: 8089
5. **Add JAR File**:
   - In the "Libraries" tab, click "Add File"
   - Select the downloaded jar-with-dependencies JAR
6. **Create Connection**:
   - Click "OK" to save the driver
   - Create new connection using the Splunk driver
   - **Connection URL**: `jdbc:splunk://your-splunk-host:8089?schema=splunk&disableSslValidation=true`
   - **Username/Password**: Your Splunk credentials

## Prerequisites

### Splunk Configuration Requirements

Before using the JDBC driver, ensure your Splunk environment is properly configured:

1. **SPL Data Models**: The driver requires SPL (Search Processing Language) data models to be created in Splunk
   - Data models define the structure and relationships of your data
   - Create data models through Splunk Web: Settings → Data models → New Data Model
   - Models should represent the datasets you want to query via SQL

2. **Data Model Acceleration**: For reasonable performance, data models **must** be accelerated
   - Acceleration creates summarized data structures for faster queries
   - Enable acceleration: Data Models → [Your Model] → Edit → Acceleration → Enable
   - Allow time for initial acceleration to complete (can take hours for large datasets)
   - Monitor acceleration status in Splunk Web

3. **CIM Model Support**: The driver provides enhanced support for Common Information Model (CIM) data models
   - CIM provides standardized field names and data structures
   - **Special CIM Handling**: Unfortunately, CIM model designers did not follow discoverable field guidelines, so the driver includes special calculated field support specifically for CIM models to work around this limitation
   - This special handling compensates for CIM's use of non-discoverable calculated fields
   - Install and configure relevant CIM add-ons for your data sources when using CIM models

### Field Discovery and Calculated Fields

**Important**: The JDBC driver has limitations in discovering certain types of calculated fields:

- **Field Aliases/Remapped Fields**: These are automatically discovered by the driver
  - Created using field aliases in Splunk (e.g., `| eval new_name=existing_field`)
  - Remapped fields in data models are properly detected

- **Search-Time Calculated Fields**: These may **not** be auto-detected by the driver
  - Ad-hoc calculated fields created in search queries using `eval` statements
  - Fields computed dynamically at search time rather than being part of the data model
  - Complex eval expressions that exist only during query execution

**Best Practice: Make Calculated Fields Discoverable in SPL**

**Option 1: Data Model Integration (Recommended)**
- Define calculated fields **within the data model itself** using eval expressions
- When the data model is accelerated, these calculated fields become part of the summary structure
- This makes them discoverable by the JDBC driver as part of the model's schema
- Example: In your data model, add calculated field `user_category = if(like(user, "admin%"), "administrator", "regular_user")`

**Option 2: Props.conf Configuration**
- Use `EVAL-<fieldname>` in props.conf to create persistent calculated fields
- These fields are applied at index/search time and become part of field discovery
- Example: `EVAL-normalized_status = case(status=="ok", "success", status=="fail", "failure", 1==1, status)`
- Combined with `FIELDALIAS` for comprehensive field mapping

**Workarounds for Search-Time Fields**:
1. **Create SQL Views**: Define calculated fields as SQL views in your queries
2. **Use SQL Expressions**: Recalculate the fields directly in your SQL queries using SQL functions
3. **Runtime Calculation**: Implement the same logic in your SQL WHERE/SELECT clauses

**Key Insight**: The discoverability issue can be completely solved through proper SPL model design by defining calculated fields within data models rather than as ad-hoc search-time eval statements.

**Note on CIM Models**: Unfortunately, the designers of Splunk's Common Information Model (CIM) did not follow these discoverable field guidelines. As a result, this JDBC driver includes special handling and workarounds specifically for CIM models to compensate for their non-discoverable calculated fields. When designing your own data models, follow the best practices above to avoid requiring such workarounds.

### Performance Considerations

- **Accelerated Models Only**: Queries against non-accelerated models will be extremely slow
- **Model Design**: Design models with your SQL query patterns in mind
- **Field Mapping**: Ensure critical fields are properly extracted and named in your models
- **Time Range**: Use appropriate time ranges in your models to balance data coverage and performance

## Usage

### Connection URL Format

The driver supports multiple connection URL formats:

```
# Standard JDBC URL format
jdbc:splunk://host:port[/schema]?param1=value1&param2=value2

# Using semicolon-separated parameters
jdbc:splunk:url=https://host:port;user=username;password=pass

# Minimal URL with all parameters in properties
jdbc:splunk:
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
// Basic connection with username/password
String url = "jdbc:splunk://localhost:8089/splunk";
Properties props = new Properties();
props.setProperty("username", "admin");
props.setProperty("password", "changeme");
Connection conn = DriverManager.getConnection(url, props);

// Token-based authentication
String url = "jdbc:splunk://localhost:8089";
Properties props = new Properties();
props.setProperty("token", "your-splunk-token");
props.setProperty("schema", "splunk");
Connection conn = DriverManager.getConnection(url, props);

// Using individual connection parameters
String url = "jdbc:splunk:";
Properties props = new Properties();
props.setProperty("host", "localhost");
props.setProperty("port", "8089");
props.setProperty("protocol", "https");
props.setProperty("username", "admin");
props.setProperty("password", "changeme");
props.setProperty("schema", "splunk");
props.setProperty("app", "search");  // Optional app context
Connection conn = DriverManager.getConnection(url, props);

// With data model filtering
String url = "jdbc:splunk://localhost:8089?datamodelFilter=web,authentication";
Connection conn = DriverManager.getConnection(url, props);

// Custom table definitions
String url = "jdbc:splunk:";
Properties props = new Properties();
props.setProperty("url", "https://localhost:8089");
props.setProperty("username", "admin");
props.setProperty("password", "changeme");
props.setProperty("tables", "[{\"name\":\"access_logs\",\"search\":\"index=web\"}]");
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

### Dynamic Data Model Discovery

The driver can automatically discover Splunk data models and expose them as tables:

- **Automatic Discovery**: When connected, the driver discovers available data models
- **Filtering**: Use `datamodelFilter` to limit which models are exposed
- **Caching**: Data model metadata is cached (configurable via `datamodelCacheTtl`)
- **Force Refresh**: Set `refreshDatamodels=true` to force cache refresh

### Custom Table Definitions

You can define custom tables with specific Splunk searches:

```json
{
  "tables": [
    {
      "name": "web_errors",
      "search": "index=web status>=400",
      "columns": [
        {"name": "host", "type": "VARCHAR"},
        {"name": "status", "type": "INTEGER"},
        {"name": "uri", "type": "VARCHAR"}
      ]
    }
  ]
}
```

Custom tables can be defined via:
- Connection properties (as JSON string)
- Environment variables (e.g., `SPLUNK_CUSTOM_TABLES`)
- Model file configuration

### Connection Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `url` | Complete Splunk server URL (e.g., https://localhost:8089) | Required (or use host/port) |
| `host` | Splunk server hostname (alternative to url) | Required if url not provided |
| `port` | Splunk server port | `8089` |
| `protocol` | Protocol to use (http or https) | `https` |
| `username` or `user` | Splunk username | Required for password auth |
| `password` | Splunk password | Required for password auth |
| `token` | Splunk authentication token (alternative to username/password) | None |
| `app` | Splunk app context for searches | None |
| `schema` | Default schema for unqualified table names | `splunk` |
| `disableSslValidation` | Disable SSL certificate validation (WARNING: dev/test only) | `false` |
| `datamodelFilter` | Filter for dynamic data model discovery | None |
| `datamodelCacheTtl` | Cache TTL for data models in minutes | `60` |
| `refreshDatamodels` | Force refresh of data model cache | `false` |
| `tables` | Custom table definitions (JSON array) | None |
| `earliest` | Default earliest time for queries | `-24h` |
| `latest` | Default latest time for queries | `now` |
| `connectTimeout` | Connection timeout (ms) | `30000` |
| `socketTimeout` | Socket timeout (ms) | `60000` |
| `modelFile` | Path to Calcite model file (for federation) | None |

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
# Build everything (Calcite and all JDBC drivers)
cd ..
./build.sh

# Or build just the Splunk driver after Calcite is built
cd splunk-jdbc-driver
mvn clean package

# The JAR with dependencies will be at:
# target/splunk-jdbc-driver-1.0.1-jar-with-dependencies.jar
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