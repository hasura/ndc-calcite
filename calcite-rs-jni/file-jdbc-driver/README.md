# File JDBC Driver for Apache Calcite

The File JDBC Driver provides SQL access to various file formats including CSV, JSON, Parquet, Excel, and HTML files. Built on Apache Calcite, it supports both simple file connections and complex data lake configurations.

## Quick Start

**Simple file connection:**
```
jdbc:file:///path/to/data.csv
jdbc:file:s3://bucket/data.parquet
```

**Directory connection:**
```
jdbc:file:///path/to/data/directory
```

**Multi-location connection:**
```
jdbc:file:multi?locations=/data/sales.csv|s3://bucket/products.parquet
```

**Configuration file approach (for complex setups):**
```
jdbc:file:config=/path/to/config.json
```

## Supported File Formats

| Format | Auto-Detection | Notes |
|--------|---------------|-------|
| CSV | ✓ | Configurable headers, separators, encoding |
| TSV | ✓ | Tab-separated values |
| JSON | ✓ | Single object or line-delimited (JSONL) |
| Parquet | ✓ | Columnar format, excellent for analytics |
| Excel | ✓ | Multi-sheet support via JSON extraction |
| HTML | ✓ | Table extraction from web pages |
| Arrow | ✓ | High-performance columnar format |
| YAML | ✓ | Configuration and data files |

## Connection URLs

### Basic Patterns

**Single file:**
```
jdbc:file:///absolute/path/to/file.csv
jdbc:file:relative/path/to/file.json
jdbc:file:s3://bucket/path/file.parquet
jdbc:file:https://api.example.com/data.json
```

**Directory (discovers all files):**
```
jdbc:file:///path/to/directory
jdbc:file:s3://bucket/data/
```

**Multi-location (pipe-separated):**
```
jdbc:file:multi?locations=file1.csv|file2.json|s3://bucket/file3.parquet
```

### Glob Patterns

The driver automatically detects glob patterns in paths:

```
jdbc:file:///data/*.csv           # All CSV files
jdbc:file:s3://bucket/2024/*/data.parquet  # Year-partitioned data
jdbc:file:///logs/**/*.json       # Recursive JSON files
```

**Glob modes:**
- `multi-table` (default): Each matching file becomes a separate table
- `single-table`: All matching files are combined into one table

### Configuration File Approach

For complex data lake setups, use hybrid config files with global settings and schema definitions:

```
jdbc:file:config=/path/to/config.json
jdbc:file:config=s3://bucket/configs/datalake.json
```

Config files combine global parameters with Calcite model.json schema definitions.

## Connection Parameters

### Format Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `format` | Force specific format (csv, json, parquet, etc.) | auto |
| `charset` | Character encoding | UTF-8 |
| `header` | CSV/TSV files have headers | true |
| `multiline` | JSON objects span multiple lines | false |
| `skipLines` | Number of lines to skip in CSV | 0 |

### Performance Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `executionEngine` | parquet, arrow, vectorized, linq4j | auto |
| `batchSize` | Rows per processing batch | 10000 |
| `memoryThreshold` | Memory limit before spillover (bytes) | 67108864 |
| `spillDirectory` | Directory for temporary spillover files | system temp |
| `refreshInterval` | Data refresh frequency ("1 hour", "-1" = never) | -1 |

### Directory Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `recursive` | Scan subdirectories | false |
| `globMode` | "multi-table" or "single-table" for globs | multi-table |

### Identifier Casing Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `table_name_casing` | Table name transformation: UPPER, LOWER, UNCHANGED | UPPER |
| `column_name_casing` | Column name transformation: UPPER, LOWER, UNCHANGED | UNCHANGED |
| `caseSensitive` | Calcite identifier case sensitivity | true |
| `quotedCasing` | How quoted identifiers are handled | UNCHANGED |
| `unquotedCasing` | How unquoted identifiers are handled | TO_LOWER |

### AWS S3 Options

Configure via environment variables or AWS CLI:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY` 
- `AWS_SESSION_TOKEN`
- `AWS_REGION`

## Examples

### Basic CSV Connection
```java
Connection conn = DriverManager.getConnection("jdbc:file:///data/sales.csv");
ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM sales");
```

### Directory with Parameters
```java
String url = "jdbc:file:///data/directory?recursive=true&format=auto";
Connection conn = DriverManager.getConnection(url);
```

### S3 Parquet Files
```java
String url = "jdbc:file:s3://datalake/analytics/*.parquet?globMode=single-table";
Connection conn = DriverManager.getConnection(url);
```

### Multi-location Setup
```java
String url = "jdbc:file:multi?locations=" +
    "/local/customers.csv|" +
    "s3://bucket/products.parquet|" +
    "https://api.com/orders.json";
Connection conn = DriverManager.getConnection(url);
```

### Configuration File
```java
Connection conn = DriverManager.getConnection("jdbc:file:config=/path/to/config.json");
```

### Table and Column Name Casing
```java
Properties props = new Properties();
props.setProperty("table_name_casing", "LOWER");
props.setProperty("column_name_casing", "LOWER");

Connection conn = DriverManager.getConnection("jdbc:file:///data/sales.csv", props);
// Table "SALES" is now accessible as "sales" (lowercase)
ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM files.sales");
```

## File Format Details

### CSV/TSV Files
- Auto-detects separators (comma, tab, semicolon, pipe)
- Configurable headers, encoding, quote characters
- Supports nested directories with `recursive=true`

### JSON Files
- Single objects or line-delimited JSONL
- Nested object flattening
- Array expansion support

### Parquet Files
- Columnar storage format
- Excellent compression and query performance
- Not memory-limited - can handle terabyte files
- Best choice for analytics workloads

### Excel Files (.xlsx)
- Multi-sheet support via JSON extraction
- Each sheet becomes a separate table
- Automatic table naming: `filename__sheetname`

### HTML Files
- Extracts tables from web pages
- Must use directory discovery (not explicit table mapping)
- Supports HTTP/HTTPS URLs with fragment identifiers
- Local HTML files supported

### Arrow Files
- High-performance columnar format
- Memory-mapped for large files
- Compression codec support

## Advanced Features

### Configuration Files
Use hybrid config files with global settings and multiple schemas:
```json
{
  "version": "1.0",
  "defaultSchema": "analytics",
  "executionEngine": "parquet",
  "batchSize": 10000,
  "memoryThreshold": 268435456,
  "charset": "UTF-8",
  "awsRegion": "us-east-1",
  
  "schemas": [
    {
      "name": "files",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data/files",
        "recursive": true
      }
    },
    {
      "name": "parquet_data",
      "type": "custom", 
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data/parquet"
      }
    }
  ]
}
```

### Configuration Hierarchy 

**Connection-wide settings** (cannot be overridden):
- `executionEngine` - Set once for entire connection (parquet, arrow, vectorized, linq4j)

**Hierarchical settings** (can be overridden at schema/table level):
- `batchSize` (default: 2048), `memoryThreshold` (default: 67108864 = 64MB), `refreshInterval`

**1. Global level** (applies to all schemas):
```json
{
  "executionEngine": "parquet",
  "batchSize": 2048,
  "memoryThreshold": 67108864,
  "refreshInterval": "15 minutes"
}
```

**2. Schema level** (overrides global for that schema):
```json
{
  "name": "s3_data",
  "operand": {
    "directory": "s3://bucket/data/",
    "batchSize": 4096,
    "refreshInterval": "1 hour"
  }
}
```

**3. Table level** (overrides schema/global for that table):
```json
{
  "name": "live_data",
  "url": "/data/live/*.parquet", 
  "refreshInterval": "5 minutes"
}
```

**Note**: Format-specific parameters like `charset`, `header`, `multiline`, `skipLines` should be specified in individual table definitions, not at schema level.

### Partitioned Tables
Tables with glob patterns in URLs are automatically treated as partitioned (requires `executionEngine: "parquet"`):
```json
{
  "name": "sales_partitioned",
  "url": "/data/sales/year=*/month=*/*.parquet"
}
```

### Materialized Views
Materialized views automatically default the table name to `view + "_materialized"`:
```json
{
  "view": "sales_summary_mv",
  "sql": "SELECT product_id, COUNT(*) as sales FROM sales GROUP BY product_id"
}
```
Creates materialized table: `sales_summary_mv_materialized`

You can still specify a custom table name:
```json
{
  "view": "daily_metrics",
  "table": "custom_table_name", 
  "sql": "SELECT DATE(order_date) as date, SUM(amount) as revenue FROM sales GROUP BY DATE(order_date)"
}
```

### Performance Optimization

**For large datasets:**
- Use Parquet format for best performance
- Set appropriate `batchSize` and `memoryThreshold`
- Configure `spillDirectory` for datasets larger than memory
- Use `executionEngine=parquet` for columnar operations

**For frequent queries:**
- Set `refreshInterval` to cache data
- Use materialized views for pre-computed aggregations
- Consider Arrow format for memory-mapped access

## Error Handling

Common issues and solutions:

**"Table not found"**
- Check file paths and permissions
- Verify glob patterns match existing files
- Use `recursive=true` for nested directories

**"Memory errors"**
- Reduce `batchSize` parameter
- Set lower `memoryThreshold`
- Configure `spillDirectory`
- Switch to Parquet format

**"HTML table errors"**
- HTML files must use directory discovery
- Cannot be used with explicit table mapping
- Verify table extraction from HTML content

## Testing

Run the test suite:
```bash
cd tests
./scripts/run-tests.sh
```

Tests cover all supported formats, transports, and features using sample data in `tests/data/`.

## Configuration Examples

See the `examples/` directory:
- `simple-config.json` - Basic single schema setup
- `complete-config.json` - Multiple schemas with global settings
- `comprehensive-config.json` - All features: views, materializations, partitioned tables

## Building

```bash
mvn clean package
# Creates: target/file-jdbc-driver-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Dependencies

- Apache Calcite 1.41.0-SNAPSHOT
- Apache Arrow (for Arrow and Parquet support)
- Jackson (for JSON/YAML processing)
- AWS SDK (for S3 support)