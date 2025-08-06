# Aperio JDBC Driver

A revolutionary JDBC driver that transforms any collection of files into a fully queryable SQL database. Query CSV, JSON, Parquet, Excel, and dozens of other file formats using standard SQL syntax with enterprise-grade performance, advanced analytics capabilities, and seamless integration with existing Java applications.

Transform your data lakes, file repositories, and document collections into powerful, queryable data sources without ETL processes or data movement.

## Supported Technologies

<table>
<tr>
<th>📄 File Formats</th>
<th>🌐 Transports</th>
<th>💾 Storage Systems</th>
</tr>
<tr>
<td valign="top">

**Text Formats**
- CSV
- TSV
- JSON
- YAML

**Binary Formats**
- Parquet
- Apache Arrow

**Spreadsheets**
- Excel (XLSX/XLS)

**Documents**
- HTML tables
- Markdown tables
- Word (DOCX) tables

**Archives**
- GZIP (.gz)

</td>
<td valign="top">

**Web Protocols**
- HTTP
- HTTPS

**File Transfer**
- FTP
- FTPS
- SFTP

**Cloud Native**
- S3 Protocol

**Enterprise**
- SharePoint API
- OAuth2

</td>
<td valign="top">

**Local**
- File System
- Network Shares

**Cloud Storage**
- AWS S3
- S3-compatible

**Enterprise**
- SharePoint Online

**Remote**
- Web Servers
- FTP Servers
- SFTP Servers

</td>
</tr>
</table>

## Key Features

### 🚀 Performance & Scale
- **Execution Engines**: CSV, Parquet, Arrow, Vectorized processors
- **Spillover Support**: Handle datasets larger than memory
- **Batch Processing**: Configurable batch sizes for optimal performance
- **Parallel Processing**: Multi-threaded file scanning

### 🔧 Advanced Capabilities
- **Materialized Views**: Automatic view caching and refresh
- **Partitioned Data**: Automatic partition discovery and pruning
- **Glob Patterns**: Query multiple files with wildcards
- **Schema Inference**: Automatic schema detection from files
- **PostgreSQL Compatibility**: Full PostgreSQL SQL syntax support
- **Multi-Schema Support**: Organize files into logical schemas
- **Auto-Refresh**: Configurable refresh intervals for remote files

## Quick Start

### Maven Dependency

```xml
<dependency>
    <groupId>com.kenstott.components</groupId>
    <artifactId>aperio-jdbc-driver</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Basic Usage

```java
import java.sql.*;

public class AperioExample {
    public static void main(String[] args) throws SQLException {
        // Connect to local CSV files
        String url = "jdbc:aperio:/data/csv-files";
        Connection conn = DriverManager.getConnection(url);
        
        // Query CSV files as tables
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
            "SELECT customer_name, SUM(amount) " +
            "FROM files.\"sales.csv\" " +
            "WHERE date >= '2024-01-01' " +
            "GROUP BY customer_name " +
            "ORDER BY 2 DESC"
        );
        
        while (rs.next()) {
            System.out.println(rs.getString(1) + ": " + rs.getBigDecimal(2));
        }
    }
}
```

## Connection URLs

### Direct Path Format
```
jdbc:aperio:/path/to/data/files
```

### PostgreSQL-Style Format
```
jdbc:aperio://localhost/data/files
```

### Parameter Format
```
jdbc:aperio:dataPath='/data/files';engine='parquet';schema='mydata'
```

### Properties-Based Configuration
```java
Properties props = new Properties();
props.setProperty("dataPath", "/data/files");
props.setProperty("engine", "parquet");
props.setProperty("defaultSchema", "files");

Connection conn = DriverManager.getConnection("jdbc:aperio:", props);
```

### Multiple Data Paths
Use pipe-delimited paths to query multiple data sources:
```java
// Multiple local directories
String url = "jdbc:aperio:dataPath='/data/sales|/data/customers|/archive/products'";

// Mixed local and remote sources with custom schema names
String url2 = "jdbc:aperio:dataPath='sales:/data/sales|customers:s3://bucket/customers|api:https://api.example.com/data.csv'";

// Using properties for complex configurations
Properties props = new Properties();
props.setProperty("dataPath", "/local/data|s3://bucket/remote|https://api.example.com/data.csv");
props.setProperty("engine", "parquet");

Connection conn = DriverManager.getConnection("jdbc:aperio:", props);

// Query data from any source
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery("SELECT * FROM files.path1"); // First path
ResultSet rs2 = stmt.executeQuery("SELECT * FROM files.path2"); // Second path
```

### Large Dataset Support with Spillover
Configure spillover for processing datasets larger than available memory:
```java
// Enable spillover with 64MB memory threshold
String url = "jdbc:aperio:dataPath='/data/large-files';engine='parquet';memoryThreshold='67108864';spillDirectory='/tmp/calcite_spill'";

// Using properties for better readability
Properties props = new Properties();
props.setProperty("dataPath", "/data/large-parquet-files");
props.setProperty("engine", "parquet");
props.setProperty("memoryThreshold", "134217728"); // 128MB
props.setProperty("spillDirectory", "/var/tmp/aperio-spill");
props.setProperty("batchSize", "10000");

Connection conn = DriverManager.getConnection("jdbc:aperio:", props);
```

### Remote File Access with Authentication
```java
// S3 access with AWS region
Properties props = new Properties();
props.setProperty("dataPath", "s3://my-bucket/data");
props.setProperty("awsRegion", "eu-west-1");
props.setProperty("refreshInterval", "5 minutes");

// SharePoint access with OAuth2
Properties sharePointProps = new Properties();
sharePointProps.setProperty("dataPath", "https://company.sharepoint.com/sites/data");
sharePointProps.setProperty("storageType", "sharepoint");
sharePointProps.setProperty("tenantId", "your-tenant-id");
sharePointProps.setProperty("clientId", "your-client-id");
sharePointProps.setProperty("clientSecret", "your-secret");

// SFTP access with SSH key
Properties sftpProps = new Properties();
sftpProps.setProperty("dataPath", "sftp://server.example.com/data");
sftpProps.setProperty("username", "sftpuser");
sftpProps.setProperty("privateKeyPath", "/home/user/.ssh/id_rsa");
```

## Configuration Parameters

### Basic Parameters
| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `dataPath` | Path to data files or directory | `./data` | `/data/files` |
| `engine` | Execution engine to use | `parquet` | `csv`, `arrow`, `vectorized` |
| `defaultSchema` | Default schema name | `files` | `mydata` |
| `batchSize` | Batch size for processing | `1000` | `5000` |

### Performance & Spillover Parameters
| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `memoryThreshold` | Memory limit before spillover (bytes) | - | `67108864` (64MB) |
| `spillDirectory` | Directory for spillover files | - | `/tmp/calcite_spill` |
| `refreshInterval` | Auto-refresh interval for remote files | - | `5 minutes`, `1 hour` |
| `recursive` | Recursively scan directories | `true` | `false` |

### Casing Parameters
| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `tableNameCasing` | Table name casing | `UPPER` | `LOWER`, `UNCHANGED` |
| `columnNameCasing` | Column name casing | `UNCHANGED` | `UPPER`, `LOWER` |

### Storage Provider Authentication
| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `storageType` | Storage provider type | auto-detect | `s3`, `sharepoint`, `sftp` |
| `awsRegion` | AWS region for S3 | `us-east-1` | `eu-west-1` |
| `tenantId` | SharePoint tenant ID | - | `your-tenant-id` |
| `clientId` | SharePoint/OAuth client ID | - | `your-client-id` |
| `clientSecret` | SharePoint/OAuth client secret | - | `your-secret` |
| `username` | FTP/SFTP username | - | `ftpuser` |
| `password` | FTP/SFTP password | - | `secret` |
| `privateKeyPath` | SSH private key path for SFTP | - | `~/.ssh/id_rsa` |

### Advanced Features
| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `viewsFile` | Path to views definition file | - | `/config/views.yaml` |
| `materializationsEnabled` | Enable materialized views | `false` | `true` |

## File Format Examples

### CSV Files
```sql
SELECT * FROM files."customers.csv" WHERE country = 'USA';
```

### JSON Files
```sql
SELECT data->>'name' as name, data->>'age' as age 
FROM files."users.json" 
WHERE data->>'active' = 'true';
```

### Parquet Files
```sql
SELECT year, month, SUM(sales) 
FROM files."sales_data.parquet" 
WHERE year >= 2023 
GROUP BY year, month;
```

### Excel Files
```sql
SELECT * FROM files."spreadsheet.xlsx" 
WHERE "Revenue" > 10000;
```

### Multiple Files with Glob Patterns
```sql
SELECT * FROM files."sales_*.csv" 
WHERE date BETWEEN '2024-01-01' AND '2024-12-31';
```

## Advanced Usage

### Materialized Views
```java
// Enable materialized view storage
String url = "jdbc:aperio:dataPath='/data/files';engine='parquet';storage='/tmp/mv_cache'";
Connection conn = DriverManager.getConnection(url);

// Create materialized view
Statement stmt = conn.createStatement();
stmt.execute(
    "CREATE MATERIALIZED VIEW daily_sales AS " +
    "SELECT date_trunc('day', order_date) as day, SUM(amount) as total " +
    "FROM files.\"orders.csv\" " +
    "GROUP BY date_trunc('day', order_date)"
);

// Query materialized view (automatically cached)
ResultSet rs = stmt.executeQuery("SELECT * FROM daily_sales WHERE day >= '2024-01-01'");
```

### Remote Files
```java
// Query files over HTTP
String url = "jdbc:aperio:dataPath='https://example.com/data'";

// Query S3 files
String s3Url = "jdbc:aperio:dataPath='s3://my-bucket/data'";
```

### Complex Queries
```sql
-- Window functions
SELECT 
    customer_id,
    order_date,
    amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as order_seq
FROM files."orders.csv";

-- CTEs with multiple files
WITH customer_summary AS (
    SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total_spent
    FROM files."orders.csv"
    GROUP BY customer_id
),
high_value_customers AS (
    SELECT customer_id FROM customer_summary WHERE total_spent > 1000
)
SELECT c.name, c.email, cs.order_count, cs.total_spent
FROM files."customers.csv" c
JOIN customer_summary cs ON c.id = cs.customer_id
WHERE c.id IN (SELECT customer_id FROM high_value_customers);
```

## Performance Optimization

### Use Parquet for Large Datasets
```java
// Configure for large dataset processing
Properties props = new Properties();
props.setProperty("dataPath", "/data/large-files");
props.setProperty("engine", "parquet");
props.setProperty("batchSize", "10000");
```

### Enable Spillover for Memory-Constrained Environments
```java
// Automatic spillover to disk for large results
String url = "jdbc:aperio:dataPath='/data/files';engine='parquet';spillover='true'";
```

### Vectorized Processing
```java
// Use vectorized execution for analytics workloads
String url = "jdbc:aperio:dataPath='/data/files';engine='vectorized';batchSize='5000'";
```

## Testing

### Unit Tests
```bash
mvn test -Dtest=AperioDriverTest
```

### Integration Tests
```bash
# Create test properties file
cp local-test.properties.sample local-test.properties
# Edit local-test.properties with your test data paths

# Run comprehensive tests
mvn test -Dtest=AperioDriverFacadeTest
```

### Sample Test Data Structure
```
/data/test/
├── csv/
│   ├── customers.csv
│   ├── orders.csv
│   └── products.csv
├── json/
│   ├── events.json
│   └── logs.json
├── parquet/
│   ├── sales_2023.parquet
│   └── sales_2024.parquet
└── excel/
    ├── financial_report.xlsx
    └── inventory.xlsx
```

## Troubleshooting

### Common Issues

#### Driver Not Found
```java
// Ensure driver is on classpath
Class.forName("com.hasura.aperio.AperioDriver");
```

#### File Not Found
```sql
-- Check file path and permissions
-- Files are referenced relative to dataPath
SELECT * FROM files."data.csv";  -- looks for {dataPath}/data.csv
```

#### Memory Issues with Large Files
```java
// Use Parquet engine with spillover
String url = "jdbc:aperio:dataPath='/data';engine='parquet';batchSize='1000'";
```

#### Unsupported File Format
```java
// Check supported formats: CSV, JSON, Parquet, Arrow, Excel, etc.
// Use appropriate engine for file type
String url = "jdbc:aperio:dataPath='/data';engine='excel'";  // for .xlsx files
```

## Building from Source

```bash
# Clone repository
git clone https://github.com/hasura/ndc-calcite.git
cd ndc-calcite/calcite-rs-jni/aperio-jdbc-driver

# Build
mvn clean package

# Install locally
mvn install
```

## Requirements

- Java 11 or higher
- Maven 3.6+ (for building)

## License

Apache License 2.0

## Support

- [GitHub Issues](https://github.com/hasura/ndc-calcite/issues)
- [Documentation](https://github.com/hasura/ndc-calcite/tree/main/calcite-rs-jni/aperio-jdbc-driver)

## Related Projects

- [File JDBC Driver](../file-jdbc-driver/) - Alternative file driver implementation
- [Splunk JDBC Driver](../splunk-jdbc-driver/) - Splunk data access
- [SharePoint JDBC Driver](../sharepoint-list-jdbc-driver/) - SharePoint Lists access