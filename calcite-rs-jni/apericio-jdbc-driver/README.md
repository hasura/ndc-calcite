# Apericio JDBC Driver

A streamlined JDBC driver for universal data access with automatic optimization, advanced analytics, and SQL query capabilities across 20+ file formats and storage systems.

## 🚀 Quick Start

### Maven Dependency
```xml
<dependency>
    <groupId>com.kenstott.components</groupId>
    <artifactId>apericio-jdbc-driver</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Basic Usage
```java
// Connect to a directory - files become tables automatically
String url = "jdbc:apericio:/data/warehouse";
Connection conn = DriverManager.getConnection(url);

// Query any file as a table
ResultSet rs = conn.createStatement().executeQuery(
    "SELECT customer_name, SUM(amount) as total " +
    "FROM sales_2024 " +  // sales_2024.csv becomes table 'sales_2024'
    "WHERE order_date >= DATE '2024-01-01' " +
    "GROUP BY customer_name " +
    "ORDER BY total DESC"
);
```

### Connection Examples
```bash
# Simple directory
jdbc:apericio:/path/to/data

# With common options
jdbc:apericio:/data?lex=ORACLE&unquotedCasing=TO_LOWER&recursive=true

# Model file for complex setups
jdbc:apericio:model=/path/to/config.json
```

## 🗂️ Supported Data Sources

### File Formats (Auto-Discovery)
| Format | Extensions | Features |
|--------|------------|----------|
| **CSV/TSV** | `.csv`, `.tsv`, `.csv.gz` | Type inference, compression |
| **JSON** | `.json`, `.jsonl` | Nested data, multi-table extraction |
| **Parquet** | `.parquet` | Columnar, partition discovery |
| **Excel** | `.xlsx`, `.xls` | Multi-sheet → multiple tables |
| **Apache Arrow** | `.arrow` | In-memory columnar |
| **HTML** | `.html` | Table extraction, web crawling |
| **YAML/XML** | `.yaml`, `.yml`, `.xml` | Structured data, XPath |
| **Apache Iceberg** | Table format | Time travel, schema evolution |

### Storage Systems
- **Local** - File system (default)
- **Amazon S3** - Native S3 support with authentication
- **HTTP/HTTPS** - REST APIs with multiple auth types
- **SharePoint** - Online and on-premises
- **FTP/SFTP** - File transfer protocols

## ⭐ Key Features

### Universal Data Access
- **Automatic Schema Discovery** - Zero-configuration table creation
- **Cross-Format Queries** - Join CSV + JSON + Parquet seamlessly
- **Multi-Storage Support** - Mix local files with cloud data
- **20+ File Formats** - From CSV to Apache Iceberg

### Enterprise Analytics
- **Apache Iceberg Integration** - Time travel, schema evolution
- **HyperLogLog Statistics** - Instant COUNT(DISTINCT) results
- **Materialized Views** - Pre-computed complex queries
- **HTML Web Crawling** - Extract data from web pages

### High Performance
- **Automatic Optimization** - Column pruning, filter pushdown
- **Memory Management** - Automatic disk spillover for large datasets
- **Multiple Engines** - Choose optimal processing engine
- **Result Caching** - Persistent caching for repeated queries

## 📖 Documentation

### Getting Started
- **[Installation Guide](docs/installation.md)** - Setup and dependencies
- **[Configuration Guide](docs/configuration.md)** - Connection options and properties
- **[File Formats Guide](docs/file-formats.md)** - Supported formats and features

### Advanced Features  
- **[Apache Iceberg Guide](docs/iceberg.md)** - Time travel, schema evolution, and enterprise data lakes
- **[Web Crawling Guide](docs/web-crawling.md)** - Automated data discovery and HTML table extraction
- **[Storage Providers Guide](docs/storage-providers.md)** - Cloud storage, APIs, and enterprise systems
- **[Execution Engines Guide](docs/execution-engines.md)** - Performance optimization across different workloads

### Reference
- **[SQL Reference](docs/sql-reference.md)** - Supported SQL features
- **[API Reference](docs/api-reference.md)** - Programmatic usage
- **[Troubleshooting](docs/troubleshooting.md)** - Common issues and solutions

## 💡 Common Use Cases

### Data Lake Analytics
```sql
-- Query across multiple formats and time periods
SELECT 
  YEAR(order_date) as year,
  QUARTER(order_date) as quarter, 
  SUM(amount) as revenue
FROM sales_data  -- Could be CSV, Parquet, or any supported format
WHERE YEAR(order_date) >= 2023
GROUP BY YEAR(order_date), QUARTER(order_date);
```

### Cross-Format Data Integration
```sql
-- Join data from different file types
SELECT 
  c.customer_name,     -- from customers.csv
  o.order_id,          -- from orders.json
  p.product_name,      -- from products.parquet  
  SUM(o.amount) as total_spent
FROM customers c
JOIN orders o ON c.id = o.customer_id
JOIN products p ON o.product_id = p.id
GROUP BY c.customer_name, o.order_id, p.product_name;
```

### Real-Time Web Data
```sql
-- Combine local data with web-crawled information
SELECT 
  local.ticker,
  local.shares_owned,
  web.current_price,
  (local.shares_owned * web.current_price) as current_value
FROM portfolio local
JOIN stock_prices web ON local.ticker = web.symbol
WHERE web.last_updated >= CURRENT_TIMESTAMP - INTERVAL '5' MINUTE;
```

## 🛠️ Building from Source

```bash
git clone https://github.com/hasura/ndc-calcite.git
cd ndc-calcite/calcite-rs-jni/apericio-jdbc-driver
mvn clean package
```

Creates: `target/apericio-jdbc-driver-1.0.0-jar-with-dependencies.jar`

## 🆘 Support

- **Documentation**: [docs/](docs/)
- **Issues**: [GitHub Issues](https://github.com/hasura/ndc-calcite/issues)
- **Examples**: [examples/](examples/)

## 📄 License

Apache License 2.0 - see [LICENSE](../LICENSE) for details.

---

**Need help?** Check the [Quick Start Guide](docs/quickstart.md) or [Troubleshooting](docs/troubleshooting.md) for common solutions.