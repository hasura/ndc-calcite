no# Configuration Guide

Comprehensive guide to configuring the Apericio JDBC Driver for optimal performance and functionality.

## Connection Methods

### 1. URL Query Parameters
```
jdbc:apericio:/data?property=value&property2=value2
```

### 2. Connection Properties
```java
Properties props = new Properties();
props.setProperty("directory", "/data");
props.setProperty("recursive", "true");
Connection conn = DriverManager.getConnection("jdbc:apericio:", props);
```

### 3. Model File Configuration
```
jdbc:apericio:model=/path/to/config.json
```

### 4. Environment Variables
All string values support `${VAR_NAME}` expansion:
```java
props.setProperty("directory", "${DATA_DIR}");
props.setProperty("spillDirectory", "${TEMP_DIR}/apericio");
```

## Core Settings

### Directory and File Discovery
| Property | Default | Environment Variable | Description |
|----------|---------|---------------------|-------------|
| `directory` | `.` (current) | - | Base directory for file discovery |
| `recursive` | `true` | - | Scan subdirectories |
| `directoryPattern` | `*` | - | Glob pattern for directory filtering |
| `filePattern` | - | `APERICIO_FILE_PATTERN` | File name pattern filter |
| `excludePattern` | - | `APERICIO_EXCLUDE_PATTERN` | Pattern to exclude files |
| `multiTable` | `false` | - | Extract multiple tables from single files |

**Examples:**
```
# Scan only CSV files from 2024
jdbc:apericio:/data?directoryPattern=2024/*.csv

# Enable multi-table Excel extraction
jdbc:apericio:/data?multiTable=true
```

### SQL Dialect Configuration
| Property | Default | Environment Variable | Description |
|----------|---------|---------------------|-------------|
| `lex` | `ORACLE` | - | SQL dialect: `ORACLE`, `MYSQL`, `SQL_SERVER`, `JAVA` |
| `unquotedCasing` | `TO_LOWER` | - | `TO_UPPER`, `TO_LOWER`, `UNCHANGED` |
| `quotedCasing` | `UNCHANGED` | - | `TO_UPPER`, `TO_LOWER`, `UNCHANGED` |
| `caseSensitive` | `false` | - | Case-sensitive identifiers |

**Oracle-compatible setup:**
```
jdbc:apericio:/data?lex=ORACLE&unquotedCasing=TO_LOWER&caseSensitive=false
```

### Table and Column Naming
| Property | Default | Environment Variable | Description |
|----------|---------|---------------------|-------------|
| `tableNameCasing` | `SMART_CASING` | - | `SMART_CASING`, `UPPER`, `LOWER`, `UNCHANGED` |
| `columnNameCasing` | `SMART_CASING` | - | `SMART_CASING`, `UPPER`, `LOWER`, `UNCHANGED` |

- `SMART_CASING` - Converts to lowercase with underscores (recommended)
- `UPPER` - All uppercase
- `LOWER` - All lowercase
- `UNCHANGED` - Keep original casing

## Performance Settings

### Memory Management
| Property | Default | Environment Variable | Description |
|----------|---------|---------------------|-------------|
| `batchSize` | `1000` | `APERICIO_BATCH_SIZE` | Rows per processing batch |
| `memoryThreshold` | `134217728` | `APERICIO_MEMORY_THRESHOLD` | Memory limit (bytes) before spillover |
| `spillDirectory` | System temp | `APERICIO_SPILL_DIRECTORY` | Directory for temporary spillover files |
| `maxConcurrency` | CPU cores | - | Maximum concurrent operations |
| `parallelism` | - | `APERICIO_PARALLELISM` | Query parallelism level |
| `fetchSize` | - | `APERICIO_FETCH_SIZE` | JDBC fetch size |

**Large dataset configuration:**
```
jdbc:apericio:/data?batchSize=10000&memoryThreshold=536870912&spillDirectory=/fast-disk/temp
```

### Caching and Statistics
| Property | Default | Description |
|----------|---------|-------------|
| `enableStatistics` | `false` | Enable HyperLogLog statistics |
| `statisticsCache` | `${user.home}/.apericio/stats` | Statistics cache directory |
| `cacheEnabled` | `true` | Enable result caching |
| `refreshInterval` | `-1` (never) | Auto-refresh interval |

**Analytics setup with statistics:**
```
jdbc:apericio:/data?enableStatistics=true&statisticsCache=/var/cache/apericio
```

### File Format Settings
| Property | Default | Environment Variable | Description |
|----------|---------|---------------------|-------------|
| `encoding` | `UTF-8` | - | Character encoding for text files |
| `csvInferTypes` | `false` | `APERICIO_CSV_INFER_TYPES` | Enable CSV type inference |
| `csvSamplingRate` | - | `CSV_SAMPLING_RATE` | Sampling rate for type inference (0.0-1.0) |
| `csvMaxSampleRows` | - | `CSV_MAX_SAMPLE_ROWS` | Max rows to sample for inference |
| `csvInferDates` | - | `CSV_INFER_DATES` | Infer date/time types |

**Note:** CSV header detection and row skipping are handled automatically by the file adapter. Manual `header` or `skipLines` configuration is not supported - the adapter analyzes file structure automatically.

## Execution Engine Configuration

### DuckDB Engine (Default)
The driver defaults to DuckDB for best performance if available in the classpath, otherwise falls back to Parquet engine.

| Property | Default | Environment Variable | Description |
|----------|---------|---------------------|-------------|
| `executionEngine` | `duckdb` | `APERICIO_EXECUTION_ENGINE` | Engine: `duckdb`, `parquet`, `arrow`, `linq4j` |
| `duckdbMemoryLimit` | - | `DUCKDB_MEMORY_LIMIT` | DuckDB memory limit (e.g., "4GB") |
| `duckdbThreads` | - | `DUCKDB_THREADS` | DuckDB thread count |
| `duckdbTempDirectory` | - | `DUCKDB_TEMP_DIRECTORY` | DuckDB temp directory |

**Note:** DuckDB provides superior performance but requires the duckdb-jdbc driver in your classpath. If not available, the driver will automatically fall back to the Parquet engine.

## Advanced Configuration

### Model File Format
For complex setups, use JSON configuration files:

```json
{
  "version": "1.0",
  "defaultSchema": "files",
  "schemas": [{
    "name": "files",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "${DATA_DIR}",
      "recursive": true,
      "executionEngine": "duckdb",
      "enableStatistics": true,
      "batchSize": 10000,
      "memoryThreshold": 268435456,
      "spillDirectory": "${TEMP_DIR}/apericio",
      "tableNameCasing": "LOWER",
      
      "tables": [{
        "name": "combined_sales",
        "url": "sales_*.csv",
        "refreshInterval": "1 hour"
      }],
      
      "views": [{
        "name": "high_value_sales",
        "sql": "SELECT * FROM sales WHERE amount > 1000"
      }],
      
      "materializedViews": [{
        "view": "monthly_summary",
        "table": "monthly_cache",
        "sql": "SELECT YEAR(date) as year, MONTH(date) as month, SUM(amount) as total FROM sales GROUP BY YEAR(date), MONTH(date)"
      }]
    }
  }]
}
```

**Note:** Materialized views require `executionEngine` to be set to either `duckdb` or `parquet`. Regular views work with any execution engine.

### Storage Provider Configuration

#### Amazon S3
| Property | Environment Variable | Description |
|----------|---------------------|-------------|
| `storageType` | `APERICIO_STORAGE_TYPE` | Set to `s3` for S3 storage |
| `s3Bucket` | `S3_BUCKET` | S3 bucket name |
| `s3Region` | `AWS_REGION` or `AWS_DEFAULT_REGION` | AWS region |
| `s3AccessKey` | `AWS_ACCESS_KEY_ID` | AWS access key (optional) |
| `s3SecretKey` | `AWS_SECRET_ACCESS_KEY` | AWS secret key (optional) |

**Example:**
```
jdbc:apericio:/data?storageType=s3&s3Bucket=my-bucket&s3Region=us-east-1
```

#### HTTP/HTTPS with Authentication
| Property | Environment Variable | Description |
|----------|---------------------|-------------|
| `storageType` | `APERICIO_STORAGE_TYPE` | Set to `http` or `https` |
| `httpBaseUrl` | `APERICIO_HTTP_BASE_URL` | Base URL for HTTP storage |
| `httpAuthType` | `APERICIO_HTTP_AUTH_TYPE` | Auth type: `bearer`, `basic` |
| `httpAuthToken` | `APERICIO_HTTP_AUTH_TOKEN` | Authentication token |

**Example:**
```
jdbc:apericio:?storageType=http&httpBaseUrl=https://api.example.com/data
```

#### Apache Iceberg Tables
| Property | Environment Variable | Description |
|----------|---------------------|-------------|
| `icebergEnabled` | `APERICIO_ICEBERG_ENABLED` | Enable Iceberg support |
| `icebergCatalogType` | `ICEBERG_CATALOG_TYPE` | Catalog type: `hadoop`, `hive`, `glue` |
| `icebergWarehouse` | `ICEBERG_WAREHOUSE` | Warehouse location |

**Example:**
```
jdbc:apericio:/data?icebergEnabled=true&icebergWarehouse=/path/to/warehouse
```

**Note:** For SharePoint and other complex storage providers, use a model file configuration.

## System Properties

Global JVM settings for performance tuning:

```bash
java -Dcalcite.spillover.dir=/fast-disk/spillover \
     -Dcalcite.file.statistics.cache.directory=/var/cache/apericio \
     -Dcalcite.default.charset=UTF-8 \
     -Xmx8g \
     -XX:MaxDirectMemorySize=4g \
     YourApplication
```

## Environment Variable Reference

Supported environment variables for configuration:

### Core Settings
| Variable | Purpose | Example |
|----------|---------|---------|
| `APERICIO_BATCH_SIZE` | Batch size for processing | `10000` |
| `APERICIO_MEMORY_THRESHOLD` | Memory threshold (bytes) | `536870912` |
| `APERICIO_SPILL_DIRECTORY` | Spillover directory | `/fast-disk/temp` |
| `APERICIO_PARALLELISM` | Query parallelism | `8` |
| `APERICIO_FETCH_SIZE` | JDBC fetch size | `1000` |
| `APERICIO_FILE_PATTERN` | File pattern filter | `*.csv` |
| `APERICIO_EXCLUDE_PATTERN` | Exclude pattern | `*.tmp` |

### Execution Engine
| Variable | Purpose | Example |
|----------|---------|---------|
| `APERICIO_EXECUTION_ENGINE` | Execution engine | `duckdb` |
| `DUCKDB_MEMORY_LIMIT` | DuckDB memory limit | `4GB` |
| `DUCKDB_THREADS` | DuckDB thread count | `4` |
| `DUCKDB_TEMP_DIRECTORY` | DuckDB temp directory | `/tmp/duckdb` |

### CSV Configuration
| Variable | Purpose | Example |
|----------|---------|---------|
| `APERICIO_CSV_INFER_TYPES` | Enable type inference | `true` |
| `CSV_SAMPLING_RATE` | Sampling rate | `0.1` |
| `CSV_MAX_SAMPLE_ROWS` | Max sample rows | `1000` |
| `CSV_INFER_DATES` | Infer date types | `true` |

### Storage Providers
| Variable | Purpose | Example |
|----------|---------|---------|
| `APERICIO_STORAGE_TYPE` | Storage type | `s3` |
| `S3_BUCKET` | S3 bucket name | `my-data-bucket` |
| `AWS_REGION` | AWS region | `us-east-1` |
| `AWS_ACCESS_KEY_ID` | AWS access key | `AKIA...` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `secret...` |
| `APERICIO_HTTP_BASE_URL` | HTTP base URL | `https://api.example.com` |
| `APERICIO_HTTP_AUTH_TYPE` | HTTP auth type | `bearer` |
| `APERICIO_HTTP_AUTH_TOKEN` | HTTP auth token | `token...` |

### Apache Iceberg
| Variable | Purpose | Example |
|----------|---------|---------|
| `APERICIO_ICEBERG_ENABLED` | Enable Iceberg | `true` |
| `ICEBERG_CATALOG_TYPE` | Catalog type | `hadoop` |
| `ICEBERG_WAREHOUSE` | Warehouse location | `/data/iceberg` |

## Configuration Examples

### Development Setup
```
jdbc:apericio:/data?refreshInterval=30s&multiTable=true&lex=ORACLE&enableStatistics=true
```

### Production Analytics
```
jdbc:apericio:model=/etc/apericio/production.json
```

With production.json:
```json
{
  "version": "1.0",
  "defaultSchema": "analytics",
  "schemas": [{
    "name": "analytics",
    "operand": {
      "directory": "${DATA_WAREHOUSE}",
      "enableStatistics": true,
      "batchSize": 50000,
      "memoryThreshold": 1073741824,
      "spillDirectory": "/fast-disk/spillover",
      "statisticsCache": "/var/cache/apericio",
      "tableNameCasing": "LOWER"
    }
  }]
}
```

### Multi-Storage Setup
```json
{
  "schemas": [{
    "name": "local_files",
    "operand": {
      "directory": "/data/local"
    }
  }, {
    "name": "s3_data", 
    "operand": {
      "storageType": "s3",
      "storageConfig": {
        "bucket": "data-lake",
        "region": "us-east-1"
      }
    }
  }, {
    "name": "api_data",
    "operand": {
      "storageType": "http",
      "storageConfig": {
        "baseUrl": "https://api.company.com/data"
      }
    }
  }]
}
```

## Troubleshooting Configuration

### Common Issues

**Configuration not taking effect?**
- Check property spelling and case sensitivity
- Ensure environment variables are set correctly
- Verify JSON syntax in model files

**Performance issues?**
- Increase `batchSize` for large datasets
- Configure adequate `memoryThreshold`  
- Set `spillDirectory` to fast local storage
- Enable `enableStatistics` for frequent queries

**Memory errors?**
- Reduce `batchSize`
- Lower `memoryThreshold` 
- Ensure spillover directory has sufficient space
- Increase JVM heap size with `-Xmx`

**File access errors?**
- Check directory permissions
- Verify environment variable expansion
- Test storage provider credentials

For more troubleshooting, see [Troubleshooting Guide](troubleshooting.md).

## Advanced Topics

### Specialized Configurations
- **[Web Crawling Guide](web-crawling.md)** - HTML table extraction, automated data discovery, and refresh strategies
- **[Storage Providers Guide](storage-providers.md)** - S3, HTTP, SharePoint, SFTP, and authentication configurations
- **[Apache Iceberg Guide](iceberg.md)** - Time travel queries, schema evolution, and ACID transactions
- **[Execution Engines Guide](execution-engines.md)** - DuckDB, Parquet, Arrow, and LINQ4J optimization
- **[Advanced Configuration Reference](advanced-configuration.md)** - Complete configuration options and examples