# Advanced Configuration Reference

Complex configuration scenarios and multi-provider setups for the Apericio JDBC Driver.

## Overview

This guide covers advanced configurations that combine multiple features or require model files. For detailed single-topic configurations, see:

- **[Storage Providers Guide](storage-providers.md)** - S3, HTTP, SharePoint, SFTP details
- **[Web Crawling Guide](web-crawling.md)** - HTML crawler configuration
- **[Apache Iceberg Guide](iceberg.md)** - Time travel and ACID transactions
- **[Execution Engines Guide](execution-engines.md)** - DuckDB, Parquet, Arrow optimization
- **[Configuration Guide](configuration.md)** - Basic settings and environment variables

## Multi-Schema Configurations

### Combining Different Storage Providers

Create a unified view across multiple storage systems:

```json
{
  "version": "1.0",
  "defaultSchema": "unified",
  "schemas": [
    {
      "name": "local_files",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data/local",
        "executionEngine": "duckdb",
        "recursive": true
      }
    },
    {
      "name": "s3_warehouse",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "storageType": "s3",
        "executionEngine": "parquet",
        "storageConfig": {
          "bucket": "data-lake",
          "region": "us-east-1",
          "prefix": "warehouse/"
        }
      }
    },
    {
      "name": "api_data",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "storageType": "http",
        "baseUrl": "https://api.example.com/data",
        "executionEngine": "arrow"
      }
    }
  ]
}
```

Query across all schemas:
```sql
SELECT 
  l.customer_id,
  l.name,
  s.total_orders,
  a.credit_score
FROM local_files.customers l
JOIN s3_warehouse.order_summary s ON l.customer_id = s.customer_id
JOIN api_data.credit_scores a ON l.customer_id = a.customer_id;
```

## Views and Materialized Views

### Regular Views (Virtual Tables)
Regular views are virtual tables that execute their SQL each time they're queried:

```json
{
  "schemas": [{
    "name": "analytics",
    "operand": {
      "views": [{
        "name": "active_customers",
        "sql": "SELECT * FROM customers WHERE status = 'ACTIVE' AND last_order_date > CURRENT_DATE - INTERVAL '90' DAY"
      }, {
        "name": "high_value_orders",
        "sql": "SELECT * FROM orders WHERE total_amount > 1000"
      }]
    }
  }]
}
```

### Materialized Views (Cached Results)
Materialized views cache query results for better performance.

**Important:** Materialized views require either `parquet` or `duckdb` execution engine.

```json
{
  "schemas": [{
    "name": "analytics",
    "operand": {
      "executionEngine": "duckdb",
      "materializedViews": [{
        "view": "customer_360",
        "table": "customer_360_cache",
        "sql": "SELECT c.*, o.lifetime_value, r.risk_score FROM s3_warehouse.customers c JOIN local_files.orders o ON c.id = o.customer_id JOIN api_data.risk r ON c.id = r.customer_id",
        "refreshInterval": "1 hour"
      }]
    }
  }]
}
```

**Key Differences:**
- **Views**: Must be explicitly referenced in queries, execute SQL on each query, always current data, no storage overhead, works with any execution engine
- **Materialized Views**: Can be automatically substituted by the optimizer when querying base tables, pre-computed results, faster queries, configurable refresh, requires parquet or duckdb engine

**Optimizer Behavior Example:**
When you have a materialized view `monthly_summary` on the `sales` table, a query like:
```sql
SELECT YEAR(date) as year, MONTH(date) as month, SUM(amount) as total 
FROM sales 
GROUP BY YEAR(date), MONTH(date)
```
Can be automatically rewritten by the optimizer to use the materialized view instead of scanning the base table.

### Star Schema Optimization

This feature is particularly powerful for star schemas. You can create materialized views for common fact-dimension joins:

```json
{
  "materializedViews": [{
    "view": "sales_cube",
    "table": "sales_cube_cache",
    "sql": "SELECT f.*, d.date_year, d.date_quarter, d.date_month, c.customer_segment, c.customer_region, p.product_category, p.product_brand FROM fact_sales f JOIN dim_date d ON f.date_key = d.date_key JOIN dim_customer c ON f.customer_key = c.customer_key JOIN dim_product p ON f.product_key = p.product_key",
    "refreshInterval": "1 hour"
  }]
}
```

Now, ANY query joining these tables will automatically use the pre-joined materialized view:
```sql
-- This query automatically uses sales_cube instead of joining 4 tables
SELECT customer_region, product_category, SUM(sales_amount)
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key  
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY customer_region, product_category
```

This transforms star schema queries from expensive multi-table joins into simple scans of pre-computed results!

## Hybrid Execution Engine Strategy

Different engines for different workloads in the same configuration:

```json
{
  "schemas": [
    {
      "name": "fast_analytics",
      "operand": {
        "directory": "/data/analytics",
        "executionEngine": "duckdb",
        "duckdbConfig": {
          "memory_limit": "8GB",
          "threads": 16
        },
        "tables": [
          {"name": "fact_sales", "url": "sales_*.parquet"}
        ]
      }
    },
    {
      "name": "streaming_data",
      "operand": {
        "directory": "/data/streaming",
        "executionEngine": "arrow",
        "arrowConfig": {
          "batchSize": 10000,
          "enableVectorization": true
        },
        "refreshInterval": "10 seconds"
      }
    },
    {
      "name": "archive",
      "operand": {
        "storageType": "s3",
        "executionEngine": "parquet",
        "storageConfig": {
          "bucket": "archive-bucket"
        }
      }
    }
  ]
}
```

## Time-Series Data with Iceberg and Partitioning

Combine Iceberg time travel with partitioned data:

```json
{
  "name": "timeseries",
  "operand": {
    "executionEngine": "duckdb",
    "tables": [
      {
        "name": "events_current",
        "format": "iceberg",
        "catalogType": "hadoop",
        "warehousePath": "/iceberg/warehouse",
        "tablePath": "events.current"
      },
      {
        "name": "events_historical",
        "format": "iceberg",
        "catalogType": "hadoop",
        "warehousePath": "/iceberg/warehouse",
        "tablePath": "events.historical",
        "asOfTimestamp": "2024-01-01T00:00:00Z"
      },
      {
        "name": "events_partitioned",
        "url": "events/year=*/month=*/day=*/*.parquet",
        "partitionColumns": ["year", "month", "day"]
      }
    ]
  }
}
```

## Dynamic Authentication with Token Refresh

For APIs requiring token refresh:

```json
{
  "storageType": "http",
  "storageConfig": {
    "baseUrl": "https://api.example.com",
    "authConfig": {
      "tokenCommand": "python /scripts/get_token.py",
      "tokenRefreshInterval": "55 minutes",
      "authHeaders": {
        "Authorization": "Bearer ${DYNAMIC_TOKEN}",
        "X-API-Version": "v2"
      }
    }
  }
}
```

## Performance Optimization Patterns

### Large Dataset Processing
```json
{
  "operand": {
    "batchSize": 50000,
    "memoryThreshold": 2147483648,
    "spillDirectory": "/fast-ssd/spillover",
    "parallelism": 16,
    "enableStatistics": true,
    "statisticsCache": "/var/cache/apericio/stats"
  }
}
```

### Real-Time Dashboard
```json
{
  "operand": {
    "refreshInterval": "5 seconds",
    "cacheEnabled": true,
    "executionEngine": "arrow",
    "arrowConfig": {
      "batchSize": 1000,
      "enableVectorization": true
    }
  }
}
```

## Environment-Specific Configurations

Use environment variables for different deployments:

```json
{
  "schemas": [{
    "name": "${SCHEMA_NAME}",
    "operand": {
      "storageType": "${STORAGE_TYPE}",
      "executionEngine": "${EXECUTION_ENGINE:-duckdb}",
      "storageConfig": {
        "bucket": "${S3_BUCKET}",
        "region": "${AWS_REGION:-us-east-1}"
      },
      "duckdbConfig": {
        "memory_limit": "${DUCKDB_MEMORY:-4GB}",
        "threads": "${DUCKDB_THREADS:-8}"
      }
    }
  }]
}
```

## Complex Web Crawling with Data Extraction

Combine web crawling with file discovery:

```json
{
  "storageType": "http",
  "baseUrl": "https://data.example.com",
  "crawlerConfig": {
    "enabled": true,
    "maxDepth": 3,
    "linkPattern": ".*/reports/.*",
    "dataFilePattern": ".*\\.(csv|xlsx|parquet)$",
    "maxDataFileSize": "100MB",
    "downloadDirectory": "/tmp/crawled-data",
    "generateTablesFromHtml": true,
    "htmlTableMinRows": 5
  },
  "executionEngine": "duckdb"
}
```

See [Web Crawling Guide](web-crawling.md) for detailed crawler configuration options.

## Multi-Tenant Configuration

Separate schemas for different tenants with access control:

```json
{
  "schemas": [
    {
      "name": "tenant_a",
      "operand": {
        "directory": "/data/tenants/a",
        "executionEngine": "duckdb",
        "duckdbConfig": {
          "memory_limit": "2GB"
        }
      }
    },
    {
      "name": "tenant_b",
      "operand": {
        "storageType": "s3",
        "storageConfig": {
          "bucket": "tenant-b-data",
          "prefix": "analytics/"
        },
        "executionEngine": "parquet"
      }
    },
    {
      "name": "shared",
      "operand": {
        "directory": "/data/shared",
        "executionEngine": "arrow",
        "tables": [
          {"name": "reference_data", "url": "ref_*.csv"}
        ]
      }
    }
  ]
}
```

## Troubleshooting Complex Configurations

### Validation Checklist
1. **Schema names** must be unique and lowercase
2. **Factory class** must be specified for each schema
3. **Storage credentials** should use environment variables
4. **Execution engines** require corresponding dependencies in classpath
5. **Memory settings** should account for concurrent schemas

### Performance Debugging
```json
{
  "operand": {
    "enableStatistics": true,
    "enableDebugLogging": true,
    "explainPlanVerbosity": "ALL_ATTRIBUTES"
  }
}
```

### Common Issues
- **Multiple schemas slow**: Use different execution engines per schema
- **Memory errors**: Configure spillover and reduce batch sizes
- **Authentication failures**: Check token refresh intervals
- **Missing tables**: Verify file patterns and storage permissions

## See Also

- [Configuration Guide](configuration.md) - Basic configuration and environment variables
- [Storage Providers Guide](storage-providers.md) - Detailed storage configurations
- [Execution Engines Guide](execution-engines.md) - Engine-specific optimizations
- [Troubleshooting Guide](troubleshooting.md) - Common issues and solutions