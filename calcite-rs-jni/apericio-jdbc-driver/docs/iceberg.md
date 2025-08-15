# Apache Iceberg Integration Guide

Complete guide to using Apache Iceberg tables with the Apericio JDBC Driver for time travel, schema evolution, and enterprise data lake functionality.

## Overview

Apache Iceberg is a high-performance table format designed for large analytic datasets. The Apericio driver provides comprehensive Iceberg support including:

- **Time Travel Queries** - Query data at specific points in time
- **Schema Evolution** - Add, drop, rename columns without breaking existing queries  
- **ACID Transactions** - Atomic, consistent, isolated, durable operations
- **Hidden Partitioning** - Partition on any column without exposing partitioning to users
- **Snapshot Management** - Track all changes with automatic cleanup
- **Performance Optimization** - Column pruning, predicate pushdown, file-level filtering

## Quick Start

### Basic Iceberg Table Access
```java
// Connect to Iceberg table via model file
String url = "jdbc:apericio:model=/path/to/iceberg-config.json";
Connection conn = DriverManager.getConnection(url);

// Query current data (latest snapshot)
ResultSet rs = conn.createStatement().executeQuery(
    "SELECT customer_id, order_total FROM events WHERE order_date >= '2024-01-01'"
);
```

**Basic iceberg-config.json:**
```json
{
  "version": "1.0", 
  "defaultSchema": "analytics",
  "schemas": [{
    "name": "analytics",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "executionEngine": "duckdb",
      "tables": [{
        "name": "events",
        "format": "iceberg",
        "catalogType": "hadoop",
        "warehousePath": "/iceberg/warehouse",
        "tablePath": "analytics.events"
      }]
    }
  }]
}
```

## Time Travel Queries

### Query Specific Snapshot
```json
{
  "name": "events_historical",
  "format": "iceberg",
  "catalogType": "hadoop", 
  "warehousePath": "/iceberg/warehouse",
  "tablePath": "analytics.events",
  "snapshotId": 123456789
}
```

**SQL Usage:**
```sql
-- Query data as it existed at snapshot 123456789
SELECT COUNT(*) FROM events_historical WHERE event_type = 'purchase';
```

### Query Specific Timestamp
```json
{
  "name": "events_yesterday",
  "format": "iceberg",
  "catalogType": "hadoop",
  "warehousePath": "/iceberg/warehouse", 
  "tablePath": "analytics.events",
  "asOfTimestamp": "2024-01-15T23:59:59Z"
}
```

**SQL Usage:**
```sql
-- Query data as it existed yesterday at 11:59 PM
SELECT product_id, SUM(quantity) FROM events_yesterday 
WHERE event_type = 'sale' 
GROUP BY product_id;
```

### Advanced Time Travel with SQL
Using Iceberg's native SQL syntax (if supported by execution engine):
```sql
-- Query specific timestamp (requires DuckDB or compatible engine)
SELECT * FROM events FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00';

-- Query specific snapshot
SELECT * FROM events FOR VERSION AS OF 123456789;

-- Compare data between timestamps
SELECT 
  current.product_id,
  current.sales - historical.sales as sales_growth
FROM events FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP current
JOIN events FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00' historical
  ON current.product_id = historical.product_id;
```

## Temporal Views and Multi-Snapshot Analysis

### Time Range Views
```json
{
  "name": "events_temporal",
  "format": "iceberg",
  "catalogType": "hadoop",
  "warehousePath": "/iceberg/warehouse",
  "tablePath": "analytics.events",
  "timeRange": {
    "start": "2024-01-01T00:00:00Z",
    "end": "2024-12-31T23:59:59Z",
    "snapshotColumn": "snapshot_time",
    "dataColumn": "event_timestamp"
  }
}
```

**SQL Usage:**
```sql
-- Analyze trends across all snapshots in 2024
SELECT 
  DATE_TRUNC('month', snapshot_time) as month,
  COUNT(*) as events_per_snapshot,
  AVG(order_value) as avg_order_value
FROM events_temporal 
WHERE event_type = 'purchase'
GROUP BY DATE_TRUNC('month', snapshot_time)
ORDER BY month;
```

### Multi-Version Comparison Tables
```json
{
  "tables": [
    {
      "name": "events_current",
      "format": "iceberg", 
      "tablePath": "analytics.events"
    },
    {
      "name": "events_last_week",
      "format": "iceberg",
      "tablePath": "analytics.events", 
      "asOfTimestamp": "2024-01-08T00:00:00Z"
    },
    {
      "name": "events_last_month",
      "format": "iceberg",
      "tablePath": "analytics.events",
      "asOfTimestamp": "2024-01-01T00:00:00Z"
    }
  ]
}
```

**SQL Usage:**
```sql
-- Weekly growth analysis
SELECT 
  'current' as period,
  COUNT(*) as total_events
FROM events_current
WHERE event_date >= '2024-01-01'

UNION ALL

SELECT 
  'last_week' as period,
  COUNT(*) as total_events  
FROM events_last_week
WHERE event_date >= '2024-01-01'

UNION ALL

SELECT
  'last_month' as period,
  COUNT(*) as total_events
FROM events_last_month
WHERE event_date >= '2024-01-01';
```

## Catalog Configurations

### Hadoop Catalog (File System Based)
```json
{
  "name": "hadoop_iceberg",
  "format": "iceberg",
  "catalogType": "hadoop",
  "warehousePath": "/iceberg/warehouse",
  "hadoopConfig": {
    "fs.defaultFS": "hdfs://namenode:9000",
    "fs.s3a.endpoint": "s3.amazonaws.com",
    "fs.s3a.access.key": "${AWS_ACCESS_KEY_ID}",
    "fs.s3a.secret.key": "${AWS_SECRET_ACCESS_KEY}"
  }
}
```

### REST Catalog (Remote Catalog Server)
```json
{
  "name": "rest_iceberg",
  "format": "iceberg",
  "catalogType": "rest",
  "catalogConfig": {
    "uri": "https://iceberg-catalog.company.com",
    "warehouse": "warehouse1",
    "authConfig": {
      "type": "oauth2",
      "clientId": "${ICEBERG_CLIENT_ID}",
      "clientSecret": "${ICEBERG_CLIENT_SECRET}",
      "tokenUrl": "https://auth.company.com/oauth/token",
      "scope": "iceberg:read"
    },
    "properties": {
      "client.region": "us-east-1",
      "client.assume-role.arn": "arn:aws:iam::123456789012:role/IcebergAccess"
    }
  }
}
```

### Hive Metastore Catalog
```json
{
  "name": "hive_iceberg",
  "format": "iceberg", 
  "catalogType": "hive",
  "catalogConfig": {
    "metastoreUris": "thrift://hive-metastore:9083",
    "warehousePath": "s3://data-lake/warehouse",
    "properties": {
      "hive.metastore.sasl.enabled": "true",
      "hive.metastore.kerberos.principal": "hive/_HOST@COMPANY.COM"
    }
  }
}
```

### AWS Glue Catalog
```json
{
  "name": "glue_iceberg",
  "format": "iceberg",
  "catalogType": "glue",
  "catalogConfig": {
    "warehousePath": "s3://company-datalake/iceberg",
    "region": "us-east-1",
    "roleArn": "arn:aws:iam::123456789012:role/GlueIcebergAccess",
    "properties": {
      "glue.skip-archive": "true",
      "glue.skip-name-validation": "true"
    }
  }
}
```

## Performance Optimization

### Execution Engine Selection
```json
{
  "operand": {
    "executionEngine": "duckdb",        // Recommended for Iceberg
    "duckdbConfig": {
      "memory_limit": "8GB",
      "threads": 16,
      "enable_object_cache": true,
      "object_cache_size": "1GB"
    }
  }
}
```

### Predicate Pushdown Configuration
```json
{
  "name": "optimized_events",
  "format": "iceberg",
  "tablePath": "analytics.events",
  "optimizations": {
    "enablePredicatePushdown": true,
    "enableColumnPruning": true,
    "enablePartitionPruning": true,
    "enableManifestFiltering": true,
    "enableDataFiltering": true
  }
}
```

### File-Level Filtering
```json
{
  "format": "iceberg",
  "tablePath": "analytics.events",
  "fileFilteringConfig": {
    "enableDataFileFiltering": true,
    "enableDeleteFileFiltering": true,
    "maxManifestReadThreads": 4,
    "manifestCacheSize": "32MB"
  }
}
```

## Schema Evolution Examples

### Add Column (Non-Breaking Change)
```sql
-- Using Iceberg SQL commands (if supported by engine)
ALTER TABLE events ADD COLUMN customer_segment STRING;

-- Or via table configuration
```
```json
{
  "name": "events_v2",
  "format": "iceberg",
  "tablePath": "analytics.events",
  "schemaEvolution": {
    "addColumns": [
      {"name": "customer_segment", "type": "string", "nullable": true}
    ]
  }
}
```

### Rename Column (Non-Breaking Change)
```json
{
  "name": "events_renamed",
  "format": "iceberg", 
  "tablePath": "analytics.events",
  "schemaEvolution": {
    "renameColumns": [
      {"from": "customer_id", "to": "user_id"}
    ]
  }
}
```

### Drop Column (Potentially Breaking)
```json
{
  "name": "events_simplified",
  "format": "iceberg",
  "tablePath": "analytics.events", 
  "schemaEvolution": {
    "dropColumns": ["deprecated_field", "old_timestamp"]
  }
}
```

## Partitioning Strategies

### Time-Based Partitioning
```json
{
  "name": "events_partitioned",
  "format": "iceberg",
  "tablePath": "analytics.events",
  "partitioning": {
    "strategy": "time",
    "columns": ["event_date"],
    "granularity": "day",
    "transforms": ["date"]
  }
}
```

### Hash Partitioning
```json
{
  "name": "events_hash_partitioned", 
  "format": "iceberg",
  "tablePath": "analytics.events",
  "partitioning": {
    "strategy": "hash",
    "columns": ["customer_id"], 
    "buckets": 16
  }
}
```

### Composite Partitioning
```json
{
  "name": "events_composite_partitioned",
  "format": "iceberg",
  "tablePath": "analytics.events",
  "partitioning": {
    "strategy": "composite",
    "partitions": [
      {"column": "event_date", "transform": "year"},
      {"column": "event_date", "transform": "month"}, 
      {"column": "region", "transform": "identity"},
      {"column": "customer_id", "transform": "bucket", "buckets": 8}
    ]
  }
}
```

## Snapshot Management

### Automatic Snapshot Cleanup
```json
{
  "format": "iceberg",
  "tablePath": "analytics.events",
  "snapshotManagement": {
    "enableAutoCleanup": true,
    "retainSnapshots": 100,
    "retainDays": 30,
    "cleanupFrequency": "daily"
  }
}
```

### Manual Snapshot Operations
```sql
-- List all snapshots (if supported by engine)
SHOW SNAPSHOTS FOR events;

-- Expire old snapshots
CALL expire_snapshots('analytics.events', TIMESTAMP '2024-01-01 00:00:00');

-- Rollback to previous snapshot
CALL rollback_to_snapshot('analytics.events', 123456789);
```

## Advanced Use Cases

### Data Versioning for ML Pipelines
```json
{
  "schemas": [
    {
      "name": "ml_features_v1",
      "operand": {
        "tables": [{
          "name": "customer_features",
          "format": "iceberg",
          "tablePath": "ml.customer_features",
          "snapshotId": 100,
          "tag": "model-v1-training"
        }]
      }
    },
    {
      "name": "ml_features_v2", 
      "operand": {
        "tables": [{
          "name": "customer_features",
          "format": "iceberg",
          "tablePath": "ml.customer_features",
          "snapshotId": 200,
          "tag": "model-v2-training"
        }]
      }
    }
  ]
}
```

**SQL Usage:**
```sql
-- Compare feature distributions between model versions
SELECT 
  'v1' as model_version,
  AVG(feature_1) as avg_feature_1,
  STDDEV(feature_1) as std_feature_1
FROM ml_features_v1.customer_features

UNION ALL

SELECT
  'v2' as model_version, 
  AVG(feature_1) as avg_feature_1,
  STDDEV(feature_1) as std_feature_1
FROM ml_features_v2.customer_features;
```

### Compliance and Audit Trails
```json
{
  "name": "audit_events",
  "format": "iceberg", 
  "tablePath": "compliance.user_actions",
  "complianceConfig": {
    "enableAuditLog": true,
    "retainAuditYears": 7,
    "encryptionKey": "${COMPLIANCE_ENCRYPTION_KEY}",
    "accessLogging": true
  }
}
```

**Audit Queries:**
```sql
-- Who accessed what data when
SELECT 
  snapshot_id,
  commit_timestamp,
  operation,
  summary['added-files-size'] as data_size,
  summary['added-records'] as record_count
FROM table_snapshots('compliance.user_actions')
WHERE commit_timestamp >= '2024-01-01'
ORDER BY commit_timestamp DESC;

-- Data lineage tracking  
SELECT DISTINCT
  file_path,
  file_size,
  record_count,
  partition_values
FROM table_files('compliance.user_actions')
WHERE snapshot_id = 123456789;
```

### Cross-Table Time Travel Analysis
```sql
-- Compare customer behavior across different time periods
WITH customers_q1 AS (
  SELECT customer_id, COUNT(*) as orders_q1
  FROM events FOR SYSTEM_TIME AS OF TIMESTAMP '2024-03-31 23:59:59'
  WHERE event_type = 'order' 
    AND event_date BETWEEN '2024-01-01' AND '2024-03-31'
  GROUP BY customer_id
),
customers_q2 AS (
  SELECT customer_id, COUNT(*) as orders_q2  
  FROM events FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-30 23:59:59'
  WHERE event_type = 'order'
    AND event_date BETWEEN '2024-04-01' AND '2024-06-30' 
  GROUP BY customer_id
)
SELECT 
  COALESCE(q1.customer_id, q2.customer_id) as customer_id,
  COALESCE(q1.orders_q1, 0) as q1_orders,
  COALESCE(q2.orders_q2, 0) as q2_orders,
  COALESCE(q2.orders_q2, 0) - COALESCE(q1.orders_q1, 0) as growth
FROM customers_q1 q1 
FULL OUTER JOIN customers_q2 q2 ON q1.customer_id = q2.customer_id
ORDER BY growth DESC;
```

## Troubleshooting

### Common Issues

**"Table not found" Errors**
```json
{
  "catalogConfig": {
    "caseSensitive": false,        // Try case-insensitive matching
    "allowCaseSensitiveIdentifiers": true
  }
}
```

**Performance Issues with Large Tables**
```json
{
  "optimizations": {
    "enableManifestCaching": true,
    "manifestCacheSize": "128MB",
    "enablePlanningParallelism": true,
    "planningThreads": 8
  }
}
```

**Authentication Failures**
```bash
# Check catalog connectivity
curl -H "Authorization: Bearer $TOKEN" https://iceberg-catalog.company.com/v1/config

# Verify AWS credentials for S3-based warehouses  
aws s3 ls s3://iceberg-warehouse/analytics/
```

**Schema Evolution Conflicts**
```sql
-- Check current schema
DESCRIBE events;

-- Check schema history
SHOW SCHEMAS FOR events;

-- Force schema refresh if needed
REFRESH TABLE events;
```

### Performance Monitoring

**Enable Iceberg metrics:**
```bash
# Add JVM arguments
-Diceberg.engine.hive.metrics-reporters=logging
-Diceberg.metrics.timer.enabled=true
```

**Monitor catalog performance:**
```json
{
  "catalogConfig": {
    "enableMetrics": true,
    "metricsReporter": "slf4j",
    "enableQueryProfiling": true
  }
}
```

## Integration with BI Tools

### Tableau/PowerBI Integration
```sql
-- Create views for BI tool consumption
CREATE VIEW current_sales AS 
SELECT * FROM events 
WHERE event_type = 'sale' 
  AND event_date >= CURRENT_DATE - INTERVAL '90' DAY;

CREATE VIEW historical_sales AS
SELECT * FROM events FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00'
WHERE event_type = 'sale';
```

### Apache Superset Integration
```python
# Superset connection string
connection_string = "apericio://iceberg-config.json"

# Time travel queries in Superset SQL Lab
sql = """
SELECT DATE_TRUNC('month', event_date) as month,
       SUM(order_value) as revenue
FROM events FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-30 23:59:59'
WHERE event_type = 'purchase'
GROUP BY DATE_TRUNC('month', event_date)
ORDER BY month
"""
```

This comprehensive Iceberg integration enables enterprise-grade data lake functionality with time travel, schema evolution, and ACID guarantees while maintaining high query performance through the Apericio driver's optimization features.