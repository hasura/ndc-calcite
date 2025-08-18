# Execution Engines Optimization Guide

Complete guide to choosing and optimizing execution engines in the Apericio JDBC Driver for maximum performance across different workloads.

## Overview

The Apericio driver supports multiple execution engines, each optimized for specific workload characteristics:

- **DuckDB** - Advanced SQL analytics with sophisticated optimizations (default for Apericio)
- **Parquet** - Handles unlimited dataset sizes through automatic spillover
- **Arrow** - Maximum in-memory performance with SIMD vectorization
- **LINQ4J** - Simple, low-overhead row processing

This multi-engine approach ensures optimal performance without compromising functionality.

## Engine Selection Strategy

### Workload-Based Engine Selection

| Workload Type | Recommended Engine | Reason |
|---------------|-------------------|---------|
| **Large Analytics** (>1GB) | Parquet | Spillover capability, columnar efficiency |
| **Complex SQL** | DuckDB | Advanced SQL features, query optimization |
| **Real-time Dashboards** | Arrow | In-memory speed, SIMD vectorization |
| **Simple ETL** | LINQ4J | Low overhead, row-by-row processing |
| **Time Travel Queries** | DuckDB | Iceberg integration, temporal SQL |
| **Cross-format Joins** | DuckDB | Universal format support, join optimization |

### Decision Matrix

```
Dataset Size vs Complexity:

High Complexity │ DuckDB        │ DuckDB + Spillover
    ↑            │               │
                 │ Arrow         │ Parquet  
Low Complexity   │               │
                 Small ────────→ Large
                      Dataset Size
```

## DuckDB Engine (Recommended Default)

### Why DuckDB for Apericio

DuckDB is the default engine because it provides:
- **Advanced SQL support** - Window functions, CTEs, complex aggregations
- **Universal format support** - Reads all file formats natively
- **Intelligent optimization** - Vectorized execution, adaptive algorithms
- **Calcite integration** - Pre-optimizer with HyperLogLog statistics

### Basic DuckDB Configuration
```json
{
  "executionEngine": "duckdb",
  "duckdbConfig": {
    "memory_limit": "4GB",
    "threads": 8,
    "enable_progress_bar": false
  }
}
```

### Performance-Optimized DuckDB
```json
{
  "executionEngine": "duckdb",
  "duckdbConfig": {
    "memory_limit": "16GB",
    "threads": 32,
    "temp_directory": "/fast-ssd/duckdb-temp",
    "max_temp_directory_size": "100GB",
    "enable_optimizer": true,
    "enable_profiling": false,
    "checkpoint_threshold": "1GB",
    "enable_object_cache": true,
    "object_cache_size": "2GB"
  }
}
```

### DuckDB Extensions and Features
```json
{
  "duckdbConfig": {
    "extensions": ["parquet", "json", "httpfs", "spatial"],
    "enable_external_access": true,
    "enable_checkpoint_on_shutdown": true,
    "preserve_insertion_order": false,
    "enable_macro_dependencies": true
  }
}
```

### Advanced DuckDB Optimization
```json
{
  "duckdbConfig": {
    "memory_limit": "32GB",
    "threads": 64,
    "worker_threads": 32,
    "enable_optimizer": true,
    "optimizer_join_order": true,
    "force_compression": "auto",
    "checkpoint_wal_size": "1GB",
    "wal_autocheckpoint": 1000,
    "enable_http_metadata_cache": true,
    "http_metadata_cache_size": "1GB"
  }
}
```

## Parquet Engine (Large Dataset Specialist)

### When to Use Parquet Engine
- Datasets > 1GB that don't fit in memory
- Column-heavy analytical queries
- Need for automatic spillover to disk
- Consistent performance regardless of dataset size

### Basic Parquet Configuration
```json
{
  "executionEngine": "parquet",
  "parquetConfig": {
    "compression": "snappy",
    "pageSize": "1MB",
    "enableStatistics": true,
    "enableDictionary": true
  }
}
```

### Performance-Optimized Parquet
```json
{
  "executionEngine": "parquet",
  "parquetConfig": {
    "compression": "zstd",
    "pageSize": "2MB",
    "enableStatistics": true,
    "enableDictionary": true,
    "maxPaddingSize": "8MB",
    "writeBatchSize": 10000,
    "enableBloomFilter": true,
    "bloomFilterFpp": 0.01
  },
  "memoryThreshold": 1073741824,     // 1GB before spillover
  "spilloverDirectory": "/fast-ssd/spillover",
  "batchSize": 50000
}
```

### Parquet with Custom Spillover Strategy
```json
{
  "executionEngine": "parquet",
  "memoryManagement": {
    "memoryThreshold": "2GB",
    "spilloverStrategy": "aggressive",
    "spilloverDirectory": "/nvme-ssd/spillover",
    "maxSpilloverSize": "100GB",
    "compressionLevel": 6,
    "enableSpilloverCompression": true
  }
}
```

## Arrow Engine (In-Memory Speed)

### When to Use Arrow Engine
- Datasets that fit entirely in memory
- Need for maximum query speed
- Real-time analytics and dashboards
- SIMD-optimized computations

### Basic Arrow Configuration
```json
{
  "executionEngine": "arrow",
  "arrowConfig": {
    "batchSize": 10000,
    "enableVectorization": true
  }
}
```

### High-Performance Arrow Configuration
```json
{
  "executionEngine": "arrow", 
  "arrowConfig": {
    "batchSize": 65536,              // Optimize for CPU cache
    "enableVectorization": true,
    "enableSIMD": true,
    "compressionCodec": "lz4",
    "memoryPool": "system",
    "enableZeroCopy": true,
    "enableColumnarBatch": true
  },
  "memoryThreshold": 8589934592      // 8GB limit
}
```

### Arrow with GPU Acceleration (if available)
```json
{
  "executionEngine": "arrow",
  "arrowConfig": {
    "enableGPU": true,
    "gpuMemoryLimit": "4GB",
    "enableCUDA": true,
    "cudaDevice": 0,
    "enableGPUColumnToRow": true
  }
}
```

## LINQ4J Engine (Simple Processing)

### When to Use LINQ4J Engine
- Simple row-by-row processing
- Streaming data scenarios
- Minimal memory footprint requirements
- Legacy compatibility needs

### Basic LINQ4J Configuration
```json
{
  "executionEngine": "linq4j",
  "linq4jConfig": {
    "enableLazyLoading": true,
    "bufferSize": 1000
  }
}
```

### Optimized LINQ4J Configuration
```json
{
  "executionEngine": "linq4j",
  "linq4jConfig": {
    "enableLazyLoading": true,
    "bufferSize": 10000,
    "enableStreaming": true,
    "streamingThreshold": 100000,
    "enableParallelism": false
  }
}
```

## Multi-Engine Architecture

### Different Engines for Different Schemas
```json
{
  "schemas": [
    {
      "name": "hot_cache",
      "operand": {
        "directory": "/data/cache",
        "executionEngine": "arrow",
        "arrowConfig": {
          "batchSize": 65536,
          "enableVectorization": true
        }
      }
    },
    {
      "name": "data_lake", 
      "operand": {
        "storageType": "s3",
        "bucket": "company-data-lake",
        "executionEngine": "parquet",
        "memoryThreshold": 1073741824,
        "spilloverDirectory": "/fast-ssd/spillover"
      }
    },
    {
      "name": "analytics",
      "operand": {
        "directory": "/data/analytics",
        "executionEngine": "duckdb",
        "duckdbConfig": {
          "memory_limit": "16GB",
          "threads": 32
        }
      }
    },
    {
      "name": "streaming",
      "operand": {
        "storageType": "http",
        "baseUrl": "https://api.example.com",
        "executionEngine": "linq4j",
        "refreshInterval": "30 seconds"
      }
    }
  ]
}
```

### Cross-Engine Query Optimization
```sql
-- Automatically optimized cross-engine join
SELECT 
  cache.user_id,
  cache.session_data,        -- From Arrow engine (fast)
  analytics.user_profile,    -- From DuckDB engine (complex)
  lake.historical_purchases  -- From Parquet engine (large)
FROM hot_cache.active_users cache
JOIN analytics.user_profiles analytics 
  ON cache.user_id = analytics.user_id
JOIN data_lake.purchase_history lake 
  ON cache.user_id = lake.customer_id
WHERE cache.last_active > CURRENT_TIMESTAMP - INTERVAL '1' HOUR;
```

## Performance Benchmarking

### Engine Performance Comparison

**Test Setup: 10M row dataset, various query types**

| Query Type | DuckDB | Parquet | Arrow | LINQ4J |
|------------|--------|---------|-------|--------|
| **Simple SELECT** | 0.8s | 1.2s | 0.3s | 2.1s |
| **Complex JOIN** | 2.1s | 3.8s | 1.2s | 15.3s |
| **Aggregation** | 1.5s | 2.2s | 0.9s | 8.7s |
| **Window Functions** | 2.8s | N/A | N/A | N/A |
| **Memory Usage** | 2.1GB | 512MB | 3.2GB | 128MB |

### Choosing Engine by Data Size

```
Performance vs Dataset Size:

Arrow     ████████████░░░░░░░░░░░░░░  (optimal < 1GB)
DuckDB    ████████████████████████░░  (good for any size)
Parquet   ░░░░████████████████████░░  (optimal > 1GB)
LINQ4J    ████░░░░░░░░░░░░░░░░░░░░░░  (optimal < 100MB)

          100MB    1GB     10GB    100GB
                  Dataset Size
```

## Engine-Specific Optimizations

### DuckDB Query Optimization
```sql
-- Enable DuckDB-specific optimizations
PRAGMA enable_optimizer = true;
PRAGMA enable_profiling = false; 
PRAGMA threads = 32;
PRAGMA memory_limit = '16GB';

-- Use DuckDB's advanced features
SELECT 
  customer_id,
  SUM(amount) OVER (
    PARTITION BY customer_id 
    ORDER BY order_date 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) as running_total
FROM orders
WHERE order_date >= '2024-01-01';
```

### Parquet Column Pruning
```json
{
  "parquetConfig": {
    "enableColumnPruning": true,
    "enablePredicatePushdown": true,
    "enableProjectionPushdown": true,
    "enableRowGroupFiltering": true
  }
}
```

### Arrow Vectorization
```json
{
  "arrowConfig": {
    "enableVectorization": true,
    "vectorSize": 1024,
    "enableSIMD": true,
    "targetCPU": "native"
  }
}
```

## Memory Management Strategies

### Adaptive Memory Management
```json
{
  "memoryManagement": {
    "strategy": "adaptive",
    "lowMemoryThreshold": "1GB",
    "highMemoryThreshold": "8GB",
    "spilloverRatio": 0.8,
    "compressionRatio": 0.6,
    "enableMemoryMonitoring": true
  }
}
```

### Engine-Specific Memory Limits
```json
{
  "schemas": [
    {
      "name": "memory_intensive",
      "operand": {
        "executionEngine": "arrow",
        "memoryThreshold": 4294967296,  // 4GB for Arrow
        "arrowConfig": {
          "memoryPool": "jemalloc",
          "enableMemoryMapping": true
        }
      }
    },
    {
      "name": "spillover_safe",
      "operand": {
        "executionEngine": "parquet", 
        "memoryThreshold": 1073741824,  // 1GB before spillover
        "spilloverDirectory": "/nvme/spillover"
      }
    }
  ]
}
```

## Real-World Performance Tuning

### Financial Data Processing
```json
{
  "name": "financial_analytics",
  "operand": {
    "executionEngine": "duckdb",
    "duckdbConfig": {
      "memory_limit": "32GB",
      "threads": 64,
      "enable_optimizer": true,
      "preserve_insertion_order": false,
      "force_compression": "uncompressed"
    },
    "csvTypeInference": {
      "enabled": true,
      "maxSampleRows": 10000,
      "inferDates": true,
      "inferTimestamps": true
    }
  }
}
```

### Real-Time Dashboard
```json
{
  "name": "realtime_dashboard",
  "operand": {
    "executionEngine": "arrow",
    "arrowConfig": {
      "batchSize": 32768,
      "enableVectorization": true,
      "enableSIMD": true,
      "memoryPool": "system"
    },
    "refreshInterval": "10 seconds",
    "enableStatistics": true
  }
}
```

### Large-Scale ETL
```json
{
  "name": "etl_processing",
  "operand": {
    "executionEngine": "parquet",
    "parquetConfig": {
      "compression": "zstd",
      "enableStatistics": true,
      "writeBatchSize": 100000
    },
    "memoryThreshold": 2147483648,    // 2GB
    "spilloverDirectory": "/fast-disk/etl-spillover",
    "batchSize": 50000,
    "maxConcurrency": 16
  }
}
```

## Monitoring and Debugging

### Enable Engine Metrics
```bash
# JVM arguments for detailed engine monitoring
-Dorg.slf4j.simpleLogger.log.org.apache.calcite.adapter.file.execution=DEBUG
-Dduckdb.enable_profiling=true
-Darrow.enable_metrics=true
```

### Performance Profiling
```json
{
  "duckdbConfig": {
    "enable_profiling": true,
    "profiling_output": "/var/log/duckdb-profile.json",
    "profile_output": "json"
  }
}
```

### Query Plan Analysis
```sql
-- DuckDB query plans
EXPLAIN ANALYZE SELECT * FROM large_table WHERE id > 1000000;

-- Arrow execution details  
SET arrow.enable_explain = true;
SELECT * FROM arrow_table WHERE amount > 1000;
```

## Troubleshooting Engine Issues

### Common Performance Problems

**Out of Memory Errors**
```json
{
  "memoryThreshold": 536870912,      // Reduce to 512MB
  "spilloverDirectory": "/large-disk/spillover",
  "executionEngine": "parquet"       // Switch to spillover-capable engine
}
```

**Slow Query Performance**
```json
{
  "executionEngine": "duckdb",       // Use advanced optimizer
  "duckdbConfig": {
    "enable_optimizer": true,
    "threads": 16,                   // Increase parallelism
    "memory_limit": "8GB"
  }
}
```

**Engine Compatibility Issues**
```json
{
  "fallbackEngine": "linq4j",        // Fallback for compatibility
  "engineSelection": "automatic",    // Let system choose optimal engine
  "enableEngineSwitching": true
}
```

This multi-engine architecture ensures that every workload gets optimal performance while maintaining the simplicity of a single JDBC interface.