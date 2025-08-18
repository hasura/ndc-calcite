he fil
# Supported File Formats

Comprehensive guide to all file formats supported by the Apericio JDBC Driver.

## Overview

The Apericio JDBC Driver automatically detects and converts 20+ file formats into queryable SQL tables. Files are discovered by extension and converted using optimized readers.

## Text-Based Formats

### CSV (Comma-Separated Values)
**Extensions:** `.csv`, `.csv.gz`, `.csv.bz2`
**Table Naming:** `sales.csv` → `sales`

**Features:**
- Automatic type inference (String, Integer, Decimal, Date)
- Header detection
- Configurable separators (comma, tab, semicolon, pipe)
- Compression support (gzip, bzip2)
- Character encoding detection

**Configuration:**
```java
// Via URL - character encoding
jdbc:apericio:/data?encoding=UTF-8

// Via properties  
props.setProperty("encoding", "UTF-8");

// CSV type inference (via model file)
{
  "csvTypeInference": {
    "enabled": true,
    "samplingRate": 0.1,
    "maxSampleRows": 1000,
    "confidenceThreshold": 0.95
  }
}
```

**Automatic Detection:**
- **Header detection** - Automatically identifies CSV headers vs data rows
- **Separator detection** - Auto-detects comma, tab, semicolon, pipe separators  
- **Type inference** - Optional automatic column type detection (INTEGER, DATE, TIMESTAMP, etc.)
- **Encoding detection** - Falls back to UTF-8 if not specified

**Example:**
```csv
id,customer_name,amount,order_date
1,Alice Smith,1250.00,2024-01-15
2,Bob Jones,875.50,2024-01-16
```
Becomes queryable as:
```sql
SELECT customer_name, amount FROM sales WHERE amount > 1000;
```

### TSV (Tab-Separated Values)
**Extensions:** `.tsv`, `.tab`, `.txt` (with tabs)
**Features:** Same as CSV but with tab delimiters

### JSON (JavaScript Object Notation)
**Extensions:** `.json`, `.jsonl`, `.ndjson`
**Table Naming:** `orders.json` → `orders`

**Features:**
- Nested object flattening
- Array expansion
- Multi-table extraction from single file
- JSONL (line-delimited JSON) support

**Single Object JSON:**
```json
{
  "customers": [
    {"id": 1, "name": "Alice", "address": {"city": "NYC", "zip": "10001"}},
    {"id": 2, "name": "Bob", "address": {"city": "LA", "zip": "90210"}}
  ]
}
```

**Multi-table extraction:**
```json
{
  "jsonTables": [{
    "name": "customers",
    "path": "$.customers[*]"
  }]
}
```

Flattened query result:
```sql
SELECT id, name, address_city, address_zip FROM customers;
```

### YAML
**Extensions:** `.yaml`, `.yml`
**Features:** Similar to JSON with YAML syntax support

## Binary Columnar Formats

### Apache Parquet
**Extensions:** `.parquet`, `.pq`
**Features:**
- Columnar storage format
- Excellent compression and query performance
- Schema preservation
- Predicate pushdown support
- Not memory-limited - handles TB-scale files

**Benefits:**
- 10-100x faster than CSV for analytical queries
- Automatic column pruning
- Efficient aggregations and filters
- Native support for complex data types

**Query example:**
```sql
-- Efficient even on billion-row Parquet files
SELECT region, SUM(sales) 
FROM large_sales_data 
WHERE year = 2024 
GROUP BY region;
```

### Apache Arrow
**Extensions:** `.arrow`, `.feather`
**Features:**
- In-memory columnar format
- SIMD vectorized processing
- Zero-copy data access
- Memory-mapped files
- Best for datasets that fit in memory

## Spreadsheet Formats

### Microsoft Excel
**Extensions:** `.xlsx`, `.xls`
**Table Naming:** 
- Single sheet: `report.xlsx` → `report`
- Multi-sheet: `report.xlsx` → `report__sheet1`, `report__summary`, `report__details`

**Features:**
- Multi-sheet support (each sheet becomes a table)
- Multi-table detection within sheets (with `multiTable=true`)
- Automatic data type inference
- Formula evaluation (where possible)

**Configuration:**
```java
// Enable multi-table detection within sheets
jdbc:apericio:/data?multiTable=true
```

**Example:**
Excel file `financial_report.xlsx` with sheets [Summary, Q1_Details, Q2_Details]:
```sql
SELECT * FROM financial_report__summary;
SELECT * FROM financial_report__q1_details;
SELECT * FROM financial_report__q2_details;
```

## Web and Markup Formats

### HTML
**Extensions:** `.html`, `.htm`
**Features:**
- Extract `<table>` elements as separate tables
- Web crawling support with link following
- File download from crawled pages
- Custom table selectors

**Table naming:** `report.html` → `report__table_1`, `report__table_2`

**Web crawling configuration:**
```json
{
  "crawlEnabled": true,
  "crawlMaxDepth": 3,
  "crawlFollowPattern": ".*\\/(data|reports)\\/.*",
  "crawlExtractFiles": true,
  "crawlFilePatterns": ["*.xlsx", "*.csv", "*.json"]
}
```

### Markdown
**Extensions:** `.md`, `.markdown`
**Features:**
- Extract markdown tables
- Multiple tables per file supported

**Example:**
```markdown
## Sales Data

| ID | Customer | Amount |
|----|----------|--------|
| 1  | Alice    | 1250   |
| 2  | Bob      | 875    |

## Customer Info

| ID | City | State |
|----|------|-------|
| 1  | NYC  | NY    |
| 2  | LA   | CA    |
```

Becomes: `report__table_1`, `report__table_2`

### XML
**Extensions:** `.xml`
**Features:**
- XPath-based table extraction
- Multiple table extraction from single file
- Configurable element selection

**Configuration:**
```json
{
  "xmlTables": [{
    "name": "products",
    "xpath": "//catalog/product"
  }, {
    "name": "categories", 
    "xpath": "//catalog/category"
  }]
}
```

## Document Formats

### Microsoft Word
**Extensions:** `.docx`
**Features:**
- Extract tables from Word documents
- Multiple tables per document
- Preserve basic formatting

**Table naming:** `document.docx` → `document__table_1`, `document__table_2`

### Microsoft PowerPoint
**Extensions:** `.pptx`
**Features:**
- Extract tables from slides
- Include slide context in table names
- Support for multiple tables per slide

**Table naming:** 
- `presentation.pptx` → `presentation__slide1__table1`
- With slide titles → `presentation__sales_summary__table1`

## Advanced Formats

### Apache Iceberg
**Table Format:** Not file-based, configured in model
**Features:**
- Schema evolution
- Time travel queries
- ACID transactions
- Snapshot management
- Hidden partitioning

**Configuration:**
```json
{
  "name": "events",
  "format": "iceberg",
  "catalogType": "hadoop",
  "warehousePath": "/iceberg/warehouse",
  "tablePath": "database.events"
}
```

**Time travel queries:**
```sql
-- Query specific timestamp
SELECT * FROM events FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00';

-- Query specific snapshot
SELECT * FROM events FOR VERSION AS OF 123456789;
```

**Temporal views:**
```json
{
  "timeRange": {
    "start": "2024-01-01T00:00:00Z",
    "end": "2024-12-31T23:59:59Z",
    "snapshotColumn": "snapshot_time"
  }
}
```

## Format-Specific Features

### Compression Support
| Format | Compression | Notes |
|--------|-------------|-------|
| CSV | gzip, bzip2 | `.csv.gz`, `.csv.bz2` |
| JSON | gzip | `.json.gz` |
| Parquet | Built-in | Snappy, LZ4, GZIP |
| Arrow | Built-in | LZ4, ZSTD |

### Character Encoding
All text formats support configurable encoding:
```java
jdbc:apericio:/data?charset=UTF-8
jdbc:apericio:/data?charset=ISO-8859-1
jdbc:apericio:/data?charset=Windows-1252
```

### Type Inference
Automatic type detection for:
- **Integers** - Whole numbers
- **Decimals** - Numbers with decimal points  
- **Dates** - ISO format, common date patterns
- **Booleans** - true/false, yes/no, 1/0
- **Strings** - Everything else

Override with explicit casting:
```sql
SELECT 
    CAST(amount AS DECIMAL(10,2)) as amount,
    CAST(order_date AS DATE) as order_date
FROM sales;
```

## Multi-Table Extraction

### When Multiple Tables Are Created

1. **Excel files** - Each sheet becomes a table
2. **HTML files** - Each `<table>` element becomes a table  
3. **JSON files** - With `jsonTables` configuration
4. **XML files** - With `xmlTables` configuration
5. **Word/PowerPoint** - Each document table becomes a table
6. **Markdown** - Each markdown table becomes a table

### Enable Multi-Table Detection
```java
// For Excel sheets and HTML tables within single sheets/pages
jdbc:apericio:/data?multiTable=true
```

## Performance by Format

| Format | Read Speed | Query Speed | Memory Usage | Best For |
|--------|------------|-------------|--------------|----------|
| **Parquet** | Fast | Very Fast | Low | Analytics, large datasets |
| **Arrow** | Very Fast | Very Fast | High | In-memory analytics |
| **CSV** | Slow | Slow | Medium | Simple data exchange |
| **JSON** | Medium | Slow | Medium | API data, nested structures |
| **Excel** | Slow | Slow | High | Business reports |
| **Iceberg** | Fast | Very Fast | Low | Enterprise data lakes |

## Best Practices

### Format Selection
- **Use Parquet** for large datasets and analytical workloads
- **Use Arrow** for in-memory processing and SIMD operations
- **Use CSV** for simple data exchange and human readability
- **Use JSON** for nested data and API integration
- **Use Iceberg** for enterprise data lakes with versioning needs

### Performance Optimization
- Convert CSV to Parquet for repeated analytical queries
- Enable statistics for instant COUNT(DISTINCT): `enableStatistics=true`
- Use columnar formats (Parquet/Arrow) for column-heavy queries
- Configure appropriate batch sizes based on memory and data size

### Memory Management
```java
// Large datasets
jdbc:apericio:/data?batchSize=10000&memoryThreshold=268435456&spillDirectory=/tmp

// Small datasets  
jdbc:apericio:/data?batchSize=1000&memoryThreshold=67108864
```

## Troubleshooting Format Issues

### File not recognized
- Verify file extension is supported
- Check file isn't corrupted or empty
- Ensure proper permissions

### Parsing errors
- Verify character encoding matches file
- Check for malformed data (invalid JSON, broken CSV)
- Try with simpler configuration first

### Performance issues
- Consider converting to Parquet format
- Adjust batch size and memory settings
- Enable spillover for large files

For more troubleshooting, see [Troubleshooting Guide](troubleshooting.md).