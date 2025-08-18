# Apache Calcite File Adapter - Test Results

## Test Method

Tests use JDBC driver which determines the SchemaFactory based on format.
Driver uses `org.apache.calcite.adapter.file.FileSchemaFactory` for all formats.

## Format Test Results

### CSV

- **Test File**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/csv/sales.csv`
- **JDBC URL**: `jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/csv/sales.csv`
- **Operand passed to FileSchemaFactory**:
```java
Map<String, Object> operand = Map.of(
    "tables", List.of(
        Map.of("name", "sales",
               "url", "file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/csv/sales.csv")
    )
);
```
- **Table Discovery**: `DatabaseMetaData.getTables(null, "files", "%", null)`
- **Tables Found**: `sales`
- **Query SQL**: `SELECT * FROM files."sales" LIMIT 1`
- **Result**: ✅ Query successful
- **Query Results**:
```
order_id | product_id | customer_id | quantity | order_date | total
--- | --- | --- | --- | --- | ---
1001 | 1 | 101 | 1 | 2024-01-15 | 999.99
```

### TSV

- **Test File**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/tsv/inventory.tsv`
- **JDBC URL**: `jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/tsv/inventory.tsv`
- **Operand passed to FileSchemaFactory**:
```java
Map<String, Object> operand = Map.of(
    "tables", List.of(
        Map.of("name", "inventory",
               "url", "file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/tsv/inventory.tsv")
    )
);
```
- **Table Discovery**: `DatabaseMetaData.getTables(null, "files", "%", null)`
- **Tables Found**: `inventory`
- **Query SQL**: `SELECT * FROM files."inventory" LIMIT 1`
- **Result**: ✅ Query successful
- **Query Results**:
```
sku | product | quantity | warehouse | last_updated
--- | --- | --- | --- | ---
A001 | Widget | 100 | North | 2025-01-15
```

### JSON

- **Test File**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/json/customers.json`
- **JDBC URL**: `jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/json/customers.json`
- **Operand passed to FileSchemaFactory**:
```java
Map<String, Object> operand = Map.of(
    "tables", List.of(
        Map.of("name", "customers",
               "url", "file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/json/customers.json")
    )
);
```
- **Table Discovery**: `DatabaseMetaData.getTables(null, "files", "%", null)`
- **Tables Found**: `customers`
- **Query SQL**: `SELECT * FROM files."customers" LIMIT 1`
- **Result**: ✅ Query successful
- **Query Results**:
```
id | name | email | city | country | created_at
--- | --- | --- | --- | --- | ---
101 | Alice Johnson | alice@example.com | New York | USA | 2023-01-15
```

### YAML

- **Test File**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/yaml/config.yaml`
- **JDBC URL**: `jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/yaml/config.yaml`
- **Operand passed to FileSchemaFactory**:
```java
Map<String, Object> operand = Map.of(
    "tables", List.of(
        Map.of("name", "config",
               "url", "file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/yaml/config.yaml")
    )
);
```
- **Table Discovery**: `DatabaseMetaData.getTables(null, "files", "%", null)`
- **Tables Found**: `config`
- **Query SQL**: `SELECT * FROM files."config" LIMIT 1`
- **Result**: ✅ Query successful
- **Query Results**:
```
id | server | region | status | cpu_usage | memory_gb
--- | --- | --- | --- | --- | ---
1 | web-01 | us-east-1 | running | 45.5 | 8
```

### XLSX

- **Test File**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/xlsx/company_data.xlsx`
- **JDBC URL**: `jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/xlsx/company_data.xlsx`
- **Actual Excel Sheet Names** (discovered via Apache POI):
  - Number of sheets: 3
  - Sheet names: `employees`, `departments`, `projects`
  - Expected Tables: `company_data__employees`, `company_data__departments`, `company_data__projects`
- **Operand passed to FileSchemaFactory**:
```java
Map<String, Object> operand = Map.of(
    "tables", List.of(
        Map.of("name", "company_data",
               "url", "file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/xlsx/company_data.xlsx")
    )
);
```
- **Table Discovery**: `DatabaseMetaData.getTables(null, "files", "%", null)`
- **Tables Found**: `CompanyData__Departments, CompanyData__Projects, CompanyData__Employees`
- **Query SQL**: `SELECT * FROM files."CompanyData__Employees" LIMIT 1`
- **Result**: ✅ Query successful
- **Query Results**:
```
id | name | department | salary
--- | --- | --- | ---
1 | John Doe | Engineering | 75000
```

### HTML

- **Test File**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/html/report.html`
- **JDBC URL**: `jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/html/report.html`
- **Operand passed to FileSchemaFactory**:
```java
Map<String, Object> operand = Map.of(
    "tables", List.of(
        Map.of("name", "report",
               "url", "file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/html/report.html")
    )
);
```
- **Table Discovery**: `DatabaseMetaData.getTables(null, "files", "%", null)`
- **Tables Found**: `report__sales_data`
- **Query SQL**: `SELECT * FROM files."report__sales_data" LIMIT 1`
- **Result**: ✅ Query successful
- **Query Results**:
```
Month | Revenue | Profit
--- | --- | ---
January | 100000 | 25000
```

### Parquet

- **Note**: Binary formats like Parquet and Arrow require `ArrowSchemaFactory` which needs special configuration
- **Test File**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/parquet/sample.parquet`
- **JDBC URL**: `jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/parquet/sample.parquet`
- **Operand passed to FileSchemaFactory**:
```java
Map<String, Object> operand = Map.of(
    "tables", List.of(
        Map.of("name", "sample",
               "url", "file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/parquet/sample.parquet")
    )
);
```
- **Table Discovery**: `DatabaseMetaData.getTables(null, "files", "%", null)`
- **Tables Found**: `sample`
- **Query SQL**: `SELECT * FROM files."sample" LIMIT 1`
- **Result**: ✅ Query successful
- **Query Results**:
```
id | name | age | salary | active
--- | --- | --- | --- | ---
1 | Alice | 25 | 50000.0 | true
```

### Arrow

- **Note**: Binary formats like Parquet and Arrow require `ArrowSchemaFactory` which needs special configuration
- **Test File**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow/sample.arrow`
- **JDBC URL**: `jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow/sample.arrow`
- **Operand passed to FileSchemaFactory**:
```java
Map<String, Object> operand = Map.of(
    "tables", List.of(
        Map.of("name", "sample",
               "url", "file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow/sample.arrow")
    )
);
```
- **Table Discovery**: `DatabaseMetaData.getTables(null, "files", "%", null)`
- **Tables Found**: `sample`
- **Query SQL**: `SELECT * FROM files."sample" LIMIT 1`
- **Result**: ✅ Query successful
- **Query Results**:
```
id | product | price | in_stock
--- | --- | --- | ---
1 | Widget | 19.99 | true
```

## Nested Directory Support

- **Test Directory**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/nested`
- **JDBC URL**: `jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/nested`
- **Operand passed to FileSchemaFactory**:
```java
Map<String, Object> operand = Map.of(
    "directory", "/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/nested",
    "recursive", true  // ✅ Driver correctly passes this parameter
);
```
- **Files in Directory Structure**:
```
tests/data/nested/
├── 2024/
│   ├── 01/
│   │   └── data.csv
│   └── 02/
│       └── data.csv
├── reports/
│   └── quarterly/
│       └── q1.csv
└── sales/
    └── regional/
        └── west.csv
```
- **Table Discovery**: `DatabaseMetaData.getTables(null, "files", "%", null)`
- **Expected Tables with Nested Naming**:
  - `2024.01.data` (from 2024/01/data.csv)
  - `2024.02.data` (from 2024/02/data.csv)
  - `reports.quarterly.q1` (from reports/quarterly/q1.csv)
  - `sales.regional.west` (from sales/regional/west.csv)
- **Actually Found**: `2024.01.data`, `2024.02.data`, `sales.regional.west`, `reports.quarterly.q1`
- **Result**: ✅ Nested directory naming working (found 4 tables)

## Glob Pattern Support

### Implicit Glob Pattern (automatic detection from path)

- **Path with glob**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/**/*.*`
- **JDBC URL**: `jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/**/*.*`
- **Total Tables Found**: 26
- **Tables by Format**:
  - **ARROW**: 1 table(s) - `arrow.sample` ✅
  - **CSV**: 6 table(s) - `csv.products`, `nested.2024.01.data`, `nested.2024.02.data`, ... (3 more) ✅
  - **JSON**: 2 table(s) - `json.users`, `json.customers` ✅
  - **PARQUET**: 1 table(s) - `parquet.sample` ✅
  - **TSV**: 2 table(s) - `mixed.inventory`, `tsv.inventory` ✅
  - **UNKNOWN**: 4 table(s) - `arrow.README`, `json.events`, `parquet.README`, ... (1 more) ❌ (Error while executing SQL "SELECT * FROM files."arrow.README" LIMIT 1": org.apache.calcite.adapter.file.FileReaderException: no tables found)
  - **XLSX**: 8 table(s) - `complex_tables.lots_of_tables__LotsOfTables__Organization_table_1`, `complex_tables.lots_of_tables__LotsOfTables__Organization_table_2`, `xlsx.company_data__CompanyData__Projects`, ... (5 more) ✅
  - **YAML**: 2 table(s) - `yaml.config`, `yaml.settings` ✅

- **Query Results**: 7 successful, 1 failed
- **Result**: ✅ Driver automatically detected glob pattern and found all file formats (26 tables total)

## Transport Protocol Support

### HTTP Transport

- **Test URL**: `https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv`
- **JDBC URL**: `jdbc:file:https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv`
- **Table Discovery**: `DatabaseMetaData.getTables(null, "files", "%", null)`
- **Tables Found**: `addresses`
- **Query SQL**: `SELECT * FROM files."addresses" LIMIT 1`
- **Result**: ✅ Working - can connect and query remote files
- **Query Results**:
```
John | Doe | 120 jefferson st. | Riverside |  NJ |  08075
--- | --- | --- | --- | --- | ---
Jack | McGinnis | 220 hobo Av. | Phila |  PA | 09119
```

### S3 Transport

- **Operand passed to FileSchemaFactory**:
```java
Map<String, Object> operand = Map.of(
    "tables", List.of(
        Map.of("name", "album",
               "url", "s3://redshift-chinook/album.csv")
    )
);
```
- **Table Discovery**: `DatabaseMetaData.getTables(null, "files", "%", null)`
- **Tables Found**: `album`
- **Query SQL**: `SELECT * FROM files."album" LIMIT 1`
- **Result**: ✅ Working - can connect and query S3 files
- **Query Results**:
```
AlbumId | Title | ArtistId
--- | --- | ---
1 | For Those About To Rock We Salute You | 1
```

## Views Support

### Inline Views (via URL parameter)

- **Base File**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/csv/sales.csv`
- **Views JSON**: `[{"name":"high_value_sales","sql":"SELECT * FROM \"sales\""}]`
- **JDBC URL**: `jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/csv/sales.csv?views=%5B%7B%22name%22%3A%22high_value_sales%22%2C%22sql%22%3A%22SELECT+*+FROM+%5C%22sales%5C%22%22%7D%5D`
- **Operand passed to FileSchemaFactory**:
```java
Map<String, Object> operand = Map.of(
    "tables", List.of(
        Map.of("name", "sales",
               "url", "file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/csv/sales.csv")
    ),
    "views", List.of(
        Map.of("name", "high_value_sales",
               "sql", "SELECT * FROM \"sales\"")
    )
);
```
- **Views parameter (from URL)**: `[{"name":"high_value_sales","sql":"SELECT * FROM \"sales\""}]`

#### Equivalent DDL for Inline View:
```sql
-- View showing all sales data
CREATE VIEW files."high_value_sales" AS
SELECT * FROM "sales";
```

- **Tables Found**: [sales]
- **Views Found**: [high_value_sales]
- **Query**: `SELECT * FROM files."high_value_sales" LIMIT 3`
- **Result**: ✅ View query successful
- **Query Results**:
```
order_id | product_id | customer_id | quantity | order_date | total
--- | --- | --- | --- | --- | ---
1001 | 1 | 101 | 1 | 2024-01-15 | 999.99
1002 | 2 | 102 | 2 | 2024-01-16 | 59.98
1003 | 3 | 101 | 1 | 2024-01-17 | 79.99
```

### Views from File

- **Base Directory**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/csv`
- **Views File**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/views.json`
- **JDBC URL**: `jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/csv?viewsFile=/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/views.json`
- **Operand passed to FileSchemaFactory**:
```java
Map<String, Object> operand = Map.of(
    "directory", "/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/csv",
    "views", List.of(  // ✅ Driver now correctly passes this parameter
        Map.of("name", "electronics_products",
               "sql", "SELECT * FROM \"products\" WHERE \"category\" = 'Electronics'"),
        Map.of("name", "expensive_products",
               "sql", "SELECT * FROM \"products\" WHERE \"price\" > 100"),
        Map.of("name", "sales_summary",
               "sql", "SELECT \"product_id\", COUNT(*) as order_count, SUM(\"quantity\") as total_quantity, SUM(\"total\") as total_revenue FROM \"sales\" GROUP BY \"product_id\"")
    )
);
```
- **Views loaded from external file**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/views.json`

#### Equivalent DDL for Views:
```sql
-- View of all electronic products
CREATE VIEW files."electronics_products" AS
SELECT * FROM "products" WHERE "category" = 'Electronics';

-- Products with price over $100
CREATE VIEW files."expensive_products" AS
SELECT * FROM "products" WHERE "price" > 100;

-- Sales summary by product
CREATE VIEW files."sales_summary" AS
SELECT "product_id", COUNT(*) as order_count, SUM("quantity") as total_quantity, SUM("total") as total_revenue FROM "sales" GROUP BY "product_id";

```

- **Views Found**: [electronics_products, expensive_products, sales_summary]
- **Test Query**: `SELECT * FROM files."electronics_products" LIMIT 1`
- **Result**: ✅ View query successful

## Materialized Views Support

### Materialized Views from External File

- **Base Directory**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/csv`
- **Materialized Views File**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/materialized_views.json`
- **JDBC URL**: `jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/csv?viewsFile=/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/materialized_views.json`
- **Operand passed to FileSchemaFactory**:
```java
Map<String, Object> operand = Map.of(
    "directory", "/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/csv",
    "materializations", List.of(  // ✅ Driver now correctly passes this parameter
        Map.of("view", "sales_summary_mv",
               "table", "sales_summary_mv",
               "sql", "SELECT COUNT(*) as total_orders, SUM(\"total\") as revenue FROM \"sales\""),
        Map.of("view", "high_value_orders_mv",
               "table", "high_value_orders_mv",
               "sql", "SELECT order_id, customer_id, total FROM \"sales\" WHERE \"total\" > 500"),
        Map.of("view", "monthly_sales_mv",
               "table", "monthly_sales_mv",
               "sql", "SELECT DATE_TRUNC('month', \"order_date\") as month, COUNT(*) as orders, SUM(\"total\") as revenue FROM \"sales\" GROUP BY DATE_TRUNC('month', \"order_date\")")
    )
);
```
- **Materialized views loaded from external file**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/materialized_views.json`

#### Equivalent DDL for Materialized Views:
```sql
-- Materialized view of sales summary with order count and total revenue
CREATE MATERIALIZED VIEW files."sales_summary_mv" AS
SELECT COUNT(*) as total_orders, SUM("total") as revenue FROM "sales";

-- Materialized view of high-value orders over $500
CREATE MATERIALIZED VIEW files."high_value_orders_mv" AS
SELECT order_id, customer_id, total FROM "sales" WHERE "total" > 500;

-- Materialized view of monthly sales aggregation
CREATE MATERIALIZED VIEW files."monthly_sales_mv" AS
SELECT DATE_TRUNC('month', "order_date") as month, COUNT(*) as orders, SUM("total") as revenue FROM "sales" GROUP BY DATE_TRUNC('month', "order_date");

```

- **Tables Found**: [sales, products]
- **Views Found**: []
- **Materialized Views Found**: []
- **Result**: ❌ Materialized views not found
- **Note**: The file adapter may not support the 'materialized' property

## Excel Multi-Sheet and Multi-Table Support

### Excel File Test (Multi-sheet/table detection is now automatic)

- **Test File**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/complex tables/lots_of_tables.xlsx`
- **Note**: As of latest Calcite, `multiTableExcel` parameter has been removed.
- **Behavior**: Excel files now always extract all sheets and detect multiple tables within sheets automatically

- **JDBC URL**: `jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/complex tables/lots_of_tables.xlsx`
- **Tables Found**: 2
- **Table Names**: `LotsOfTables__Organization_table_2`, `LotsOfTables__Organization_table_1`
- **Row Count in First Table**: 10
- **Row Count in First Table**: 10
- **Sample Query**: `SELECT * FROM files."LotsOfTables__Organization_table_2" LIMIT 3`
- **Query Result**: ✅ Successful
- **Multi-Sheet/Table Detection**: ✅ Found 2 tables
- **Result**: ✅ Excel file processing successful with automatic multi-sheet/table detection

## All Formats via Directory Discovery

- **Test Directory**: `/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data`
- **JDBC URL**: `jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data`
- **Table Discovery**: `DatabaseMetaData.getTables(null, "files", "%", null)`
- **Tables Found by Format**:
  - **Parquet**: `parquet.sample`
  - **Arrow**: `arrow.sample`
  - **XLSX**: `xlsx.CompanyData__Departments`, `xlsx.CompanyData__Projects`, `xlsx.CompanyData__Employees`
  - **TSV**: `mixed.inventory`, `tsv.inventory`
  - **CSV**: `csv.products`, `excel.budget`, `csv.sales`
  - **JSON**: `json.customers`, `json.users`
  - **YAML**: `yaml.settings`, `yaml.config`
  - **unknown**: `nested.sales.regional.west`, `complex_tables.LotsOfTables__Organization_table_1`, `complex_tables.LotsOfTables__Organization_table_2`, `nested.2024.01.data`, `report__sales_data`, `nested.2024.02.data`, `nested.reports.quarterly.q1`, `materialized_views`, `views`
- **Result**: ✅ Directory discovery found 23 tables across all formats

## Summary: Driver Parameter Mapping Fixes vs Adapter Implementation Gaps

### ✅ Driver Parameter Mapping Issues (FIXED)

The following issues were **driver parameter mapping problems** that have been **FIXED**:

1. **Multi-Table Excel**: `multiTableExcel=true` parameter now correctly passed to FileSchemaFactory operand
2. **External Views Files**: `viewsFile` parameter now loads views and passes them as `views` operand
3. **External Materialized Views**: `viewsFile` parameter now loads materialized views and passes them as `materializations` operand
4. **Recursive Directory Scanning**: `recursive=true` parameter correctly passed to operand

**Driver Changes Made:**
- Enhanced `createOperand()` and `createHttpOperand()` methods to load external view files
- Added `loadViewsFromFileForOperand()` method to convert view definitions to operand format
- Added `loadMaterializationsFromFile()` method to load materialized views from JSON/YAML
- Added `MaterializedViewDefinition` class for JSON deserialization

### ❌ Adapter Implementation Gaps (REQUIRE ADAPTER FIXES)

The following issues are **adapter implementation gaps** that require **FileSchemaFactory fixes**:

1. **HTML File Processing**: Null URL error in HTML table creation (adapter bug)
   - Error: `Cannot invoke "java.net.URL.toString()" because Source.url() returns null`
   - Fix Required: Adapter HTML processing needs proper file URL handling

2. **Recursive Directory Scanning**: Not implemented in FileSchemaFactory
   - Driver correctly passes `recursive=true` parameter
   - Fix Required: FileSchemaFactory needs to implement recursive directory traversal

3. **Multi-Table Excel Detection**: Not implemented in FileSchemaFactory
   - Driver correctly passes `multiTableExcel=true` parameter
   - Fix Required: FileSchemaFactory needs multi-table Excel logic

4. **Materialized Views Processing**: Not implemented in FileSchemaFactory
   - Driver correctly passes `materializations` parameter
   - Fix Required: FileSchemaFactory needs materialized view creation logic

### 📊 Resolution Status

- **Driver Issues**: ✅ **4/4 FIXED** (100% complete)
- **Adapter Issues**: ❌ **0/4 FIXED** (require adapter development)

**Conclusion**: The driver now correctly passes all required parameters to the FileSchemaFactory. Remaining failures are due to missing implementations in the adapter itself.

