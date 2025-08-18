# Testing the File JDBC Driver

## Test Structure

The test suite covers all supported file formats, connection methods, and features:

```
tests/
├── data/           # Sample test data files
│   ├── csv/       # CSV test files
│   ├── json/      # JSON and JSONL files  
│   ├── parquet/   # Parquet binary files
│   ├── excel/     # Excel workbooks
│   ├── html/      # HTML tables
│   ├── arrow/     # Arrow format files
│   ├── nested/    # Nested directory structure
│   └── mixed/     # Multiple formats
├── java/          # Java test classes
└── scripts/       # Test execution scripts
```

## Running Tests

**Run all tests:**
```bash
cd tests
./scripts/run-tests.sh
```

**Run specific test class:**
```bash
mvn test -Dtest=TestComprehensiveFormats
```

**Compile and run individual test:**
```bash
javac -cp "../target/classes:../target/dependency/*" java/TestDirectoryApproach.java
java -cp ".:../target/classes:../target/dependency/*" TestDirectoryApproach  
```

## Test Categories

### Format Tests
- **CSV/TSV**: Headers, separators, encoding variations
- **JSON**: Single objects, line-delimited (JSONL), nested structures
- **Parquet**: Binary columnar format with compression
- **Excel**: Multi-sheet workbooks with JSON extraction
- **HTML**: Table extraction from web pages
- **Arrow**: Columnar format with compression codecs

### Connection Tests
- **Single files**: Direct file paths
- **Directories**: Recursive and non-recursive discovery
- **Multi-location**: Pipe-separated file lists
- **Remote URLs**: HTTP, HTTPS, S3 protocols
- **Glob patterns**: Wildcard file matching
- **Model files**: Calcite model.json configurations

### Feature Tests
- **Recursive scanning**: Nested directory structures
- **Table naming**: Auto-generated vs explicit names
- **Format detection**: Automatic format recognition
- **Parameter handling**: All connection parameters
- **Error handling**: Missing files, invalid formats

## Key Test Files

### Sample Data Files

**CSV Data** (`tests/data/csv/`):
- `sales.csv` - Sales transaction data
- `products.csv` - Product catalog

**JSON Data** (`tests/data/json/`):
- `customers.json` - Customer records
- `events.jsonl` - Line-delimited event stream

**Parquet Data** (`tests/data/parquet/`):
- `sample.parquet` - Compressed columnar data

**Excel Data** (`tests/data/xlsx/`):
- `company_data.xlsx` - Multi-sheet workbook
- Auto-generated JSON files for each sheet

**Nested Structure** (`tests/data/nested/`):
```
nested/
├── 2024/01/data.csv
├── 2024/02/data.csv  
├── reports/quarterly/q1.csv
└── sales/regional/west.csv
```

### Test Classes

**Core Functionality**:
- `TestDirectoryApproach.java` - Directory-based discovery
- `TestComprehensiveFormats.java` - All format support
- `TestNestedDirectories.java` - Recursive scanning
- `TestGlobPatterns.java` - Wildcard matching

**Specific Formats**:
- `TestCSVExample.java` - CSV variations
- `TestJSONFile.java` - JSON parsing
- `TestExcelFile.java` - Excel multi-sheet
- `TestArrow.java` - Arrow format support

**Advanced Features**:
- `TestRealBinaryFormats.java` - Binary format handling
- `TestTransports.java` - Remote URL support
- `TestTableNames.java` - Table naming logic

## Test Scenarios

### Basic Directory Test
```java
// Connect to directory with mixed file formats
String url = "jdbc:file:tests/data/mixed";
Connection conn = DriverManager.getConnection(url);
ResultSet rs = conn.getMetaData().getTables(null, null, "%", null);
// Should discover: inventory.tsv table
```

### Recursive Directory Test
```java
// Scan nested directories for all CSV files
String url = "jdbc:file:tests/data/nested?recursive=true&format=csv";
Connection conn = DriverManager.getConnection(url);
// Should find: data, q1, west tables
```

### Multi-location Test
```java
// Connect to multiple files simultaneously
String url = "jdbc:file:multi?locations=" +
    "tests/data/csv/sales.csv|" +
    "tests/data/json/customers.json|" +
    "tests/data/parquet/sample.parquet";
Connection conn = DriverManager.getConnection(url);
// Should have: sales, customers, sample tables
```

### Excel Multi-sheet Test
```java
// Excel file with multiple sheets
String url = "jdbc:file:tests/data/xlsx/company_data.xlsx";
Connection conn = DriverManager.getConnection(url);
// Should create: CompanyData__Employees, CompanyData__Departments, etc.
```

### HTML Table Test
```java
// Extract tables from HTML pages
String url = "jdbc:file:tests/data/html";
Connection conn = DriverManager.getConnection(url);
// Should discover tables from report.html
```

### Glob Pattern Test
```java
// Match files with wildcard patterns
String url = "jdbc:file:tests/data/nested/**/*.csv?globMode=multi-table";
Connection conn = DriverManager.getConnection(url);
// Should find all CSV files in nested structure
```

### Model File Test
```java
// Use Calcite model.json configuration
String url = "jdbc:file:model=examples/simple-model.json";
Connection conn = DriverManager.getConnection(url);
// Schema and tables defined by model
```

## Expected Results

### Table Discovery
The driver should automatically discover these tables from test data:

**From CSV directory**:
- `sales` (sales.csv)
- `products` (products.csv)

**From JSON directory**:
- `customers` (customers.json)
- `events` (events.jsonl)

**From Excel file**:
- `CompanyData__Employees`
- `CompanyData__Departments`
- `CompanyData__Projects`

**From nested directories** (with recursive=true):
- `data` (from 2024/01/data.csv and 2024/02/data.csv)
- `q1` (from reports/quarterly/q1.csv)
- `west` (from sales/regional/west.csv)

### Query Results
Sample queries should return expected data:

```sql
-- Sales data should have columns: id, product_id, customer_id, amount, order_date
SELECT COUNT(*) FROM sales;

-- Customer data should have columns: customer_id, name, email, segment
SELECT segment, COUNT(*) FROM customers GROUP BY segment;

-- Excel employee data should have columns: employee_id, name, department, salary
SELECT department, AVG(salary) FROM CompanyData__Employees GROUP BY department;
```

## Debugging Tests

### Enable Logging
Add to test JVM args:
```
-Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG
-Dorg.slf4j.simpleLogger.log.com.hasura.file=DEBUG
```

### Common Issues

**"Table not found"**:
- Check file paths are correct
- Verify file formats are supported
- Use `recursive=true` for nested directories
- Check glob patterns match existing files

**"Memory errors"**:
- Reduce test data size
- Set lower batch sizes: `?batchSize=1000`
- Use spillover: `?spillDirectory=/tmp`

**"Format detection failed"**:
- Check file extensions
- Use explicit format: `?format=csv`
- Verify file content is valid

**"Connection refused"**:
- Check HTTP URLs are accessible
- Verify S3 credentials for S3 URLs
- Test local file permissions

## Performance Testing

### Large File Tests
```java
// Test with large Parquet files (not memory limited)
String url = "jdbc:file:large_data.parquet";
// Should handle multi-gigabyte files efficiently
```

### Concurrent Connection Tests
```java
// Multiple simultaneous connections
for (int i = 0; i < 10; i++) {
    new Thread(() -> {
        Connection conn = DriverManager.getConnection("jdbc:file:tests/data/csv");
        // Perform queries
    }).start();
}
```

### Memory Usage Tests
```java
// Monitor memory with large datasets
String url = "jdbc:file:tests/data/parquet?memoryThreshold=1048576"; // 1MB limit
// Should spill to disk when threshold exceeded
```

## Test Data Maintenance

### Regenerating Excel JSON Files
```bash
# Excel files generate JSON extraction files automatically
# Delete JSON files to force regeneration:
rm tests/data/xlsx/*.json
# Next test run will recreate them
```

### Adding New Test Files
1. Add data files to appropriate `tests/data/` subdirectory
2. Update test classes to include new scenarios
3. Verify table discovery and query results
4. Update this documentation

### Test Data Formats
Ensure test files represent realistic data patterns:
- CSV: Various separators, encodings, header styles
- JSON: Single objects and line-delimited streams
- Parquet: Different compression and schema patterns
- Excel: Multiple sheets with different data types
- HTML: Tables with various structures

The test suite validates that the File JDBC Driver correctly handles all supported formats and connection patterns in real-world scenarios.