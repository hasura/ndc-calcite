# Quick Start Guide

Get started with the Apericio JDBC Driver in under 5 minutes.

## Installation

### Option 1: Maven Dependency
```xml
<dependency>
    <groupId>com.kenstott.components</groupId>
    <artifactId>apericio-jdbc-driver</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Option 2: Download JAR
Download the latest JAR from releases and add to your classpath:
```bash
java -cp "apericio-jdbc-driver-1.0.0-jar-with-dependencies.jar:." YourApp
```

## First Connection

### Step 1: Prepare Sample Data
Create a directory with some data files:
```bash
mkdir sample_data
echo "id,name,amount,date" > sample_data/sales.csv
echo "1,Alice,1200.00,2024-01-15" >> sample_data/sales.csv  
echo "2,Bob,850.50,2024-01-16" >> sample_data/sales.csv

echo '{"customers":[{"id":1,"name":"Alice","city":"NYC"},{"id":2,"name":"Bob","city":"LA"}]}' > sample_data/customers.json
```

### Step 2: Connect and Query
```java
import java.sql.*;

public class ApericioQuickStart {
    public static void main(String[] args) throws SQLException {
        // Connect to directory - all files become queryable tables
        String url = "jdbc:apericio:sample_data";
        Connection conn = DriverManager.getConnection(url);
        
        // List available tables
        System.out.println("Available tables:");
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet tables = meta.getTables(null, null, "%", null);
        while (tables.next()) {
            System.out.println("  " + tables.getString("TABLE_NAME"));
        }
        
        // Query the CSV data
        System.out.println("\nSales data:");
        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT name, amount FROM sales WHERE amount > 1000"
        );
        while (rs.next()) {
            System.out.println("  " + rs.getString("name") + ": $" + rs.getBigDecimal("amount"));
        }
        
        // Query JSON data
        System.out.println("\nCustomers from JSON:");
        rs = conn.createStatement().executeQuery(
            "SELECT id, name, city FROM customers"
        );
        while (rs.next()) {
            System.out.println("  " + rs.getInt("id") + ": " + rs.getString("name") + " (" + rs.getString("city") + ")");
        }
        
        conn.close();
    }
}
```

**Expected Output:**
```
Available tables:
  sales
  customers

Sales data:
  Alice: $1200.00

Customers from JSON:
  1: Alice (NYC)
  2: Bob (LA)
```

## Common Connection Patterns

### Directory with Options
```java
// Oracle-style SQL with lowercase table names
String url = "jdbc:apericio:/data?lex=ORACLE&unquotedCasing=TO_LOWER";

// Enable multi-table Excel extraction
String url = "jdbc:apericio:/data?multiTable=true&recursive=true";

// Large dataset configuration
String url = "jdbc:apericio:/data?batchSize=10000&memoryThreshold=67108864";
```

### Properties-Based Connection
```java
Properties props = new Properties();
props.setProperty("directory", "/path/to/data");
props.setProperty("recursive", "true");
props.setProperty("lex", "ORACLE");
props.setProperty("unquotedCasing", "TO_LOWER");

Connection conn = DriverManager.getConnection("jdbc:apericio:", props);
```

### Environment Variables
```bash
export DATA_DIR=/path/to/warehouse
export TEMP_DIR=/fast-disk/temp
```

```java
String url = "jdbc:apericio:${DATA_DIR}?spillDirectory=${TEMP_DIR}";
```

## File Format Examples

### CSV Files
```java
// sales.csv automatically becomes table "sales"
ResultSet rs = stmt.executeQuery("SELECT * FROM sales WHERE amount > 100");
```

### JSON Files  
```java  
// orders.json becomes table "orders"
// Nested JSON is automatically flattened
ResultSet rs = stmt.executeQuery("SELECT customer_name, order_total FROM orders");
```

### Excel Files
```java
// report.xlsx with sheets [Summary, Details] becomes:
//   - report__summary
//   - report__details
ResultSet rs = stmt.executeQuery("SELECT * FROM report__summary");
```

### Parquet Files
```java
// data.parquet becomes table "data"
ResultSet rs = stmt.executeQuery("SELECT * FROM data WHERE year = 2024");
```

## Cross-Format Queries

```java
// Join CSV with JSON data
String sql = """
    SELECT 
        s.id,
        s.amount,
        c.name,
        c.city
    FROM sales s
    JOIN customers c ON s.id = c.id
    WHERE s.amount > 1000
    """;
    
ResultSet rs = stmt.executeQuery(sql);
```

## Next Steps

- **[Configuration Guide](configuration.md)** - Detailed connection options
- **[File Formats Guide](file-formats.md)** - All supported formats
- **[Advanced Features](../README.md#advanced-features)** - Iceberg, web crawling, materialized views

## Troubleshooting

**Table not found?**
- Check file exists in the directory
- Verify file extension is supported
- Table names are typically lowercase versions of filename

**Memory issues?**
- Add `?batchSize=1000&memoryThreshold=67108864` to URL
- Configure spillover: `?spillDirectory=/tmp/apericio`

**Performance slow?**
- Use Parquet format when possible
- Enable statistics: `?enableStatistics=true`
- Increase batch size for large datasets

See [Troubleshooting Guide](troubleshooting.md) for more solutions.