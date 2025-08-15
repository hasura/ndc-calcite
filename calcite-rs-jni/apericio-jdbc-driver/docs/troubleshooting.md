# Troubleshooting Guide

Common issues and solutions when using the Apericio JDBC Driver.

## Connection Issues

### "No suitable driver found"
**Problem:** Driver not found in classpath
**Solutions:**
```bash
# Ensure JAR is in classpath
java -cp "apericio-jdbc-driver-1.0.0-jar-with-dependencies.jar:." YourApp

# Or add to Maven/Gradle dependencies
<dependency>
    <groupId>com.kenstott.components</groupId>
    <artifactId>apericio-jdbc-driver</artifactId>
    <version>1.0.0</version>
</dependency>
```

### "Connection refused" or timeout errors
**Problem:** Network connectivity issues
**Solutions:**
- Verify URL is correct and accessible
- Check firewall settings
- For HTTP URLs, test in browser first
- For S3, verify region and credentials

### "Access denied" errors
**Problem:** Permission or authentication issues  
**Solutions:**
```bash
# Check file system permissions
ls -la /path/to/data

# Verify AWS credentials
aws s3 ls s3://your-bucket --region us-east-1

# Test HTTP authentication
curl -H "Authorization: Bearer token" https://api.example.com/data
```

## Table Discovery Issues

### "Table 'filename' not found"
**Problem:** File not discovered or incorrect table name
**Solutions:**
1. **List available tables:**
   ```java
   DatabaseMetaData meta = conn.getMetaData();
   ResultSet tables = meta.getTables(null, null, "%", null);
   while (tables.next()) {
       System.out.println(tables.getString("TABLE_NAME"));
   }
   ```

2. **Check file location:**
   - Verify file exists in configured directory
   - Check `recursive=true` for subdirectories
   - Ensure file extension is supported

3. **Check table naming:**
   - Table names are usually lowercase
   - `sales.csv` becomes table `sales`
   - `report.xlsx` with sheets becomes `report__sheet1`, `report__sheet2`

### Files not discovered
**Problem:** Directory scanning not finding files
**Solutions:**
```
# Enable recursive scanning
jdbc:apericio:/data?recursive=true

# Use specific pattern
jdbc:apericio:/data?directoryPattern=2024/*.csv

# Check directory exists and has permissions
ls -la /path/to/data
```

## Memory and Performance Issues

### Out of memory errors
**Problem:** Dataset larger than available memory
**Solutions:**
```
# Reduce batch size
jdbc:apericio:/data?batchSize=1000

# Configure memory threshold and spillover
jdbc:apericio:/data?memoryThreshold=67108864&spillDirectory=/tmp/apericio

# Increase JVM heap
java -Xmx8g -XX:MaxDirectMemorySize=4g YourApp
```

### Slow query performance
**Problem:** Queries taking too long
**Solutions:**

1. **Enable statistics for instant COUNT(DISTINCT):**
   ```
   jdbc:apericio:/data?enableStatistics=true&statisticsCache=/var/cache/apericio
   ```

2. **Use columnar formats:**
   - Convert CSV to Parquet when possible
   - Parquet provides 10-100x performance improvement

3. **Optimize batch size:**
   ```
   # For large datasets
   jdbc:apericio:/data?batchSize=10000
   
   # For memory-constrained systems  
   jdbc:apericio:/data?batchSize=500
   ```

4. **Use materialized views:**
   ```json
   {
     "materializedViews": [{
       "view": "sales_summary",
       "table": "sales_cache", 
       "sql": "SELECT region, SUM(amount) FROM sales GROUP BY region"
     }]
   }
   ```

### Disk space issues
**Problem:** Spillover files filling disk
**Solutions:**
```
# Configure spillover to different drive
jdbc:apericio:/data?spillDirectory=/large-disk/temp

# Clean up spillover directory periodically
find /tmp/apericio -type f -mtime +1 -delete

# Reduce memory threshold to use less spillover
jdbc:apericio:/data?memoryThreshold=33554432
```

## File Format Issues

### CSV parsing errors
**Problem:** CSV files not parsed correctly
**Solutions:**
```
# Specify encoding
jdbc:apericio:/data?charset=UTF-8

# Skip header lines
jdbc:apericio:/data?skipLines=1

# Disable header detection
jdbc:apericio:/data?header=false
```

### JSON parsing errors  
**Problem:** JSON files not readable
**Solutions:**
- Verify JSON is valid using online validator
- For JSONL (line-delimited), ensure one JSON object per line
- Check character encoding matches file encoding

### Excel files not opening
**Problem:** Excel files not converted to tables
**Solutions:**
```
# Enable multi-table extraction
jdbc:apericio:/data?multiTable=true

# Check file isn't password protected
# Ensure .xlsx format (not .xls for complex sheets)

# Verify sufficient memory for large Excel files
jdbc:apericio:/data?memoryThreshold=268435456
```

### Parquet compatibility issues
**Problem:** Parquet files causing errors
**Solutions:**
- Ensure Parquet files aren't corrupted
- Check Parquet schema compatibility
- Try reading with different tools to verify file integrity

## SQL and Query Issues

### SQL syntax errors
**Problem:** Queries failing with syntax errors
**Solutions:**

1. **Check SQL dialect:**
   ```
   # Use Oracle syntax
   jdbc:apericio:/data?lex=ORACLE&unquotedCasing=TO_LOWER
   
   # Use MySQL syntax  
   jdbc:apericio:/data?lex=MYSQL
   ```

2. **Quote reserved words:**
   ```sql
   -- If table name conflicts with reserved word
   SELECT * FROM "order" WHERE amount > 100
   ```

3. **Check identifier casing:**
   ```sql
   -- With unquotedCasing=TO_LOWER
   SELECT * FROM sales  -- table name is lowercase
   
   -- With unquotedCasing=TO_UPPER  
   SELECT * FROM SALES  -- table name is uppercase
   ```

### Join failures
**Problem:** Joins between tables failing
**Solutions:**
- Verify column names exist in both tables
- Check data types are compatible
- Use table prefixes to avoid ambiguity:
  ```sql
  SELECT s.id, c.name 
  FROM sales s 
  JOIN customers c ON s.customer_id = c.id
  ```

## Storage Provider Issues

### S3 access errors
**Problem:** Cannot access S3 data
**Solutions:**
```bash
# Test AWS credentials
aws s3 ls s3://bucket-name --region us-east-1

# Check IAM permissions
aws iam get-user

# Verify in configuration
{
  "storageConfig": {
    "bucket": "your-bucket",
    "region": "us-east-1",
    "accessKey": "${AWS_ACCESS_KEY_ID}",
    "secretKey": "${AWS_SECRET_ACCESS_KEY}"
  }
}
```

### HTTP API authentication
**Problem:** API returning 401/403 errors
**Solutions:**
```json
{
  "storageConfig": {
    "authType": "bearer",
    "authToken": "${API_TOKEN}",
    "headers": {
      "Accept": "application/json",
      "User-Agent": "Apericio-JDBC/1.0"
    }
  }
}
```

### SharePoint access issues
**Problem:** Cannot access SharePoint data
**Solutions:**
- Verify app registration and permissions
- Check tenant ID, client ID, client secret
- Ensure SharePoint site is accessible
- Test authentication with Microsoft Graph Explorer

## Environment and Configuration Issues

### Environment variables not expanding
**Problem:** `${VAR_NAME}` not replaced with values
**Solutions:**
```bash
# Verify environment variable is set
echo $DATA_DIR

# Check spelling and case sensitivity
export DATA_WAREHOUSE=/path/to/data  # not data_warehouse

# Test in simple connection first
jdbc:apericio:${DATA_DIR}?recursive=true
```

### Model file errors
**Problem:** JSON configuration not loading
**Solutions:**
1. **Validate JSON syntax:**
   ```bash
   # Use online JSON validator or
   python -m json.tool config.json
   ```

2. **Check file path and permissions:**
   ```bash
   ls -la /path/to/config.json
   ```

3. **Verify model file structure:**
   ```json
   {
     "version": "1.0",
     "defaultSchema": "files",
     "schemas": [...]
   }
   ```

## Debugging Tips

### Enable debug logging
```bash
# Add to JVM arguments
-Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG
-Dorg.slf4j.simpleLogger.log.org.apache.calcite=DEBUG
```

### Check driver version
```java
DatabaseMetaData meta = conn.getMetaData();
System.out.println("Driver: " + meta.getDriverName());
System.out.println("Version: " + meta.getDriverVersion());
```

### Verify configuration
```java
// Print effective configuration
Properties info = conn.getClientInfo();
for (Object key : info.keySet()) {
    System.out.println(key + " = " + info.get(key));
}
```

### Test minimal setup
```java
// Start with simplest possible configuration
String url = "jdbc:apericio:.";  // Current directory only
Connection conn = DriverManager.getConnection(url);

// Add complexity gradually
String url2 = "jdbc:apericio:./data";
String url3 = "jdbc:apericio:./data?recursive=true";
```

## Getting Help

If none of these solutions work:

1. **Check logs** for detailed error messages
2. **Create minimal reproduction case** with sample data
3. **Report issues** at [GitHub Issues](https://github.com/hasura/ndc-calcite/issues)
4. **Include in bug reports:**
   - Driver version
   - Java version  
   - Connection URL (sanitized)
   - Full error message and stack trace
   - Sample data if possible

## Performance Monitoring

### Monitor spillover usage
```bash
# Check spillover directory size
du -sh /tmp/apericio/*

# Monitor during queries
watch "du -sh /tmp/apericio"
```

### Monitor memory usage
```bash  
# JVM memory monitoring
jstat -gc -t $JAVA_PID 1s

# System memory
watch "free -h"
```

### Query performance profiling
```java
long start = System.currentTimeMillis();
ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM large_table");
long end = System.currentTimeMillis();
System.out.println("Query took: " + (end - start) + "ms");
```