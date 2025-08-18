# Table and Column Name Casing Feature

The File JDBC Driver now supports configurable table and column name casing transformations. This feature allows you to control how table and column names are presented to SQL queries, regardless of how they are stored in the underlying files.

## Configuration Options

### Connection Properties

You can configure casing through connection properties:

- `table_name_casing`: Controls how table names are transformed
  - `UPPER`: Transform table names to UPPERCASE (default)
  - `LOWER`: Transform table names to lowercase
  - `UNCHANGED`: Keep table names as-is from the source

- `column_name_casing`: Controls how column names are transformed
  - `UPPER`: Transform column names to UPPERCASE
  - `LOWER`: Transform column names to lowercase
  - `UNCHANGED`: Keep column names as-is from the source (default)

### Calcite Connection Properties

The following Calcite connection properties can be configured to control SQL identifier handling:

- `caseSensitive`: Whether identifiers are case-sensitive (default: `true`)
- `quotedCasing`: How quoted identifiers are handled (default: `UNCHANGED`)
- `unquotedCasing`: How unquoted identifiers are handled (default: `TO_LOWER`)

## Usage Examples

### Example 1: Uppercase Table Names
```java
Properties props = new Properties();
props.setProperty("table_name_casing", "LOWER");
props.setProperty("column_name_casing", "LOWER");

Connection conn = DriverManager.getConnection(
    "jdbc:file:///path/to/data.csv", props);

// Table "DATA" will be accessible as "data"
// Columns are lowercase
ResultSet rs = stmt.executeQuery("SELECT * FROM files.data");
```

### Example 2: Preserve Original Casing
```java
Properties props = new Properties();
props.setProperty("table_name_casing", "UNCHANGED");
props.setProperty("column_name_casing", "UNCHANGED");

Connection conn = DriverManager.getConnection(
    "jdbc:file:///path/to/data.csv", props);

// Table and column names remain as in the source file
```

### Example 3: Configuration File
```yaml
# config.yaml
tableNameCasing: UPPER
columnNameCasing: LOWER
schemas:
  - name: files
    type: custom
    factory: org.apache.calcite.adapter.file.FileSchemaFactory
    operand:
      directory: /data
```

### Example 4: Custom Calcite Settings
```java
Properties props = new Properties();
props.setProperty("table_name_casing", "UPPER");
props.setProperty("column_name_casing", "LOWER");

// Override default Calcite settings
props.setProperty("caseSensitive", "false");
props.setProperty("unquotedCasing", "TO_UPPER");

Connection conn = DriverManager.getConnection(
    "jdbc:file:///path/to/data.csv", props);
```

## How It Works

1. The FileSchemaFactory creates tables with their original names
2. If casing transformation is needed, a CasingSchema wrapper is applied
3. The wrapper transforms table and column names according to the configured rules
4. SQL queries use the transformed names

## Compatibility Notes

- The default behavior (`table_name_casing=UPPER`, `column_name_casing=UNCHANGED`) follows SQL standard conventions
- The feature works with all supported file formats (CSV, TSV, JSON, etc.)
- Casing transformations are applied consistently across all schema types (single file, directory, glob, multi-location)
- The transformations respect Calcite's identifier quoting rules