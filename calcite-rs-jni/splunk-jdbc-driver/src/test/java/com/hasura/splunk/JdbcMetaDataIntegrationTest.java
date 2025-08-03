package com.hasura.splunk;

import org.junit.Before;
import org.junit.Test;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.*;
import java.io.InputStream;

/**
 * Integration tests for JDBC DatabaseMetaData functionality.
 * Tests the standard JDBC metadata discovery APIs.
 */
public class JdbcMetaDataIntegrationTest {
    
    private static final String TEST_MODEL_JSON = "/test-splunk-model.json";
    private Connection connection;
    private DatabaseMetaData metaData;
    private static Properties localProperties;
    
    @BeforeClass
    public static void setUpClass() {
        try {
            Class.forName("com.hasura.splunk.SplunkDriver");
        } catch (ClassNotFoundException e) {
            fail("SplunkDriver not found: " + e.getMessage());
        }
        
        // Load local properties
        localProperties = new Properties();
        try (java.io.FileInputStream fis = new java.io.FileInputStream("local-properties.settings")) {
            localProperties.load(fis);
        } catch (java.io.IOException e) {
            System.out.println("Could not load local-properties.settings: " + e.getMessage());
            localProperties = new Properties();
        }
    }
    
    @Before
    public void setUp() throws Exception {
        connection = createTestConnection();
        metaData = connection.getMetaData();
    }
    
    private Connection createTestConnection() throws Exception {
        // Use configuration from local-properties.settings if available
        String splunkUrl = localProperties.getProperty("splunk.url", "https://kentest.xyz:8089");
        java.net.URI uri = new java.net.URI(splunkUrl);
        String jdbcUrl = String.format("jdbc:splunk://%s:%d/main", uri.getHost(), uri.getPort());
        
        Properties props = new Properties();
        props.setProperty("user", localProperties.getProperty("splunk.username", "admin"));
        props.setProperty("password", localProperties.getProperty("splunk.password", ""));
        props.setProperty("ssl", "true");
        props.setProperty("disableSslValidation", "true");
        props.setProperty("schema", "splunk");
        props.setProperty("cimModels", "web,authentication,network_traffic,events");
        
        return DriverManager.getConnection(jdbcUrl, props);
    }
    
    @Test
    public void testDatabaseInfo() throws SQLException {
        System.out.println("\n=== Testing Database Info ===");
        
        String databaseProductName = metaData.getDatabaseProductName();
        String databaseProductVersion = metaData.getDatabaseProductVersion();
        String driverName = metaData.getDriverName();
        String driverVersion = metaData.getDriverVersion();
        String url = metaData.getURL();
        String userName = metaData.getUserName();
        
        System.out.println("Database Product: " + databaseProductName);
        System.out.println("Database Version: " + databaseProductVersion);
        System.out.println("Driver Name: " + driverName);
        System.out.println("Driver Version: " + driverVersion);
        System.out.println("URL: " + url);
        System.out.println("User: " + userName);
        
        assertNotNull("Database product name should not be null", databaseProductName);
        assertNotNull("Database product version should not be null", databaseProductVersion);
        assertNotNull("Driver name should not be null", driverName);
        assertNotNull("Driver version should not be null", driverVersion);
        assertNotNull("URL should not be null", url);
        
        // Validate it's Apache Calcite
        assertTrue("Should be Apache Calcite", databaseProductName.contains("Calcite"));
    }
    
    @Test
    public void testSchemaDiscovery() throws SQLException {
        System.out.println("\n=== Testing Schema Discovery ===");
        
        try (ResultSet schemas = metaData.getSchemas()) {
            System.out.println("Available schemas:");
            int count = 0;
            Set<String> foundSchemas = new HashSet<>();
            
            while (schemas.next()) {
                String schemaName = schemas.getString("TABLE_SCHEM");
                String catalogName = schemas.getString("TABLE_CATALOG");
                
                foundSchemas.add(schemaName);
                count++;
                
                System.out.printf("  %-20s | Catalog: %s%n", schemaName, catalogName);
                assertNotNull("Schema name should not be null", schemaName);
            }
            
            System.out.println("Total schemas: " + count);
            assertTrue("Should find at least 1 schema", count >= 1);
            assertTrue("Should find 'splunk' schema", foundSchemas.contains("splunk"));
        }
    }
    
    @Test
    public void testTableDiscovery() throws SQLException {
        System.out.println("\n=== Testing Table Discovery ===");
        
        // Get all tables
        try (ResultSet tables = metaData.getTables(null, "splunk", "%", new String[]{"TABLE"})) {
            System.out.println("Tables in splunk schema:");
            int count = 0;
            Set<String> foundTables = new HashSet<>();
            
            while (tables.next()) {
                String catalogName = tables.getString("TABLE_CAT");
                String schemaName = tables.getString("TABLE_SCHEM");
                String tableName = tables.getString("TABLE_NAME");
                String tableType = tables.getString("TABLE_TYPE");
                String remarks = tables.getString("REMARKS");
                
                foundTables.add(tableName);
                count++;
                
                System.out.printf("  %-20s | Type: %-10s | Schema: %-10s | Remarks: %s%n",
                    tableName, tableType, schemaName, remarks);
                
                assertEquals("Schema should be 'splunk'", "splunk", schemaName);
                assertEquals("Table type should be 'TABLE'", "TABLE", tableType);
                assertNotNull("Table name should not be null", tableName);
            }
            
            System.out.println("Total tables: " + count);
            assertTrue("Should find at least 2 tables", count >= 2);
            assertTrue("Should find 'events' table", foundTables.contains("events"));
            assertTrue("Should find 'users' table", foundTables.contains("users"));
        }
    }
    
    @Test
    public void testColumnDiscovery() throws SQLException {
        System.out.println("\n=== Testing Column Discovery ===");
        
        // Get columns for events table
        try (ResultSet columns = metaData.getColumns(null, "splunk", "events", "%")) {
            System.out.println("Columns in 'events' table:");
            int count = 0;
            Set<String> foundColumns = new HashSet<>();
            
            while (columns.next()) {
                String catalogName = columns.getString("TABLE_CAT");
                String schemaName = columns.getString("TABLE_SCHEM");
                String tableName = columns.getString("TABLE_NAME");
                String columnName = columns.getString("COLUMN_NAME");
                int dataType = columns.getInt("DATA_TYPE");
                String typeName = columns.getString("TYPE_NAME");
                int columnSize = columns.getInt("COLUMN_SIZE");
                int nullable = columns.getInt("NULLABLE");
                String remarks = columns.getString("REMARKS");
                int ordinalPosition = columns.getInt("ORDINAL_POSITION");
                
                foundColumns.add(columnName);
                count++;
                
                String nullableStr = (nullable == DatabaseMetaData.columnNullable) ? "YES" :
                                   (nullable == DatabaseMetaData.columnNoNulls) ? "NO" : "UNKNOWN";
                
                System.out.printf("  [%d] %-15s | %-12s | Size: %4d | Nullable: %s%n",
                    ordinalPosition, columnName, typeName, columnSize, nullableStr);
                
                assertEquals("Schema should be 'splunk'", "splunk", schemaName);
                assertEquals("Table should be 'events'", "events", tableName);
                assertNotNull("Column name should not be null", columnName);
                assertTrue("Ordinal position should be positive", ordinalPosition > 0);
            }
            
            System.out.println("Total columns: " + count);
            assertTrue("Should find events columns", count > 0);
            assertTrue("Should find '_time' column", foundColumns.contains("_time"));
            assertTrue("Should find 'host' column", foundColumns.contains("host"));
        }
    }
    
    @Test
    public void testColumnDiscoveryAllTables() throws SQLException {
        System.out.println("\n=== Testing Column Discovery for All Tables ===");
        
        // Get all columns for all tables in splunk schema
        try (ResultSet columns = metaData.getColumns(null, "splunk", "%", "%")) {
            System.out.println("All columns in splunk schema:");
            int count = 0;
            String currentTable = "";
            Map<String, Set<String>> tableColumns = new HashMap<>();
            
            while (columns.next()) {
                String tableName = columns.getString("TABLE_NAME");
                String columnName = columns.getString("COLUMN_NAME");
                String typeName = columns.getString("TYPE_NAME");
                int ordinalPosition = columns.getInt("ORDINAL_POSITION");
                
                tableColumns.computeIfAbsent(tableName, k -> new HashSet<>()).add(columnName);
                
                if (!tableName.equals(currentTable)) {
                    System.out.println("\n" + tableName + " columns:");
                    currentTable = tableName;
                }
                
                System.out.printf("  [%d] %-20s | %s%n", ordinalPosition, columnName, typeName);
                count++;
            }
            
            System.out.println("\nTotal columns across all tables: " + count);
            System.out.println("Tables found: " + tableColumns.keySet());
            
            assertTrue("Should find columns", count > 0);
            assertTrue("Should find at least 2 tables", tableColumns.size() >= 2);
        }
    }
    
    @Test
    public void testPrimaryKeyDiscovery() throws SQLException {
        System.out.println("\n=== Testing Primary Key Discovery ===");
        
        // Check for primary keys in events table
        try (ResultSet primaryKeys = metaData.getPrimaryKeys(null, "splunk", "events")) {
            System.out.println("Primary keys in 'events' table:");
            int count = 0;
            
            while (primaryKeys.next()) {
                String catalogName = primaryKeys.getString("TABLE_CAT");
                String schemaName = primaryKeys.getString("TABLE_SCHEM");
                String tableName = primaryKeys.getString("TABLE_NAME");
                String columnName = primaryKeys.getString("COLUMN_NAME");
                short keySeq = primaryKeys.getShort("KEY_SEQ");
                String pkName = primaryKeys.getString("PK_NAME");
                
                count++;
                System.out.printf("  [%d] Column: %-15s | PK Name: %s%n", keySeq, columnName, pkName);
                
                assertEquals("Schema should be 'splunk'", "splunk", schemaName);
                assertEquals("Table should be 'events'", "events", tableName);
            }
            
            System.out.println("Total primary key columns: " + count);
            // Primary keys are optional in this test setup
        }
    }
    
    @Test
    public void testForeignKeyDiscovery() throws SQLException {
        System.out.println("\n=== Testing Foreign Key Discovery ===");
        
        // Check for foreign keys from events table
        try (ResultSet foreignKeys = metaData.getImportedKeys(null, "splunk", "events")) {
            System.out.println("Foreign keys in 'events' table:");
            int count = 0;
            
            while (foreignKeys.next()) {
                String pkTableName = foreignKeys.getString("PKTABLE_NAME");
                String pkColumnName = foreignKeys.getString("PKCOLUMN_NAME");
                String fkTableName = foreignKeys.getString("FKTABLE_NAME");
                String fkColumnName = foreignKeys.getString("FKCOLUMN_NAME");
                String fkName = foreignKeys.getString("FK_NAME");
                
                count++;
                System.out.printf("  %s.%s -> %s.%s | FK Name: %s%n",
                    pkTableName, pkColumnName, fkTableName, fkColumnName, fkName);
            }
            
            System.out.println("Total foreign key relationships: " + count);
            // Foreign keys are optional in this test setup
        }
    }
    
    @Test
    public void testIndexDiscovery() throws SQLException {
        System.out.println("\n=== Testing Index Discovery ===");
        
        // Check for indexes on events table
        try (ResultSet indexes = metaData.getIndexInfo(null, "splunk", "events", false, false)) {
            System.out.println("Indexes on 'events' table:");
            int count = 0;
            
            while (indexes.next()) {
                String catalogName = indexes.getString("TABLE_CAT");
                String schemaName = indexes.getString("TABLE_SCHEM");
                String tableName = indexes.getString("TABLE_NAME");
                boolean nonUnique = indexes.getBoolean("NON_UNIQUE");
                String indexName = indexes.getString("INDEX_NAME");
                String columnName = indexes.getString("COLUMN_NAME");
                short ordinalPosition = indexes.getShort("ORDINAL_POSITION");
                
                count++;
                System.out.printf("  [%d] Index: %-15s | Column: %-15s | Unique: %s%n",
                    ordinalPosition, indexName, columnName, !nonUnique);
            }
            
            System.out.println("Total index entries: " + count);
            // Indexes are optional in this test setup
        }
    }
    
    @Test
    public void testSqlKeywords() throws SQLException {
        System.out.println("\n=== Testing SQL Keywords ===");
        
        String sqlKeywords = metaData.getSQLKeywords();
        System.out.println("SQL Keywords: " + sqlKeywords);
        
        assertNotNull("SQL keywords should not be null", sqlKeywords);
        
        // Test some common keywords
        String[] commonKeywords = {"SELECT", "FROM", "WHERE", "JOIN", "GROUP", "HAVING", "ORDER"};
        for (String keyword : commonKeywords) {
            boolean isKeyword = metaData.getSQLKeywords().contains(keyword) || 
                              isSQL92Keyword(keyword); // Built-in SQL92 keywords
            System.out.println("  " + keyword + ": " + (isKeyword ? "KEYWORD" : "identifier"));
        }
    }
    
    private boolean isSQL92Keyword(String word) {
        // Common SQL92 keywords that are always reserved
        String[] sql92Keywords = {"SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", 
                                 "CREATE", "DROP", "ALTER", "TABLE", "INDEX", "JOIN", "INNER", "LEFT", "RIGHT"};
        return Arrays.asList(sql92Keywords).contains(word.toUpperCase());
    }
    
    @Test
    public void testDataTypeInfo() throws SQLException {
        System.out.println("\n=== Testing Data Type Info ===");
        
        try (ResultSet typeInfo = metaData.getTypeInfo()) {
            System.out.println("Supported data types:");
            int count = 0;
            Set<String> foundTypes = new HashSet<>();
            
            while (typeInfo.next()) {
                String typeName = typeInfo.getString("TYPE_NAME");
                int dataType = typeInfo.getInt("DATA_TYPE");
                int precision = typeInfo.getInt("PRECISION");
                String literalPrefix = typeInfo.getString("LITERAL_PREFIX");
                String literalSuffix = typeInfo.getString("LITERAL_SUFFIX");
                short nullable = typeInfo.getShort("NULLABLE");
                boolean caseSensitive = typeInfo.getBoolean("CASE_SENSITIVE");
                
                foundTypes.add(typeName);
                count++;
                
                if (count <= 20) { // Show first 20 types
                    System.out.printf("  %-15s | JDBC Type: %3d | Precision: %6d | Nullable: %s%n",
                        typeName, dataType, precision, 
                        nullable == DatabaseMetaData.typeNullable ? "YES" : "NO");
                }
            }
            
            if (count > 20) {
                System.out.println("  ... and " + (count - 20) + " more types");
            }
            
            System.out.println("Total data types: " + count);
            assertTrue("Should find data types", count > 0);
            
            // Check for common types
            assertTrue("Should support VARCHAR", foundTypes.contains("VARCHAR"));
            assertTrue("Should support INTEGER", foundTypes.contains("INTEGER") || foundTypes.contains("INT"));
            assertTrue("Should support TIMESTAMP", foundTypes.contains("TIMESTAMP"));
        }
    }
    
    @Test
    public void testDriverCapabilities() throws SQLException {
        System.out.println("\n=== Testing Driver Capabilities ===");
        
        System.out.println("Driver Capabilities:");
        System.out.println("  Supports transactions: " + metaData.supportsTransactions());
        System.out.println("  Supports stored procedures: " + metaData.supportsStoredProcedures());
        System.out.println("  Supports multiple result sets: " + metaData.supportsMultipleResultSets());
        System.out.println("  Supports outer joins: " + metaData.supportsOuterJoins());
        System.out.println("  Supports full outer joins: " + metaData.supportsFullOuterJoins());
        System.out.println("  Supports subqueries in comparisons: " + metaData.supportsSubqueriesInComparisons());
        System.out.println("  Supports subqueries in EXISTS: " + metaData.supportsSubqueriesInExists());
        System.out.println("  Supports subqueries in IN: " + metaData.supportsSubqueriesInIns());
        System.out.println("  Supports correlated subqueries: " + metaData.supportsCorrelatedSubqueries());
        System.out.println("  Supports GROUP BY: " + metaData.supportsGroupBy());
        System.out.println("  Supports LIKE escape clause: " + metaData.supportsLikeEscapeClause());
        System.out.println("  Supports column aliasing: " + metaData.supportsColumnAliasing());
        System.out.println("  Supports table correlation names: " + metaData.supportsTableCorrelationNames());
        System.out.println("  Supports UNION: " + metaData.supportsUnion());
        System.out.println("  Supports UNION ALL: " + metaData.supportsUnionAll());
        
        // Test identifier quote string
        String identifierQuoteString = metaData.getIdentifierQuoteString();
        System.out.println("  Identifier quote string: '" + identifierQuoteString + "'");
        assertNotNull("Identifier quote string should not be null", identifierQuoteString);
        
        // Test maximum identifier length
        int maxColumnNameLength = metaData.getMaxColumnNameLength();
        int maxTableNameLength = metaData.getMaxTableNameLength();
        System.out.println("  Max column name length: " + maxColumnNameLength);
        System.out.println("  Max table name length: " + maxTableNameLength);
    }
    
    @Test
    public void testResultSetMetaData() throws SQLException {
        System.out.println("\n=== Testing ResultSet MetaData ===");
        
        String query = "SELECT _time, host, source, _raw FROM splunk.events LIMIT 1";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            
            System.out.println("Query: " + query);
            System.out.println("ResultSet metadata for " + columnCount + " columns:");
            
            for (int i = 1; i <= columnCount; i++) {
                String columnName = rsmd.getColumnName(i);
                String columnLabel = rsmd.getColumnLabel(i);
                String columnTypeName = rsmd.getColumnTypeName(i);
                int columnType = rsmd.getColumnType(i);
                int precision = rsmd.getPrecision(i);
                int scale = rsmd.getScale(i);
                int nullable = rsmd.isNullable(i);
                String schemaName = rsmd.getSchemaName(i);
                String tableName = rsmd.getTableName(i);
                
                System.out.printf("  [%d] %-15s | Type: %-12s | JDBC: %3d | Precision: %4d | Nullable: %s%n",
                    i, columnName, columnTypeName, columnType, precision,
                    nullable == ResultSetMetaData.columnNullable ? "YES" : "NO");
                
                assertNotNull("Column name should not be null", columnName);
                assertNotNull("Column type name should not be null", columnTypeName);
                assertTrue("Column type should be valid JDBC type", columnType > 0);
            }
            
            assertTrue("Should have columns", columnCount > 0);
            assertEquals("Should have 4 columns", 4, columnCount);
        }
    }
}