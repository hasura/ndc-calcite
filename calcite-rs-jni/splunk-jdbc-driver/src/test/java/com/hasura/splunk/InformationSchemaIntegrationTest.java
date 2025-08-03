package com.hasura.splunk;

import org.junit.Before;
import org.junit.Test;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.*;
import java.io.InputStream;

/**
 * Integration tests for information_schema functionality.
 * Tests compatibility with SQL standard information_schema views.
 */
public class InformationSchemaIntegrationTest {
    
    private static final String TEST_MODEL_JSON = "/test-splunk-model.json";
    private Connection connection;
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
    public void testInformationSchemaTables() throws SQLException {
        System.out.println("\n=== Testing information_schema.tables ===");
        
        String query = "SELECT table_catalog, table_schema, table_name, table_type " +
                      "FROM information_schema.tables " +
                      "WHERE table_schema = 'splunk' " +
                      "ORDER BY table_name";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("Tables in information_schema:");
            int count = 0;
            Set<String> foundTables = new HashSet<>();
            
            while (rs.next()) {
                String catalog = rs.getString("table_catalog");
                String schema = rs.getString("table_schema");
                String tableName = rs.getString("table_name");
                String tableType = rs.getString("table_type");
                
                foundTables.add(tableName);
                count++;
                
                System.out.printf("  %-20s | Schema: %-10s | Type: %-10s | Catalog: %s%n",
                    tableName, schema, tableType, catalog);
                
                assertEquals("Schema should be 'splunk'", "splunk", schema);
                assertEquals("Table type should be 'TABLE'", "TABLE", tableType);
                assertNotNull("Table name should not be null", tableName);
            }
            
            System.out.println("Total tables: " + count);
            assertTrue("Should find at least 2 tables", count >= 2);
            assertTrue("Should find 'events' table", foundTables.contains("events"));
            assertTrue("Should find 'web' table", foundTables.contains("web"));
        }
    }
    
    @Test
    public void testInformationSchemaColumns() throws SQLException {
        System.out.println("\n=== Testing information_schema.columns ===");
        
        String query = "SELECT table_name, column_name, ordinal_position, column_default, " +
                      "is_nullable, data_type, character_maximum_length, numeric_precision " +
                      "FROM information_schema.columns " +
                      "WHERE table_schema = 'splunk' " +
                      "ORDER BY table_name, ordinal_position";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("Columns in information_schema:");
            int count = 0;
            String currentTable = "";
            Map<String, List<String>> tableColumns = new HashMap<>();
            
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                String columnName = rs.getString("column_name");
                int ordinalPosition = rs.getInt("ordinal_position");
                String isNullable = rs.getString("is_nullable");
                String dataType = rs.getString("data_type");
                
                tableColumns.computeIfAbsent(tableName, k -> new ArrayList<>()).add(columnName);
                
                if (!tableName.equals(currentTable)) {
                    System.out.println("\n" + tableName + " columns:");
                    currentTable = tableName;
                }
                
                System.out.printf("  [%d] %-20s | %-15s | Nullable: %s%n",
                    ordinalPosition, columnName, dataType, isNullable);
                count++;
                
                assertTrue("Ordinal position should be positive", ordinalPosition > 0);
                assertNotNull("Column name should not be null", columnName);
                assertTrue("Is nullable should be YES or NO", 
                    "YES".equals(isNullable) || "NO".equals(isNullable));
            }
            
            System.out.println("\nTotal columns: " + count);
            assertTrue("Should find columns", count > 0);
            
            // Validate specific table structures
            assertTrue("Should find events table", tableColumns.containsKey("events"));
            assertTrue("Should find web table", tableColumns.containsKey("web"));
            
            List<String> eventsColumns = tableColumns.get("events");
            if (eventsColumns != null) {
                assertTrue("Events should have '_time' column", eventsColumns.contains("_time"));
                assertTrue("Events should have 'host' column", eventsColumns.contains("host"));
                assertTrue("Events should have '_raw' column", eventsColumns.contains("_raw"));
            }
        }
    }
    
    @Test
    public void testInformationSchemaSchemata() throws SQLException {
        System.out.println("\n=== Testing information_schema.schemata ===");
        
        String query = "SELECT catalog_name, schema_name, schema_owner, default_character_set_catalog " +
                      "FROM information_schema.schemata " +
                      "ORDER BY schema_name";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("Schemas in information_schema:");
            int count = 0;
            Set<String> foundSchemas = new HashSet<>();
            
            while (rs.next()) {
                String catalogName = rs.getString("catalog_name");
                String schemaName = rs.getString("schema_name");
                String schemaOwner = rs.getString("schema_owner");
                
                foundSchemas.add(schemaName);
                count++;
                
                System.out.printf("  %-20s | Catalog: %-10s | Owner: %s%n",
                    schemaName, catalogName, schemaOwner);
                
                assertNotNull("Schema name should not be null", schemaName);
            }
            
            System.out.println("Total schemas: " + count);
            assertTrue("Should find at least 1 schema", count >= 1);
            assertTrue("Should find 'splunk' schema", foundSchemas.contains("splunk"));
        }
    }
    
    @Test
    public void testInformationSchemaViews() throws SQLException {
        System.out.println("\n=== Testing information_schema.views ===");
        
        String query = "SELECT table_catalog, table_schema, table_name, view_definition, " +
                      "check_option, is_updatable " +
                      "FROM information_schema.views " +
                      "WHERE table_schema = 'splunk' " +
                      "ORDER BY table_name";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("Views in information_schema:");
            int count = 0;
            
            while (rs.next()) {
                String catalog = rs.getString("table_catalog");
                String schema = rs.getString("table_schema");
                String viewName = rs.getString("table_name");
                String isUpdatable = rs.getString("is_updatable");
                
                count++;
                System.out.printf("  %-20s | Schema: %-10s | Updatable: %s%n",
                    viewName, schema, isUpdatable);
                
                assertEquals("Schema should be 'splunk'", "splunk", schema);
            }
            
            System.out.println("Total views: " + count);
            // Views are optional, so we don't assert a minimum count
        }
    }
    
    @Test
    public void testInformationSchemaTableConstraints() throws SQLException {
        System.out.println("\n=== Testing information_schema.table_constraints ===");
        
        String query = "SELECT constraint_catalog, constraint_schema, constraint_name, " +
                      "table_catalog, table_schema, table_name, constraint_type " +
                      "FROM information_schema.table_constraints " +
                      "WHERE table_schema = 'splunk' " +
                      "ORDER BY table_name, constraint_name";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("Table constraints in information_schema:");
            int count = 0;
            
            while (rs.next()) {
                String constraintName = rs.getString("constraint_name");
                String tableName = rs.getString("table_name");
                String constraintType = rs.getString("constraint_type");
                
                count++;
                System.out.printf("  %-30s | Table: %-15s | Type: %s%n",
                    constraintName, tableName, constraintType);
                
                assertNotNull("Constraint name should not be null", constraintName);
                assertNotNull("Table name should not be null", tableName);
                assertNotNull("Constraint type should not be null", constraintType);
            }
            
            System.out.println("Total constraints: " + count);
            // Constraints are optional, so we don't assert a minimum count
        }
    }
    
    @Test
    public void testInformationSchemaKeyColumnUsage() throws SQLException {
        System.out.println("\n=== Testing information_schema.key_column_usage ===");
        
        String query = "SELECT constraint_catalog, constraint_schema, constraint_name, " +
                      "table_catalog, table_schema, table_name, column_name, ordinal_position " +
                      "FROM information_schema.key_column_usage " +
                      "WHERE table_schema = 'splunk' " +
                      "ORDER BY table_name, ordinal_position";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("Key column usage in information_schema:");
            int count = 0;
            
            while (rs.next()) {
                String constraintName = rs.getString("constraint_name");
                String tableName = rs.getString("table_name");
                String columnName = rs.getString("column_name");
                int ordinalPosition = rs.getInt("ordinal_position");
                
                count++;
                System.out.printf("  %-20s | Table: %-15s | Column: %-15s | Pos: %d%n",
                    constraintName, tableName, columnName, ordinalPosition);
            }
            
            System.out.println("Total key column usages: " + count);
        }
    }
    
    @Test
    public void testCrossSchemaJoins() throws SQLException {
        System.out.println("\n=== Testing Cross-Schema Joins ===");
        
        // Join information_schema.tables with information_schema.columns
        String query = "SELECT t.table_name, COUNT(c.column_name) as column_count, " +
                      "MIN(c.ordinal_position) as first_column_pos, " +
                      "MAX(c.ordinal_position) as last_column_pos " +
                      "FROM information_schema.tables t " +
                      "LEFT JOIN information_schema.columns c " +
                      "  ON t.table_catalog = c.table_catalog " +
                      "  AND t.table_schema = c.table_schema " +
                      "  AND t.table_name = c.table_name " +
                      "WHERE t.table_schema = 'splunk' " +
                      "GROUP BY t.table_name " +
                      "ORDER BY t.table_name";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("Cross-schema join results:");
            int count = 0;
            
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                int columnCount = rs.getInt("column_count");
                int firstPos = rs.getInt("first_column_pos");
                int lastPos = rs.getInt("last_column_pos");
                
                count++;
                System.out.printf("  %-20s | Columns: %2d | Positions: %d-%d%n",
                    tableName, columnCount, firstPos, lastPos);
                
                assertTrue("Column count should be positive", columnCount > 0);
                assertTrue("First position should be 1", firstPos == 1);
                assertTrue("Last position should be >= first", lastPos >= firstPos);
            }
            
            System.out.println("Total tables with column info: " + count);
            assertTrue("Should find table-column relationships", count > 0);
        }
    }
    
    @Test
    public void testComplexInformationSchemaQuery() throws SQLException {
        System.out.println("\n=== Testing Complex Information Schema Query ===");
        
        // Complex query with multiple joins and aggregations
        String query = "WITH table_stats AS ( " +
                      "  SELECT " +
                      "    t.table_name, " +
                      "    COUNT(c.column_name) as total_columns, " +
                      "    COUNT(CASE WHEN c.is_nullable = 'NO' THEN 1 END) as not_null_columns, " +
                      "    COUNT(CASE WHEN c.data_type LIKE '%CHAR%' THEN 1 END) as text_columns, " +
                      "    COUNT(CASE WHEN c.data_type IN ('INTEGER', 'BIGINT', 'DECIMAL', 'NUMERIC') THEN 1 END) as numeric_columns " +
                      "  FROM information_schema.tables t " +
                      "  LEFT JOIN information_schema.columns c " +
                      "    ON t.table_schema = c.table_schema AND t.table_name = c.table_name " +
                      "  WHERE t.table_schema = 'splunk' " +
                      "  GROUP BY t.table_name " +
                      ") " +
                      "SELECT " +
                      "  table_name, " +
                      "  total_columns, " +
                      "  not_null_columns, " +
                      "  text_columns, " +
                      "  numeric_columns, " +
                      "  ROUND((not_null_columns * 100.0 / NULLIF(total_columns, 0)), 1) as not_null_percentage " +
                      "FROM table_stats " +
                      "ORDER BY total_columns DESC";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("Complex table statistics:");
            System.out.println("Table                | Total | NotNull | Text | Numeric | NotNull%");
            System.out.println("---------------------|-------|---------|------|---------|--------");
            
            int count = 0;
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                int totalColumns = rs.getInt("total_columns");
                int notNullColumns = rs.getInt("not_null_columns");
                int textColumns = rs.getInt("text_columns");
                int numericColumns = rs.getInt("numeric_columns");
                double notNullPercentage = rs.getDouble("not_null_percentage");
                
                count++;
                System.out.printf("%-20s | %5d | %7d | %4d | %7d | %6.1f%%\n",
                    tableName, totalColumns, notNullColumns, textColumns, 
                    numericColumns, notNullPercentage);
                
                assertTrue("Total columns should be positive", totalColumns > 0);
                assertTrue("Not null columns should be non-negative", notNullColumns >= 0);
                assertTrue("Not null columns should not exceed total", notNullColumns <= totalColumns);
            }
            
            System.out.println("\nTotal tables analyzed: " + count);
            assertTrue("Should analyze at least 1 table", count >= 1);
        }
    }
    
    @Test
    public void testInformationSchemaWithFiltering() throws SQLException {
        System.out.println("\n=== Testing Information Schema with Filtering ===");
        
        // Test various filtering scenarios
        String[] testQueries = {
            // Filter by data type
            "SELECT table_name, column_name FROM information_schema.columns " +
            "WHERE table_schema = 'splunk' AND data_type = 'VARCHAR' ORDER BY table_name, column_name",
            
            // Filter by nullable columns
            "SELECT table_name, COUNT(*) as nullable_columns FROM information_schema.columns " +
            "WHERE table_schema = 'splunk' AND is_nullable = 'YES' " +
            "GROUP BY table_name ORDER BY table_name",
            
            // Filter by column name pattern
            "SELECT table_name, column_name FROM information_schema.columns " +
            "WHERE table_schema = 'splunk' AND column_name LIKE '%time%' " +
            "ORDER BY table_name, column_name"
        };
        
        String[] testNames = {
            "VARCHAR columns",
            "Nullable column counts",
            "Time-related columns"
        };
        
        for (int i = 0; i < testQueries.length; i++) {
            System.out.println("\n" + testNames[i] + ":");
            
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(testQueries[i])) {
                
                int count = 0;
                while (rs.next() && count < 10) { // Limit output
                    StringBuilder row = new StringBuilder("  ");
                    
                    // Dynamic column handling
                    ResultSetMetaData rsmd = rs.getMetaData();
                    for (int col = 1; col <= rsmd.getColumnCount(); col++) {
                        if (col > 1) row.append(" | ");
                        row.append(rs.getString(col));
                    }
                    
                    System.out.println(row.toString());
                    count++;
                }
                
                System.out.println("  (Showing up to " + count + " results)");
            }
        }
    }
}