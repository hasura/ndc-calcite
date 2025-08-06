package com.hasura.splunk;

import org.junit.Before;
import org.junit.Test;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.*;
import java.io.InputStream;

/**
 * Integration tests for PostgreSQL catalog functionality (pg_catalog schema).
 * Tests the compatibility with PostgreSQL's system catalogs like pg_tables, pg_columns, etc.
 */
public class PostgreSQLCatalogIntegrationTest {
    
    private static final String TEST_MODEL_JSON = "/test-splunk-model.json";
    private Connection connection;
    private static Properties localProperties;
    
    @BeforeClass
    public static void setUpClass() {
        // Ensure driver is registered
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
        props.setProperty("earliest", "-24h");
        props.setProperty("latest", "now");
        
        return DriverManager.getConnection(jdbcUrl, props);
    }
    
    @Test
    public void testPgTablesQuery() throws SQLException {
        System.out.println("\n=== Testing pg_catalog.pg_tables ===");
        
        String query = "SELECT schemaname, tablename, tableowner, hasindexes, hasrules, hastriggers " +
                      "FROM pg_catalog.pg_tables " +
                      "WHERE schemaname = 'splunk' " +
                      "ORDER BY tablename";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("Tables found in splunk schema:");
            int count = 0;
            Set<String> expectedTables = new HashSet<>(Arrays.asList("events", "authentication", "web", "network_traffic"));
            Set<String> foundTables = new HashSet<>();
            
            while (rs.next()) {
                String schemaName = rs.getString("schemaname");
                String tableName = rs.getString("tablename");
                String tableOwner = rs.getString("tableowner");
                
                foundTables.add(tableName);
                count++;
                
                System.out.printf("  %-20s | Schema: %-10s | Owner: %s%n", 
                    tableName, schemaName, tableOwner);
                
                // Validate schema name
                assertEquals("Schema should be 'splunk'", "splunk", schemaName);
                assertNotNull("Table name should not be null", tableName);
            }
            
            System.out.println("Total tables found: " + count);
            
            // Verify we found at least some expected tables
            assertTrue("Should find at least 2 tables", count >= 2);
            assertTrue("Should find 'events' table", foundTables.contains("events"));
            assertTrue("Should find 'web' table", foundTables.contains("web"));
        }
    }
    
    @Test
    public void testPgColumnsQuery() throws SQLException {
        System.out.println("\n=== Testing pg_catalog.pg_columns (via information_schema.columns) ===");
        
        // PostgreSQL pg_columns is complex, so we use information_schema.columns which is more standard
        String query = "SELECT table_name, column_name, data_type, is_nullable, column_default " +
                      "FROM information_schema.columns " +
                      "WHERE table_schema = 'splunk' " +
                      "ORDER BY table_name, ordinal_position";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("Columns found:");
            int count = 0;
            String currentTable = "";
            Map<String, Set<String>> tableColumns = new HashMap<>();
            
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");
                String isNullable = rs.getString("is_nullable");
                
                tableColumns.computeIfAbsent(tableName, k -> new HashSet<>()).add(columnName);
                
                if (!tableName.equals(currentTable)) {
                    System.out.println("\nTable: " + tableName);
                    currentTable = tableName;
                }
                
                System.out.printf("  %-20s | %-15s | Nullable: %s%n", 
                    columnName, dataType, isNullable);
                count++;
            }
            
            System.out.println("\nTotal columns found: " + count);
            
            // Validate expected columns exist
            assertTrue("Should find columns", count > 0);
            assertTrue("Events table should exist", tableColumns.containsKey("events"));
            assertTrue("Web table should exist", tableColumns.containsKey("web"));
            
            // Check specific columns
            Set<String> eventsColumns = tableColumns.get("events");
            if (eventsColumns != null) {
                assertTrue("Events should have '_time' column", eventsColumns.contains("_time"));
                assertTrue("Events should have 'host' column", eventsColumns.contains("host"));
            }
        }
    }
    
    @Test
    public void testPgNamespaceQuery() throws SQLException {
        System.out.println("\n=== Testing pg_catalog.pg_namespace ===");
        
        String query = "SELECT nspname, nspowner " +
                      "FROM pg_catalog.pg_namespace " +
                      "WHERE nspname IN ('splunk', 'pg_catalog', 'information_schema') " +
                      "ORDER BY nspname";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("Namespaces/Schemas found:");
            int count = 0;
            Set<String> foundSchemas = new HashSet<>();
            
            while (rs.next()) {
                String schemaName = rs.getString("nspname");
                foundSchemas.add(schemaName);
                count++;
                
                System.out.printf("  Schema: %s%n", schemaName);
            }
            
            System.out.println("Total schemas found: " + count);
            
            // Validate expected schemas
            assertTrue("Should find at least 1 schema", count >= 1);
            assertTrue("Should find 'splunk' schema", foundSchemas.contains("splunk"));
        }
    }
    
    @Test
    public void testPgClassQuery() throws SQLException {
        System.out.println("\n=== Testing pg_catalog.pg_class ===");
        
        // pg_class contains information about tables, indexes, sequences, etc.
        String query = "SELECT relname, relkind, relowner " +
                      "FROM pg_catalog.pg_class " +
                      "WHERE relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'splunk') " +
                      "AND relkind = 'r' " +  // 'r' = ordinary table
                      "ORDER BY relname";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("Relations (tables) found in splunk schema:");
            int count = 0;
            
            while (rs.next()) {
                String relName = rs.getString("relname");
                String relKind = rs.getString("relkind");
                
                count++;
                System.out.printf("  %-20s | Kind: %s%n", relName, relKind);
                
                assertEquals("All relations should be tables", "r", relKind);
            }
            
            System.out.println("Total relations found: " + count);
            assertTrue("Should find at least 1 table", count >= 1);
        }
    }
    
    @Test
    public void testPgAttributeQuery() throws SQLException {
        System.out.println("\n=== Testing pg_catalog.pg_attribute ===");
        
        // pg_attribute contains information about table columns
        String query = "SELECT c.relname, a.attname, a.atttypid, a.attnotnull, a.attnum " +
                      "FROM pg_catalog.pg_class c " +
                      "JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid " +
                      "WHERE c.relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'splunk') " +
                      "AND a.attnum > 0 " +  // Exclude system columns
                      "AND NOT a.attisdropped " +
                      "ORDER BY c.relname, a.attnum";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("Attributes (columns) found:");
            int count = 0;
            String currentTable = "";
            
            while (rs.next()) {
                String tableName = rs.getString("relname");
                String columnName = rs.getString("attname");
                boolean notNull = rs.getBoolean("attnotnull");
                int columnNum = rs.getInt("attnum");
                
                if (!tableName.equals(currentTable)) {
                    System.out.println("\nTable: " + tableName);
                    currentTable = tableName;
                }
                
                System.out.printf("  [%d] %-20s | Not Null: %s%n", 
                    columnNum, columnName, notNull);
                count++;
            }
            
            System.out.println("\nTotal attributes found: " + count);
            assertTrue("Should find attributes", count > 0);
        }
    }
    
    @Test
    public void testCombinedMetadataQuery() throws SQLException {
        System.out.println("\n=== Testing Combined Metadata Query ===");
        
        // Complex query joining multiple catalog tables
        String query = "SELECT " +
                      "  n.nspname as schema_name, " +
                      "  c.relname as table_name, " +
                      "  a.attname as column_name, " +
                      "  t.typname as data_type, " +
                      "  a.attnotnull as not_null " +
                      "FROM pg_catalog.pg_namespace n " +
                      "JOIN pg_catalog.pg_class c ON n.oid = c.relnamespace " +
                      "JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid " +
                      "JOIN pg_catalog.pg_type t ON a.atttypid = t.oid " +
                      "WHERE n.nspname = 'splunk' " +
                      "AND c.relkind = 'r' " +
                      "AND a.attnum > 0 " +
                      "AND NOT a.attisdropped " +
                      "ORDER BY c.relname, a.attnum " +
                      "LIMIT 10";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("Combined metadata results:");
            int count = 0;
            
            while (rs.next()) {
                String schemaName = rs.getString("schema_name");
                String tableName = rs.getString("table_name");
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");
                boolean notNull = rs.getBoolean("not_null");
                
                count++;
                System.out.printf("  %s.%s.%s | %s | Not Null: %s%n",
                    schemaName, tableName, columnName, dataType, notNull);
                
                assertEquals("Schema should be splunk", "splunk", schemaName);
                assertNotNull("Table name should not be null", tableName);
                assertNotNull("Column name should not be null", columnName);
            }
            
            System.out.println("Total combined results: " + count);
            assertTrue("Should find combined results", count > 0);
        }
    }
    
    @Test
    public void testPostgreSQLSpecificSyntax() throws SQLException {
        System.out.println("\n=== Testing PostgreSQL-Specific Syntax ===");
        
        try (Statement stmt = connection.createStatement()) {
            
            // Test 1: Double-quoted identifiers (PostgreSQL style)
            System.out.println("Testing double-quoted identifiers...");
            try (ResultSet rs = stmt.executeQuery(
                "SELECT \"table_name\", \"table_schema\" FROM information_schema.\"tables\" WHERE \"table_schema\" = 'splunk' LIMIT 1")) {
                assertTrue("Should support double-quoted identifiers", rs.next());
                System.out.println("✓ Double-quoted identifiers work");
            }
            
            // Test 2: Cast operator (PostgreSQL ::)
            System.out.println("Testing cast operator...");
            try (ResultSet rs = stmt.executeQuery("SELECT 'test'::VARCHAR as test_cast")) {
                assertTrue("Should support :: cast operator", rs.next());
                assertEquals("Cast result should be 'test'", "test", rs.getString("test_cast"));
                System.out.println("✓ :: cast operator works");
            }
            
            // Test 3: LIMIT without ORDER BY (PostgreSQL allows this)
            System.out.println("Testing LIMIT without ORDER BY...");
            try (ResultSet rs = stmt.executeQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'splunk' LIMIT 2")) {
                assertTrue("Should support LIMIT without ORDER BY", rs.next());
                System.out.println("✓ LIMIT without ORDER BY works");
            }
            
            // Test 4: Case insensitive unquoted identifiers
            System.out.println("Testing case insensitive identifiers...");
            try (ResultSet rs = stmt.executeQuery(
                "SELECT Table_Name, TABLE_SCHEMA from Information_Schema.Tables where table_schema = 'splunk' limit 1")) {
                assertTrue("Should support case insensitive identifiers", rs.next());
                System.out.println("✓ Case insensitive identifiers work");
            }
        }
    }
}