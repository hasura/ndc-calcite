package com.hasura.splunk;

import org.junit.Test;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.calcite.adapter.splunk.SplunkSchemaFactory;
import org.apache.calcite.adapter.splunk.SplunkSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.impl.AbstractSchema;

/**
 * Test to verify the JDBC driver correctly passes through configuration to the Splunk adapter
 */
public class SplunkAdapterPassthroughTest {
    
    private static Properties localProperties;
    
    @BeforeClass
    public static void loadProperties() {
        localProperties = new Properties();
        try (FileInputStream fis = new FileInputStream("local-properties.settings")) {
            localProperties.load(fis);
        } catch (IOException e) {
            System.out.println("Could not load local-properties.settings: " + e.getMessage());
        }
    }
    
    @Test
    public void testDirectSplunkSchemaFactory() throws Exception {
        System.out.println("\n=== Testing Direct SplunkSchemaFactory ===");
        
        // Build the exact same operand map that the driver should create
        Map<String, Object> operand = new HashMap<>();
        
        String splunkUrl = localProperties.getProperty("splunk.url", "https://kentest.xyz:8089");
        URI uri = new URI(splunkUrl);
        
        operand.put("host", uri.getHost());
        operand.put("port", uri.getPort());
        operand.put("username", localProperties.getProperty("splunk.username", "admin"));
        operand.put("password", localProperties.getProperty("splunk.password", ""));
        operand.put("index", "*");
        operand.put("earliest", "-24h");
        operand.put("latest", "now");
        operand.put("scheme", "https");
        operand.put("disableSslValidation", true);
        operand.put("connectTimeout", 30000);
        operand.put("socketTimeout", 60000);
        
        System.out.println("Operand map: " + operand);
        
        try {
            // Create a dummy root schema for testing
            org.apache.calcite.jdbc.Driver driver = new org.apache.calcite.jdbc.Driver();
            Properties props = new Properties();
            Connection calciteConn = driver.connect("jdbc:calcite:", props);
            CalciteConnection calciteConnection = calciteConn.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            
            // Create the schema factory directly
            SplunkSchemaFactory factory = new SplunkSchemaFactory();
            Schema schema = factory.create(rootSchema, "splunk", operand);
            
            assertNotNull("Schema should not be null", schema);
            System.out.println("✅ Successfully created SplunkSchema: " + schema.getClass().getName());
            
            // Verify it's actually a SplunkSchema
            assertTrue("Should be a SplunkSchema", schema instanceof SplunkSchema);
            
            // Try to get table names
            System.out.println("Table names: " + schema.getTableNames());
            
            calciteConn.close();
            
        } catch (Exception e) {
            System.out.println("❌ Failed to create SplunkSchema: " + e.getMessage());
            e.printStackTrace();
            fail("Direct SplunkSchemaFactory should work if adapter works");
        }
    }
    
    @Test
    public void testJdbcDriverOperandPassthrough() throws Exception {
        System.out.println("\n=== Testing JDBC Driver Operand Passthrough ===");
        
        Class.forName("com.hasura.splunk.SplunkDriver");
        
        String splunkUrl = localProperties.getProperty("splunk.url", "https://kentest.xyz:8089");
        URI uri = new URI(splunkUrl);
        String jdbcUrl = String.format("jdbc:splunk://%s:%d/main", uri.getHost(), uri.getPort());
        
        Properties props = new Properties();
        props.setProperty("user", localProperties.getProperty("splunk.username", "admin"));
        props.setProperty("password", localProperties.getProperty("splunk.password", ""));
        props.setProperty("ssl", "true");
        props.setProperty("disableSslValidation", "true");
        props.setProperty("cimModels", "web,authentication,network_traffic,events");
        
        System.out.println("JDBC URL: " + jdbcUrl);
        System.out.println("Properties: " + props);
        
        try {
            Connection conn = DriverManager.getConnection(jdbcUrl, props);
            assertNotNull("Connection should not be null", conn);
            
            // Verify it's a CalciteConnection
            assertTrue("Should be able to unwrap CalciteConnection", 
                      conn.isWrapperFor(CalciteConnection.class));
            
            CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConn.getRootSchema();
            
            // Check if splunk schema was added
            Schema splunkSchema = rootSchema.getSubSchema("splunk");
            assertNotNull("Splunk schema should exist", splunkSchema);
            System.out.println("✅ Splunk schema found: " + splunkSchema.getClass().getName());
            
            // Check what type of schema it actually is
            System.out.println("Actual schema type: " + splunkSchema.getClass().getName());
            System.out.println("Schema table names: " + splunkSchema.getTableNames());
            
            // Don't fail immediately - let's try to query tables anyway
            if (splunkSchema.getTableNames().isEmpty()) {
                System.out.println("⚠️ Schema reports no tables - this might be due to lazy loading");
                System.out.println("   Will try to query tables directly...");
            }
            
            // Try a simple query
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1");
                assertTrue("Should have result", rs.next());
                assertEquals("Should return 1", 1, rs.getInt(1));
                System.out.println("✅ Basic query works");
            }
            
            // Try to query Splunk data - use pg_tables to find the actual table names
            System.out.println("\n--- Testing Splunk Data Query ---");
            
            // First, let's list all available tables using pg_catalog.pg_tables
            System.out.println("\nQuerying pg_catalog.pg_tables to find all tables:");
            try (Statement stmt = conn.createStatement()) {
                String query = "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname = 'splunk'";
                System.out.println("Query: " + query);
                ResultSet rs = stmt.executeQuery(query);
                
                List<String> foundTables = new ArrayList<>();
                while (rs.next()) {
                    String schema = rs.getString("schemaname");
                    String table = rs.getString("tablename");
                    System.out.println("  Found table: " + schema + "." + table);
                    foundTables.add(table);
                }
                
                if (foundTables.isEmpty()) {
                    System.out.println("  No tables found in pg_tables");
                    
                    // Try information_schema as well
                    System.out.println("\nTrying information_schema.tables:");
                    try (Statement stmt2 = conn.createStatement()) {
                        String query2 = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'splunk'";
                        ResultSet rs2 = stmt2.executeQuery(query2);
                        while (rs2.next()) {
                            String schema = rs2.getString("table_schema");
                            String table = rs2.getString("table_name");
                            System.out.println("  Found table: " + schema + "." + table);
                            foundTables.add(table);
                        }
                    } catch (SQLException e2) {
                        System.out.println("  Could not query information_schema: " + e2.getMessage());
                    }
                }
                
                // Try to query any tables we found
                for (String tableName : foundTables) {
                    try (Statement stmt3 = conn.createStatement()) {
                        String countQuery = "SELECT COUNT(*) as cnt FROM splunk." + tableName;
                        ResultSet rs3 = stmt3.executeQuery(countQuery);
                        if (rs3.next()) {
                            System.out.println("  Table " + tableName + " has " + rs3.getInt("cnt") + " rows");
                        }
                    } catch (SQLException e) {
                        System.out.println("  Could not count rows in " + tableName + ": " + e.getMessage());
                    }
                }
                
            } catch (SQLException e) {
                System.out.println("Could not query pg_tables: " + e.getMessage());
                
                // Fall back to DatabaseMetaData
                System.out.println("\nFalling back to DatabaseMetaData:");
                try {
                    DatabaseMetaData dbMeta = conn.getMetaData();
                    try (ResultSet tables = dbMeta.getTables(null, "splunk", "%", new String[]{"TABLE"})) {
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");
                            System.out.println("  Found table: " + tableName);
                        }
                    }
                } catch (SQLException e2) {
                    System.out.println("  Could not list tables via metadata: " + e2.getMessage());
                }
            }
            
            // Try to query the web events table
            String[] tablesToTry = {"web_events", "webevents", "web", "events"};
            for (String tableName : tablesToTry) {
                try (Statement stmt = conn.createStatement()) {
                    String query = "SELECT COUNT(*) as event_count FROM splunk." + tableName;
                    System.out.println("\nTrying query: " + query);
                    ResultSet rs = stmt.executeQuery(query);
                    if (rs.next()) {
                        int count = rs.getInt("event_count");
                        System.out.println("✅ Found " + count + " events in " + tableName + " table");
                        
                        // Get some sample data
                        try (Statement stmt2 = conn.createStatement()) {
                            String sampleQuery = "SELECT * FROM splunk." + tableName + " LIMIT 5";
                            ResultSet rs2 = stmt2.executeQuery(sampleQuery);
                            ResultSetMetaData rsMeta = rs2.getMetaData();
                            int columnCount = rsMeta.getColumnCount();
                            
                            System.out.println("\nSample data from " + tableName + ":");
                            System.out.println("Columns: ");
                            for (int i = 1; i <= columnCount; i++) {
                                System.out.println("  - " + rsMeta.getColumnName(i) + " (" + rsMeta.getColumnTypeName(i) + ")");
                            }
                            
                            int rowNum = 0;
                            while (rs2.next() && rowNum < 5) {
                                System.out.println("\nRow " + (++rowNum) + ":");
                                for (int i = 1; i <= Math.min(columnCount, 5); i++) {
                                    System.out.println("  " + rsMeta.getColumnName(i) + ": " + rs2.getString(i));
                                }
                            }
                        }
                        break; // Found the table, stop trying
                    }
                } catch (SQLException e) {
                    System.out.println("  Could not query " + tableName + ": " + e.getMessage());
                }
            }
            
            conn.close();
            
        } catch (SQLException e) {
            System.out.println("❌ Connection failed: " + e.getMessage());
            e.printStackTrace();
            
            // Let's check if it's just the connection or something else
            if (e.getMessage().contains("Failed to create Calcite connection")) {
                System.out.println("\nDebugging: Trying minimal Calcite connection...");
                testMinimalCalciteConnection();
            }
        }
    }
    
    @Test
    public void testSplunkSchemaTableAccess() throws Exception {
        System.out.println("\n=== Testing Splunk Schema Table Access ===");
        
        Class.forName("com.hasura.splunk.SplunkDriver");
        
        String splunkUrl = localProperties.getProperty("splunk.url", "https://kentest.xyz:8089");
        URI uri = new URI(splunkUrl);
        String jdbcUrl = String.format("jdbc:splunk://%s:%d/main", uri.getHost(), uri.getPort());
        
        Properties props = new Properties();
        props.setProperty("user", localProperties.getProperty("splunk.username", "admin"));
        props.setProperty("password", localProperties.getProperty("splunk.password", ""));
        props.setProperty("ssl", "true");
        props.setProperty("disableSslValidation", "true");
        props.setProperty("cimModels", "web,authentication,network_traffic,events");
        
        try {
            Connection conn = DriverManager.getConnection(jdbcUrl, props);
            DatabaseMetaData metaData = conn.getMetaData();
            
            // List schemas
            System.out.println("\nSchemas:");
            try (ResultSet schemas = metaData.getSchemas()) {
                while (schemas.next()) {
                    String schemaName = schemas.getString("TABLE_SCHEM");
                    System.out.println("  - " + schemaName);
                }
            }
            
            // List tables in splunk schema
            System.out.println("\nTables in 'splunk' schema:");
            try (ResultSet tables = metaData.getTables(null, "splunk", "%", null)) {
                int count = 0;
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    String tableType = tables.getString("TABLE_TYPE");
                    System.out.println("  - " + tableName + " (" + tableType + ")");
                    count++;
                }
                System.out.println("Total tables: " + count);
            }
            
            conn.close();
            
        } catch (SQLException e) {
            System.out.println("❌ Failed to access schema tables: " + e.getMessage());
        }
    }
    
    private void testMinimalCalciteConnection() {
        try {
            // Test 1: Completely empty properties
            Properties emptyProps = new Properties();
            Connection conn1 = DriverManager.getConnection("jdbc:calcite:", emptyProps);
            System.out.println("✅ Empty properties connection works");
            conn1.close();
            
            // Test 2: With our properties one by one
            Properties testProps = new Properties();
            
            // Add properties one by one to find which one breaks it
            String[] propNames = {"lex", "parserFactory", "caseSensitive", "quotedCasing", 
                                 "unquotedCasing", "conformance", "fun"};
            
            for (String propName : propNames) {
                if (propName.equals("lex")) testProps.setProperty(propName, "MYSQL");
                else if (propName.equals("parserFactory")) 
                    testProps.setProperty(propName, "org.apache.calcite.sql.parser.babel.SqlBabelParserImpl#FACTORY");
                else if (propName.equals("caseSensitive")) testProps.setProperty(propName, "false");
                else if (propName.equals("quotedCasing")) testProps.setProperty(propName, "UNCHANGED");
                else if (propName.equals("unquotedCasing")) testProps.setProperty(propName, "TO_LOWER");
                else if (propName.equals("conformance")) testProps.setProperty(propName, "LENIENT");
                else if (propName.equals("fun")) testProps.setProperty(propName, "postgresql");
                
                try {
                    Connection testConn = DriverManager.getConnection("jdbc:calcite:", testProps);
                    System.out.println("✅ Connection works with " + propName + " = " + testProps.getProperty(propName));
                    testConn.close();
                } catch (SQLException e) {
                    System.out.println("❌ Connection fails when adding " + propName + " = " + testProps.getProperty(propName));
                    System.out.println("   Error: " + e.getMessage());
                }
            }
            
        } catch (SQLException e) {
            System.out.println("❌ Even minimal connection failed: " + e.getMessage());
        }
    }
}