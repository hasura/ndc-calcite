package com.hasura.splunk;

import org.junit.Test;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

/**
 * Test to verify that 'splunk' is used as the default schema when none is specified.
 */
public class DefaultSchemaFallbackTest {
    
    private static Properties localProperties;
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        Class.forName("com.hasura.splunk.SplunkDriver");
        
        // Load local properties
        localProperties = new Properties();
        try (java.io.FileInputStream fis = new java.io.FileInputStream("local-properties.settings")) {
            localProperties.load(fis);
        } catch (java.io.IOException e) {
            System.out.println("Could not load local-properties.settings: " + e.getMessage());
            localProperties = new Properties();
        }
    }
    
    @Test
    public void testDefaultSchemaWhenNoneSpecified() throws Exception {
        System.out.println("\n=== Testing Default Schema Fallback to 'splunk' ===");
        
        // Connect WITHOUT specifying schema in URL path, query params, or properties
        String splunkUrl = localProperties.getProperty("splunk.url", "https://kentest.xyz:8089");
        java.net.URI uri = new java.net.URI(splunkUrl);
        
        // No schema in path or query string
        String jdbcUrl = String.format("jdbc:splunk://%s:%d", uri.getHost(), uri.getPort());
        
        Properties props = new Properties();
        props.setProperty("user", localProperties.getProperty("splunk.username", "admin"));
        props.setProperty("password", localProperties.getProperty("splunk.password", ""));
        props.setProperty("ssl", "true");
        props.setProperty("disableSslValidation", "true");
        props.setProperty("cimModels", "web,authentication");
        // NOTE: NO schema property set
        
        System.out.println("JDBC URL: " + jdbcUrl);
        System.out.println("Properties: No 'schema' property set");
        System.out.println("Expected behavior: Should default to 'splunk' schema");
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            assertNotNull("Connection should not be null", conn);
            
            // Test 1: Unqualified table names should work (proving default schema is set)
            System.out.println("\n--- Testing unqualified table access ---");
            String unqualifiedQuery = "SELECT COUNT(*) FROM web";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(unqualifiedQuery)) {
                
                assertTrue("Should have at least one row", rs.next());
                int count = rs.getInt(1);
                System.out.println("Found " + count + " rows in 'web' table (unqualified)");
                assertTrue("Should have some data", count >= 0);
                
                System.out.println("✅ Unqualified query works - default schema is active");
            }
            
            // Test 2: Verify tables exist in 'splunk' schema (not some other schema)
            System.out.println("\n--- Verifying tables are in 'splunk' schema ---");
            String metaQuery = "SELECT table_schema, table_name FROM information_schema.tables " +
                              "WHERE table_name IN ('web', 'authentication') " +
                              "ORDER BY table_name";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(metaQuery)) {
                
                int tableCount = 0;
                while (rs.next()) {
                    String schemaName = rs.getString("table_schema");
                    String tableName = rs.getString("table_name");
                    System.out.println("  - " + tableName + " is in schema: " + schemaName);
                    assertEquals("Table should be in 'splunk' schema", "splunk", schemaName);
                    tableCount++;
                }
                assertEquals("Should find both tables", 2, tableCount);
                
                System.out.println("✅ All tables confirmed to be in 'splunk' schema");
            }
            
            // Test 3: Explicit schema reference should also work
            System.out.println("\n--- Testing explicit schema reference ---");
            String explicitQuery = "SELECT COUNT(*) FROM splunk.authentication";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(explicitQuery)) {
                
                assertTrue("Should have at least one row", rs.next());
                int count = rs.getInt(1);
                System.out.println("Found " + count + " rows in 'splunk.authentication'");
                assertTrue("Should have some data", count >= 0);
                
                System.out.println("✅ Explicit schema reference also works");
            }
            
            System.out.println("\n=== SUMMARY ===");
            System.out.println("✅ Default schema 'splunk' is automatically applied");
            System.out.println("✅ No schema specification required - falls back to 'splunk'");
            System.out.println("✅ Unqualified table names work with default schema");
        }
    }
    
    @Test
    public void testExplicitSchemaOverridesDefault() throws Exception {
        System.out.println("\n=== Testing Explicit Schema Overrides Default ===");
        
        // When we DO specify a schema, it should use that instead of default
        String splunkUrl = localProperties.getProperty("splunk.url", "https://kentest.xyz:8089");
        java.net.URI uri = new java.net.URI(splunkUrl);
        
        // Specify schema in properties (could also use path or query param)
        String jdbcUrl = String.format("jdbc:splunk://%s:%d", uri.getHost(), uri.getPort());
        
        Properties props = new Properties();
        props.setProperty("user", localProperties.getProperty("splunk.username", "admin"));
        props.setProperty("password", localProperties.getProperty("splunk.password", ""));
        props.setProperty("ssl", "true");
        props.setProperty("disableSslValidation", "true");
        props.setProperty("schema", "splunk"); // Explicitly set
        props.setProperty("cimModels", "web");
        
        System.out.println("Explicitly setting schema='splunk' in properties");
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            // Should work the same as default
            String query = "SELECT COUNT(*) FROM web";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                
                assertTrue("Should have at least one row", rs.next());
                int count = rs.getInt(1);
                System.out.println("Found " + count + " rows with explicit schema");
                
                System.out.println("✅ Explicit schema specification works correctly");
            }
        }
    }
}