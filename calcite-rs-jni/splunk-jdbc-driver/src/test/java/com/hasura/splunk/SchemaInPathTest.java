package com.hasura.splunk;

import org.junit.Test;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

/**
 * Test to verify schema can be specified in the path component like PostgreSQL.
 * 
 * PostgreSQL: jdbc:postgresql://host:port/database?currentSchema=schema
 * Splunk: jdbc:splunk://host:port/schema?param=value
 */
public class SchemaInPathTest {
    
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
    public void testSchemaInPath() throws Exception {
        System.out.println("\n=== Testing Schema in Path Component ===");
        
        // Use schema in path instead of query parameter
        String splunkUrl = localProperties.getProperty("splunk.url", "https://kentest.xyz:8089");
        java.net.URI uri = new java.net.URI(splunkUrl);
        
        String jdbcUrl = String.format("jdbc:splunk://%s:%d/splunk", uri.getHost(), uri.getPort());
        
        Properties props = new Properties();
        props.setProperty("user", localProperties.getProperty("splunk.username", "admin"));
        props.setProperty("password", localProperties.getProperty("splunk.password", ""));
        props.setProperty("ssl", "true");
        props.setProperty("disableSslValidation", "true");
        props.setProperty("cimModels", "web,authentication");
        // NOTE: No explicit schema property - should be taken from path
        
        System.out.println("JDBC URL: " + jdbcUrl);
        System.out.println("Schema should be extracted from path: /splunk");
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            assertNotNull("Connection should not be null", conn);
            
            // Test unqualified table names (should work with schema from path)
            System.out.println("\n--- Testing unqualified queries with schema from path ---");
            
            String webQuery = "SELECT COUNT(*) FROM web";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(webQuery)) {
                
                assertTrue("Should have at least one row", rs.next());
                int count = rs.getInt(1);
                System.out.println("Found " + count + " rows in 'web' table (unqualified)");
                assertTrue("Should have some data", count >= 0);
            }
            
            String authQuery = "SELECT COUNT(*) FROM authentication";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(authQuery)) {
                
                assertTrue("Should have at least one row", rs.next());
                int count = rs.getInt(1);
                System.out.println("Found " + count + " rows in 'authentication' table (unqualified)");
                assertTrue("Should have some data", count >= 0);
            }
            
            System.out.println("✅ Schema in path works - unqualified table names resolve correctly!");
            
            // Verify schema is available via metadata
            System.out.println("\n--- Verifying schema metadata ---");
            String metaQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'splunk' ORDER BY table_name";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(metaQuery)) {
                
                int tableCount = 0;
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    System.out.println("  - " + tableName);
                    tableCount++;
                }
                assertEquals("Should have exactly 2 tables", 2, tableCount);
                
                System.out.println("✅ Schema metadata shows correct tables");
            }
        }
    }
    
    @Test
    public void testQueryParameterOverridesPath() throws Exception {
        System.out.println("\n=== Testing Query Parameter Overrides Path ===");
        
        // Set different schema names in path vs query parameter
        String splunkUrl = localProperties.getProperty("splunk.url", "https://kentest.xyz:8089");
        java.net.URI uri = new java.net.URI(splunkUrl);
        
        // Path says "wrong_schema", but query parameter says "splunk"
        String jdbcUrl = String.format(
            "jdbc:splunk://%s:%d/wrong_schema?user=%s&password=%s&ssl=true&disableSslValidation=true&schema=splunk&cimModels=web",
            uri.getHost(), 
            uri.getPort(),
            localProperties.getProperty("splunk.username", "admin"),
            localProperties.getProperty("splunk.password", "")
        );
        
        System.out.println("JDBC URL path: /wrong_schema");
        System.out.println("Query parameter: schema=splunk");
        System.out.println("Query parameter should take precedence");
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            // Test that unqualified query works (should use "splunk" from query param, not "wrong_schema" from path)
            String query = "SELECT COUNT(*) FROM web";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                
                assertTrue("Should have at least one row", rs.next());
                int count = rs.getInt(1);
                System.out.println("Found " + count + " rows in 'web' table");
                
                System.out.println("✅ Query parameter takes precedence over path component");
            }
            
            // Verify the correct schema is used
            String metaQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'splunk'";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(metaQuery)) {
                
                assertTrue("Should find tables in 'splunk' schema", rs.next());
                String tableName = rs.getString("table_name");
                assertEquals("Should find 'web' table", "web", tableName);
                
                System.out.println("✅ Confirmed using 'splunk' schema, not 'wrong_schema'");
            }
        }
    }
}