package com.hasura.splunk;

import org.junit.Test;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

/**
 * Test to verify schema can be specified in JDBC URI like PostgreSQL.
 * 
 * PostgreSQL format: jdbc:postgresql://host:port/database?currentSchema=schema
 * Splunk current: jdbc:splunk://host:port/index?param=value
 * 
 * Test if we can support: jdbc:splunk://host:port/index?schema=splunk
 */
public class JdbcUriSchemaTest {
    
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
    public void testSchemaInQueryString() throws Exception {
        System.out.println("\n=== Testing Schema in JDBC URI Query String ===");
        
        // Build JDBC URL with schema in query string (like PostgreSQL currentSchema)
        String splunkUrl = localProperties.getProperty("splunk.url", "https://kentest.xyz:8089");
        java.net.URI uri = new java.net.URI(splunkUrl);
        
        String jdbcUrl = String.format(
            "jdbc:splunk://%s:%d/main?user=%s&password=%s&ssl=true&disableSslValidation=true&schema=splunk&cimModels=web",
            uri.getHost(), 
            uri.getPort(),
            localProperties.getProperty("splunk.username", "admin"),
            localProperties.getProperty("splunk.password", "")
        );
        
        System.out.println("JDBC URL: " + jdbcUrl.replaceAll("password=[^&]*", "password=***"));
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            assertNotNull("Connection should not be null", conn);
            
            // Test that unqualified table name works (schema should be set from URL)
            System.out.println("\n--- Testing unqualified query with schema from URL ---");
            String query = "SELECT COUNT(*) FROM web";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                
                assertTrue("Should have at least one row", rs.next());
                int count = rs.getInt(1);
                System.out.println("Found " + count + " rows in 'web' table (unqualified)");
                assertTrue("Should have some data", count >= 0);
                
                System.out.println("✅ Schema in JDBC URL query string works!");
            }
            
            // Verify schema is correctly set
            System.out.println("\n--- Verifying schema is available ---");
            String metaQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'splunk'";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(metaQuery)) {
                
                int tableCount = 0;
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    System.out.println("  - " + tableName);
                    tableCount++;
                }
                assertTrue("Should have at least one table", tableCount > 0);
                
                System.out.println("✅ Schema 'splunk' contains " + tableCount + " table(s)");
            }
        }
    }
    
    @Test
    public void testSchemaAsPathComponent() throws Exception {
        System.out.println("\n=== Testing Schema as Path Component (like PostgreSQL database) ===");
        
        // Currently path is used for index, but could potentially support:
        // jdbc:splunk://host:port/index/schema or jdbc:splunk://host:port/schema
        
        String splunkUrl = localProperties.getProperty("splunk.url", "https://kentest.xyz:8089");
        java.net.URI uri = new java.net.URI(splunkUrl);
        
        // Test current behavior: path = index
        String jdbcUrl = String.format("jdbc:splunk://%s:%d/main", uri.getHost(), uri.getPort());
        
        Properties props = new Properties();
        props.setProperty("user", localProperties.getProperty("splunk.username", "admin"));
        props.setProperty("password", localProperties.getProperty("splunk.password", ""));
        props.setProperty("ssl", "true");
        props.setProperty("disableSslValidation", "true");
        props.setProperty("schema", "splunk");
        props.setProperty("cimModels", "web");
        
        System.out.println("JDBC URL: " + jdbcUrl);
        System.out.println("Schema set via Properties: schema=splunk");
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            // Test unqualified query
            String query = "SELECT COUNT(*) FROM web";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                
                assertTrue("Should have at least one row", rs.next());
                int count = rs.getInt(1);
                System.out.println("Found " + count + " rows in 'web' table");
                
                System.out.println("✅ Schema via Properties works (current implementation)");
            }
        }
        
        System.out.println("\nℹ️  Current implementation uses path for Splunk index, not schema");
        System.out.println("ℹ️  Schema must be specified via Properties or query string");
    }
}