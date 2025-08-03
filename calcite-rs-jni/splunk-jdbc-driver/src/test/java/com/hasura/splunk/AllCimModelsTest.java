package com.hasura.splunk;

import org.junit.Test;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

/**
 * Test to verify that all 24 CIM models are created when no cimModels configuration is provided.
 */
public class AllCimModelsTest {
    
    private static Properties localProperties;
    
    // All 24 official Splunk CIM data models
    private static final Set<String> EXPECTED_CIM_MODELS = new HashSet<>(Arrays.asList(
        "alerts",
        "authentication", 
        "certificates",
        "change",
        "data_access",
        "databases",
        "data_loss_prevention",
        "email",
        "endpoint",
        "event_signatures",
        "interprocess_messaging",
        "intrusion_detection",
        "inventory",
        "jvm",
        "malware",
        "network_resolution",
        "network_sessions",
        "network_traffic",
        "performance",
        "splunk_audit_logs",
        "ticket_management",
        "updates",
        "vulnerabilities",
        "web"
    ));
    
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
    public void testAllCimModelsCreatedByDefault() throws Exception {
        System.out.println("\n=== Testing All 24 CIM Models Created by Default ===");
        
        // Use configuration from local-properties.settings but WITHOUT cimModels parameter
        String splunkUrl = localProperties.getProperty("splunk.url", "https://kentest.xyz:8089");
        java.net.URI uri = new java.net.URI(splunkUrl);
        String jdbcUrl = String.format("jdbc:splunk://%s:%d/main", uri.getHost(), uri.getPort());
        
        Properties props = new Properties();
        props.setProperty("user", localProperties.getProperty("splunk.username", "admin"));
        props.setProperty("password", localProperties.getProperty("splunk.password", ""));
        props.setProperty("ssl", "true");
        props.setProperty("disableSslValidation", "true");
        props.setProperty("schema", "splunk");
        // NOTE: Deliberately NOT setting cimModels - should default to all 24
        
        System.out.println("Connecting without cimModels configuration...");
        System.out.println("JDBC URL: " + jdbcUrl);
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            assertNotNull("Connection should not be null", conn);
            
            // Query information_schema.tables to get all available tables
            String query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'splunk' ORDER BY table_name";
            
            Set<String> foundTables = new HashSet<>();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                
                System.out.println("\nFound tables:");
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    foundTables.add(tableName);
                    System.out.println("  - " + tableName);
                }
            }
            
            System.out.println("\nTotal tables found: " + foundTables.size());
            System.out.println("Expected CIM models: " + EXPECTED_CIM_MODELS.size());
            
            // Verify we have all 24 CIM models
            assertEquals("Should have all 24 CIM models", EXPECTED_CIM_MODELS.size(), foundTables.size());
            
            // Verify each expected CIM model is present
            for (String expectedModel : EXPECTED_CIM_MODELS) {
                assertTrue("Should find CIM model: " + expectedModel, foundTables.contains(expectedModel));
            }
            
            // Verify no unexpected tables
            for (String foundTable : foundTables) {
                assertTrue("Unexpected table found: " + foundTable, EXPECTED_CIM_MODELS.contains(foundTable));
            }
            
            System.out.println("✅ All 24 CIM models confirmed!");
            
            // Also test via pg_catalog.pg_tables
            System.out.println("\n--- Verifying via pg_catalog.pg_tables ---");
            String pgQuery = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'splunk' ORDER BY tablename";
            
            Set<String> pgFoundTables = new HashSet<>();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(pgQuery)) {
                
                while (rs.next()) {
                    String tableName = rs.getString("tablename");
                    pgFoundTables.add(tableName);
                }
            }
            
            assertEquals("pg_catalog should also show all 24 CIM models", EXPECTED_CIM_MODELS.size(), pgFoundTables.size());
            assertEquals("Both queries should return the same tables", foundTables, pgFoundTables);
            
            System.out.println("✅ pg_catalog.pg_tables also confirms all 24 CIM models!");
        }
    }
    
    @Test
    public void testLimitedCimModelsConfiguration() throws Exception {
        System.out.println("\n=== Testing Limited CIM Models Configuration ===");
        
        // Use configuration WITH specific cimModels parameter for comparison
        String splunkUrl = localProperties.getProperty("splunk.url", "https://kentest.xyz:8089");
        java.net.URI uri = new java.net.URI(splunkUrl);
        String jdbcUrl = String.format("jdbc:splunk://%s:%d/main", uri.getHost(), uri.getPort());
        
        Properties props = new Properties();
        props.setProperty("user", localProperties.getProperty("splunk.username", "admin"));
        props.setProperty("password", localProperties.getProperty("splunk.password", ""));
        props.setProperty("ssl", "true");
        props.setProperty("disableSslValidation", "true");
        props.setProperty("schema", "splunk");
        props.setProperty("cimModels", "web,authentication,network_traffic"); // Only 3 models
        
        System.out.println("Connecting with limited cimModels=web,authentication,network_traffic...");
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            String query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'splunk' ORDER BY table_name";
            
            Set<String> foundTables = new HashSet<>();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                
                System.out.println("\nFound tables:");
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    foundTables.add(tableName);
                    System.out.println("  - " + tableName);
                }
            }
            
            System.out.println("\nTotal tables found: " + foundTables.size());
            
            // Should only have 3 tables
            assertEquals("Should have only 3 specified CIM models", 3, foundTables.size());
            assertTrue("Should have web model", foundTables.contains("web"));
            assertTrue("Should have authentication model", foundTables.contains("authentication"));
            assertTrue("Should have network_traffic model", foundTables.contains("network_traffic"));
            
            System.out.println("✅ Limited CIM models configuration works correctly!");
        }
    }
}