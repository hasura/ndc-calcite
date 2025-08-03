package com.hasura.splunk;

import org.junit.Test;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Debug test to troubleshoot Splunk connection issues
 */
public class SplunkConnectionDebugTest {
    
    private Properties loadLocalProperties() {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("local-properties.settings")) {
            props.load(fis);
        } catch (IOException e) {
            System.out.println("Could not load local-properties.settings: " + e.getMessage());
        }
        return props;
    }
    
    @Test
    public void testSplunkConnectionDebug() throws Exception {
        Class.forName("com.hasura.splunk.SplunkDriver");
        
        Properties localProps = loadLocalProperties();
        
        // Build URL from properties
        String splunkUrl = localProps.getProperty("splunk.url", "https://kentest.xyz:8089");
        URI uri = new URI(splunkUrl);
        String jdbcUrl = String.format("jdbc:splunk://%s:%d/main", uri.getHost(), uri.getPort());
        
        Properties props = new Properties();
        props.setProperty("user", localProps.getProperty("splunk.username", "admin"));
        props.setProperty("password", localProps.getProperty("splunk.password", ""));
        props.setProperty("ssl", "true");
        props.setProperty("disableSslValidation", localProps.getProperty("splunk.ssl.insecure", "true"));
        
        System.out.println("=== Splunk Connection Debug ===");
        System.out.println("URL: " + jdbcUrl);
        System.out.println("Username: " + props.getProperty("user"));
        System.out.println("SSL: " + props.getProperty("ssl"));
        System.out.println("Disable SSL Validation: " + props.getProperty("disableSslValidation"));
        
        // First test basic Calcite connection
        try {
            System.out.println("\n--- Testing Basic Calcite Connection ---");
            Properties calciteProps = new Properties();
            Connection basicConn = DriverManager.getConnection("jdbc:calcite:", calciteProps);
            System.out.println("✅ Basic Calcite connection successful");
            basicConn.close();
        } catch (SQLException e) {
            System.out.println("❌ Basic Calcite connection failed: " + e.getMessage());
        }
        
        try {
            Connection conn = DriverManager.getConnection(jdbcUrl, props);
            System.out.println("✅ Connection successful!");
            System.out.println("Connection: " + conn.getClass().getName());
            
            DatabaseMetaData metaData = conn.getMetaData();
            System.out.println("Database Product: " + metaData.getDatabaseProductName());
            System.out.println("URL: " + metaData.getURL());
            
            // Try a simple query
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 'Hello Splunk' as message");
                if (rs.next()) {
                    System.out.println("Query result: " + rs.getString("message"));
                }
            }
            
            conn.close();
            
        } catch (SQLException e) {
            System.out.println("❌ Connection failed: " + e.getMessage());
            System.out.println("SQL State: " + e.getSQLState());
            System.out.println("Error Code: " + e.getErrorCode());
            e.printStackTrace();
            
            // Don't fail the test, just report the issue
            System.out.println("Note: This is expected if Splunk instance is not available");
        }
    }
}