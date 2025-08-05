package com.hasura.splunk;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

/**
 * Debug test to troubleshoot Splunk connection issues
 */
@Category(IntegrationTest.class)
public class SplunkConnectionDebugTest extends BaseIntegrationTest {
    
    @Test
    public void testSplunkConnectionDebug() throws Exception {
        System.out.println("=== Splunk Connection Debug ===");
        System.out.println("URL: " + jdbcUrl);
        System.out.println("Connection Properties: " + connectionProps);
        
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
        
        // Test actual Splunk connection
        try {
            Connection conn = createTestConnection();
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