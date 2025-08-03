package com.hasura.splunk;

import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.*;
import java.sql.*;
import java.util.Properties;

public class CastPushdownTest {
    private Connection connection;
    private boolean isRealSplunkConnection = false;
    
    @BeforeClass
    public static void setUpClass() {
        try {
            Class.forName("com.hasura.splunk.SplunkDriver");
        } catch (ClassNotFoundException e) {
            fail("SplunkDriver not found: " + e.getMessage());
        }
    }
    
    @Before
    public void setUp() throws SQLException {
        connection = createTestConnection();
        
        // Try to manually force rule registration 
        System.out.println("🔧 Attempting to manually register Splunk rules...");
        // We'll add this after getting the connection working
    }
    
    private Properties loadLocalProperties() {
        Properties props = new Properties();
        try (java.io.FileInputStream fis = new java.io.FileInputStream("local-properties.settings")) {
            props.load(fis);
        } catch (java.io.IOException e) {
            System.out.println("Could not load local-properties.settings: " + e.getMessage());
        }
        return props;
    }
    
    private Connection createTestConnection() throws SQLException {
        // First try to connect with SplunkDriver using real settings from local properties
        try {
            Properties localProps = loadLocalProperties();
            
            // Build URL from properties
            String splunkUrl = localProps.getProperty("splunk.url", "https://kentest.xyz:8089");
            java.net.URI uri = new java.net.URI(splunkUrl);
            String url = String.format("jdbc:splunk://%s:%d/splunk", uri.getHost(), uri.getPort());
            
            Properties props = new Properties();
            props.setProperty("user", localProps.getProperty("splunk.username", "admin"));
            props.setProperty("password", localProps.getProperty("splunk.password", ""));
            props.setProperty("ssl", "true");
            props.setProperty("disableSslValidation", localProps.getProperty("splunk.ssl.insecure", "true"));
            props.setProperty("earliest", "-1h");
            props.setProperty("latest", "now");
            props.setProperty("cimModels", "web");  // Include the web CIM table
            
            Connection conn = DriverManager.getConnection(url, props);
            isRealSplunkConnection = true;
            System.out.println("✅ Connected to real Splunk instance for CAST testing");
            return conn;
            
        } catch (Exception e) {
            System.out.println("⚠️ Could not connect to Splunk, skipping CAST pushdown tests: " + e.getMessage());
            throw new SQLException("Real Splunk connection required for CAST pushdown testing", e);
        }
    }
    
    @Test
    public void testCastBytesToVarchar() throws SQLException {
        // Skip if no real Splunk connection
        if (!isRealSplunkConnection) {
            System.out.println("Skipping CAST test - no real Splunk connection");
            return;
        }
        
        // First test simple projection to see if pushdown rule is working at all
        System.out.println("🔍 Testing simple projection first:");
        System.out.println("  -> Executing: SELECT web.bytes FROM web LIMIT 1");
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT web.bytes FROM web LIMIT 1")) {
            System.out.println("  -> Simple projection query executed");
            if (rs.next()) {
                System.out.println("Simple projection works: bytes=" + rs.getObject("bytes"));
            }
        }
        
        // Test CAST in WHERE clause to see if filter CAST handling works
        System.out.println("🔍 Testing CAST in WHERE clause:");
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT web.bytes FROM web WHERE CAST(web.bytes AS VARCHAR) = '950' LIMIT 1")) {
            if (rs.next()) {
                System.out.println("CAST in WHERE works: bytes=" + rs.getObject("bytes"));  
            } else {
                System.out.println("CAST in WHERE: no results");
            }
        }
        
        // Test if basic CAST works with literals  
        System.out.println("🔍 Testing CAST with literal:");
        System.out.println("  -> Executing: SELECT CAST(123 AS VARCHAR) as literal_cast");
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT CAST(123 AS VARCHAR) as literal_cast")) {
            System.out.println("  -> Query executed successfully");
            if (rs.next()) {
                String literalCast = rs.getString("literal_cast");
                System.out.println("CAST(123 AS VARCHAR) = '" + literalCast + "' (is null: " + (literalCast == null) + ")");
            }
        } catch (SQLException e) {
            System.out.println("  -> ERROR in CAST literal test: " + e.getMessage());
            throw e;
        }
        
        // Test simple expression that should work
        System.out.println("🔍 Testing simple string literal:");
        System.out.println("  -> Executing: SELECT 'test' as string_literal");
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 'test' as string_literal")) {
            System.out.println("  -> Query executed successfully");
            if (rs.next()) {
                String stringLiteral = rs.getString("string_literal");
                System.out.println("'test' = '" + stringLiteral + "'");
            }
        }
        
        // Test CAST(bytes AS VARCHAR) to ensure it returns the string representation
        String sql = "SELECT web.bytes, CAST(web.bytes AS VARCHAR) as bytes_cast FROM web LIMIT 5";
        
        System.out.println("🔍 Executing SQL: " + sql);
        System.out.println("  -> About to execute CAST query");
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            System.out.println("  -> CAST query executed successfully");
            
            boolean foundData = false;
            while (rs.next()) {
                foundData = true;
                Object bytesValue = rs.getObject("bytes");
                String bytesCast = rs.getString("bytes_cast");
                
                // Check if bytesValue is actually null when we do the CAST query
                System.out.println("Row data: bytes=" + bytesValue + ", bytes_cast=" + bytesCast);
                System.out.println("Are they the same row? bytes is " + (bytesValue == null ? "NULL" : "NOT NULL"));
                
                // The CAST should convert the numeric value to string, or return null for null input
                if (bytesValue != null) {
                    assertNotNull("CAST(bytes AS VARCHAR) should not return null for non-null input", bytesCast);
                    assertEquals("CAST(bytes AS VARCHAR) should return string representation of numeric value", 
                        bytesValue.toString(), bytesCast);
                } else {
                    assertNull("CAST(NULL AS VARCHAR) should return null", bytesCast);
                }
            }
            
            assertTrue("Test should have found some data", foundData);
        }
    }
    
    @Test
    public void testCastSpecificValue() throws SQLException {
        // Skip if no real Splunk connection
        if (!isRealSplunkConnection) {
            System.out.println("Skipping specific CAST test - no real Splunk connection");
            return;
        }
        
        // Test a specific value that was failing before
        String sql = "SELECT web.bytes, CAST(web.bytes AS VARCHAR) as bytes_cast FROM web WHERE web.bytes = 950";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            if (rs.next()) {
                Object bytesValue = rs.getObject("bytes");
                String bytesCast = rs.getString("bytes_cast");
                
                System.out.println("Specific test: bytes=" + bytesValue + " -> bytes_cast='" + bytesCast + "'");
                
                assertEquals(950L, bytesValue);
                assertEquals("CAST(950 AS VARCHAR) should return '950'", "950", bytesCast);
            }
        }
    }
}