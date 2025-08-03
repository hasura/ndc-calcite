package com.hasura.splunk;

import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.*;
import java.sql.*;
import java.util.Properties;

public class ConverterRuleTest {
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
        try {
            Properties localProps = loadLocalProperties();
            
            String splunkUrl = localProps.getProperty("splunk.url", "https://kentest.xyz:8089");
            java.net.URI uri = new java.net.URI(splunkUrl);
            String url = String.format("jdbc:splunk://%s:%d/main", uri.getHost(), uri.getPort());
            
            Properties props = new Properties();
            props.setProperty("user", localProps.getProperty("splunk.username", "admin"));
            props.setProperty("password", localProps.getProperty("splunk.password", ""));
            props.setProperty("ssl", "true");
            props.setProperty("disableSslValidation", localProps.getProperty("splunk.ssl.insecure", "true"));
            props.setProperty("earliest", "-1h");
            props.setProperty("latest", "now");
            
            Connection conn = DriverManager.getConnection(url, props);
            isRealSplunkConnection = true;
            System.out.println("✅ Connected to real Splunk instance for converter rule testing");
            return conn;
            
        } catch (Exception e) {
            System.out.println("⚠️ Could not connect to Splunk: " + e.getMessage());
            throw new SQLException("Real Splunk connection required", e);
        }
    }
    
    @Test
    public void testSimpleProjection() throws SQLException {
        if (!isRealSplunkConnection) {
            System.out.println("Skipping converter rule test - no real Splunk connection");
            return;
        }
        
        System.out.println("🔍 Testing simple projection to see if converter rules are triggered:");
        System.out.println("  -> Executing: SELECT web.bytes FROM web LIMIT 1");
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT web.bytes FROM web LIMIT 1")) {
            
            System.out.println("  -> Query executed successfully");
            if (rs.next()) {
                Object bytesValue = rs.getObject("bytes");
                System.out.println("  -> Result: bytes=" + bytesValue + " (type: " + (bytesValue != null ? bytesValue.getClass().getSimpleName() : "null") + ")");
            }
        }
    }
    
    @Test
    public void testBasicCast() throws SQLException {
        if (!isRealSplunkConnection) {
            System.out.println("Skipping basic CAST test - no real Splunk connection");
            return;
        }
        
        System.out.println("🔍 Testing basic CAST to see if converter rules handle it:");
        System.out.println("  -> Executing: SELECT CAST(web.bytes AS VARCHAR) as bytes_cast FROM web LIMIT 1");
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT CAST(web.bytes AS VARCHAR) as bytes_cast FROM web LIMIT 1")) {
            
            System.out.println("  -> CAST query executed successfully");
            if (rs.next()) {
                String bytesCast = rs.getString("bytes_cast");
                System.out.println("  -> Result: bytes_cast='" + bytesCast + "' (is null: " + (bytesCast == null) + ")");
                
                if (bytesCast != null) {
                    System.out.println("🎉 SUCCESS: CAST is working!");
                } else {
                    System.out.println("❌ FAILURE: CAST still returns null");
                }
            }
        }
    }
}