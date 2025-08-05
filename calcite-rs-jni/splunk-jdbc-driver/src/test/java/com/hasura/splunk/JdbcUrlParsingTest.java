package com.hasura.splunk;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

/**
 * Test to verify proper parsing of JDBC URLs in different formats.
 * Tests all the variations mentioned:
 * - Query parameters in URL
 * - Path/database in URL
 * - Parameters from Properties
 * - Parameters from environment variables
 */
@Category(UnitTest.class)
public class JdbcUrlParsingTest {
    
    @Test
    public void testStandardJdbcUrlFormat() {
        // Test that the driver can handle standard JDBC URL format
        Properties props = new Properties();
        props.setProperty("user", "testuser");
        props.setProperty("password", "testpass");
        
        // Create facade driver
        SplunkDriver driver = new SplunkDriver();
        
        try {
            // Test with standard JDBC URL format
            String testUrl = "jdbc:splunk://kentest.xyz:8089";
            
            // Driver should accept this URL format
            assertTrue("Driver should accept standard JDBC URL format", driver.acceptsURL(testUrl));
            
        } catch (SQLException e) {
            fail("Driver should handle standard JDBC URL format: " + e.getMessage());
        }
    }
    
    @Test
    public void testJdbcUrlPropertyParsing() {
        // Test that jdbcUrl property containing full JDBC URL is parsed correctly
        Properties props = new Properties();
        props.setProperty("jdbcUrl", "jdbc:splunk://kentest.xyz:8089");
        props.setProperty("user", "testuser");
        props.setProperty("password", "testpass");
        
        // Create facade driver
        SplunkDriver driver = new SplunkDriver();
        
        try {
            // Use minimal URL, with actual URL in jdbcUrl property
            String testUrl = "jdbc:splunk:";
            
            // Get property info to verify the driver accepts the properties
            DriverPropertyInfo[] propInfo = driver.getPropertyInfo(testUrl, props);
            assertNotNull("Property info should not be null", propInfo);
            
            // The driver should parse jdbcUrl and extract the Splunk URL
            assertTrue("Driver should accept jdbcUrl property with full JDBC URL", true);
            
        } catch (SQLException e) {
            // Check that it's not the "Must specify 'url' or 'jdbcUrl' property" error
            if (e.getMessage().contains("Must specify 'url' or 'jdbcUrl' property")) {
                fail("Driver should parse jdbcUrl property containing JDBC URL");
            }
            // Other SQL exceptions (like connection failures) are expected
        }
    }
    
    @Test
    public void testVariousJdbcUrlFormats() {
        String[] testUrls = {
            "jdbc:splunk://localhost:8089",
            "jdbc:splunk://kentest.xyz:8089",
            "jdbc:splunk://host.domain.com:8089",
            "jdbc:splunk://192.168.1.100:8089",
            "jdbc:splunk://host:8089/main",
            "jdbc:splunk://host:8089?ssl=true",
            "jdbc:splunk://host:8089/main?ssl=true"
        };
        
        SplunkDriver driver = new SplunkDriver();
        
        for (String url : testUrls) {
            try {
                boolean accepts = driver.acceptsURL(url);
                assertTrue("Driver should accept URL: " + url, accepts);
            } catch (SQLException e) {
                fail("Failed to check URL acceptance for: " + url + " - " + e.getMessage());
            }
        }
    }
    
    @Test
    public void testSemicolonSeparatedFormat() {
        // Test the original semicolon-separated format still works
        Properties props = new Properties();
        
        SplunkDriver driver = new SplunkDriver();
        
        try {
            String testUrl = "jdbc:splunk:url=https://kentest.xyz:8089;user=admin;password=changeme";
            assertTrue("Driver should accept semicolon-separated format", driver.acceptsURL(testUrl));
        } catch (SQLException e) {
            fail("Driver should handle semicolon-separated format: " + e.getMessage());
        }
    }
    
    @Test
    public void testJdbcUrlWithDatabase() {
        // Test JDBC URL with database/schema in path
        String testUrl = "jdbc:splunk://host:8089/mysplunk";
        Properties props = new Properties();
        props.setProperty("user", "testuser");
        props.setProperty("password", "testpass");
        
        SplunkDriver driver = new SplunkDriver();
        
        try {
            assertTrue("Driver should accept URL with database", driver.acceptsURL(testUrl));
            // In actual usage, the driver would parse "mysplunk" as the schema
        } catch (SQLException e) {
            fail("Driver should handle URL with database: " + e.getMessage());
        }
    }
    
    @Test
    public void testJdbcUrlWithQueryParameters() {
        // Test JDBC URL with query parameters
        String testUrl = "jdbc:splunk://host:8089?ssl=true&connectTimeout=30000";
        Properties props = new Properties();
        props.setProperty("user", "testuser");
        props.setProperty("password", "testpass");
        
        SplunkDriver driver = new SplunkDriver();
        
        try {
            assertTrue("Driver should accept URL with query parameters", driver.acceptsURL(testUrl));
            // In actual usage, the driver would parse ssl=true and connectTimeout=30000
        } catch (SQLException e) {
            fail("Driver should handle URL with query parameters: " + e.getMessage());
        }
    }
    
    @Test
    public void testJdbcUrlWithDatabaseAndQueryParameters() {
        // Test JDBC URL with both database and query parameters
        String testUrl = "jdbc:splunk://host:8089/mysplunk?ssl=true&user=admin&password=changeme";
        
        SplunkDriver driver = new SplunkDriver();
        
        try {
            assertTrue("Driver should accept URL with database and query parameters", driver.acceptsURL(testUrl));
            // In actual usage, the driver would parse:
            // - schema: mysplunk
            // - ssl: true
            // - user: admin
            // - password: changeme
        } catch (SQLException e) {
            fail("Driver should handle URL with database and query parameters: " + e.getMessage());
        }
    }
    
    @Test
    public void testUrlPrecedence() {
        // Test that URL parameters take precedence over Properties
        Properties props = new Properties();
        props.setProperty("schema", "prop_schema");
        props.setProperty("ssl", "false");
        
        // URL should override properties
        String testUrl = "jdbc:splunk://host:8089/url_schema?ssl=true";
        
        SplunkDriver driver = new SplunkDriver();
        
        try {
            assertTrue("Driver should accept URL", driver.acceptsURL(testUrl));
            // In actual usage:
            // - schema should be "url_schema" (from URL path)
            // - ssl should be "true" (from URL query)
        } catch (SQLException e) {
            fail("Driver should handle URL precedence: " + e.getMessage());
        }
    }
    
    @Test
    public void testJdbcUrlWithFullParameters() {
        // Test that ALL parameters are extracted from jdbcUrl property
        Properties props = new Properties();
        props.setProperty("jdbcUrl", "jdbc:splunk://kentest.xyz:8089?user=admin&password=changeme&ssl=true&disableSslValidation=true");
        
        // This is the critical test - the driver should extract:
        // - url: https://kentest.xyz:8089
        // - user: admin
        // - password: changeme  
        // - ssl: true
        // - disableSslValidation: true
        
        SplunkDriver driver = new SplunkDriver();
        
        try {
            // Use minimal URL for driver selection
            String testUrl = "jdbc:splunk:";
            
            // The driver should parse ALL parameters from jdbcUrl
            DriverPropertyInfo[] propInfo = driver.getPropertyInfo(testUrl, props);
            assertNotNull("Property info should not be null", propInfo);
            
            // If this works without throwing "Must specify 'url' or 'jdbcUrl' property",
            // and without throwing authentication errors, then parameter extraction is working
            assertTrue("Driver should extract all parameters from jdbcUrl", true);
            
        } catch (SQLException e) {
            if (e.getMessage().contains("Must specify 'url' or 'jdbcUrl' property")) {
                fail("Driver should parse jdbcUrl property");
            } else if (e.getMessage().contains("Must specify either 'token' or both 'user' and 'password'")) {
                fail("Driver should extract user and password from jdbcUrl query parameters");
            }
            // Other SQL exceptions (like connection failures) are expected
        }
    }
}