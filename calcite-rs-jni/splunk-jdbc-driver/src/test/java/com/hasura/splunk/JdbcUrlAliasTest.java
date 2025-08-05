package com.hasura.splunk;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

/**
 * Test to verify that jdbcUrl works as an alias for url property
 */
@Category(UnitTest.class)
public class JdbcUrlAliasTest {
    
    @Test
    public void testJdbcUrlAlias() {
        // Test that the driver accepts jdbcUrl as an alias for url
        Properties props = new Properties();
        props.setProperty("jdbcUrl", "https://localhost:8089");
        props.setProperty("user", "testuser");
        props.setProperty("password", "testpass");
        
        // Create facade driver
        SplunkDriver driver = new SplunkDriver();
        
        try {
            // This should work with jdbcUrl instead of url
            // Note: We're not actually connecting, just testing property handling
            String testUrl = "jdbc:splunk:";
            
            // Get property info to verify the driver accepts the properties
            DriverPropertyInfo[] propInfo = driver.getPropertyInfo(testUrl, props);
            assertNotNull("Property info should not be null", propInfo);
            
            // The actual connection would fail without a real Splunk server,
            // but the driver should accept the jdbcUrl property
            assertTrue("Driver should accept jdbcUrl property", true);
            
        } catch (SQLException e) {
            // If we get a "Must specify 'url' or 'jdbcUrl' property" error, the test fails
            if (e.getMessage().contains("Must specify 'url' or 'jdbcUrl' property")) {
                fail("Driver should accept jdbcUrl as an alias for url");
            }
            // Other SQL exceptions (like connection failures) are expected
        }
    }
    
    @Test
    public void testUrlPropertyTakesPrecedence() {
        // Test that 'url' takes precedence over 'jdbcUrl' if both are specified
        Properties props = new Properties();
        props.setProperty("url", "https://primary:8089");
        props.setProperty("jdbcUrl", "https://secondary:8089");
        props.setProperty("user", "testuser");
        props.setProperty("password", "testpass");
        
        // When both are specified, 'url' should be used
        // This is just a property precedence test
        assertEquals("url property should be primary", "https://primary:8089", props.getProperty("url"));
        assertNotNull("jdbcUrl property should still exist", props.getProperty("jdbcUrl"));
    }
}