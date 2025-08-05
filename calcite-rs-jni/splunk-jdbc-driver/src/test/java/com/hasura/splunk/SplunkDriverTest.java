package com.hasura.splunk;

import org.junit.Test;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class SplunkDriverTest {
    
    @Before
    public void setUp() {
        // Ensure driver is registered
        try {
            Class.forName("com.hasura.splunk.SplunkDriver");
        } catch (ClassNotFoundException e) {
            fail("SplunkDriver not found");
        }
    }
    
    @Test
    public void testDriverRegistration() throws SQLException {
        // Test that we can get a driver for Splunk URLs
        Driver driver = DriverManager.getDriver("jdbc:splunk:");
        assertNotNull("Driver should be registered", driver);
        
        // Test that our SplunkDriver can be instantiated directly
        SplunkDriver splunkDriver = new SplunkDriver();
        assertNotNull("SplunkDriver should be instantiable", splunkDriver);
        assertTrue("SplunkDriver should accept Splunk URLs", splunkDriver.acceptsURL("jdbc:splunk:"));
    }
    
    @Test
    public void testAcceptsURL() throws SQLException {
        SplunkDriver driver = new SplunkDriver();
        
        // Valid URLs (Splunk driver uses semicolon-separated parameters)
        assertTrue(driver.acceptsURL("jdbc:splunk:"));
        assertTrue(driver.acceptsURL("jdbc:splunk:url=https://localhost:8089"));
        assertTrue(driver.acceptsURL("jdbc:splunk:url=https://localhost:8089;ssl=true"));
        assertTrue(driver.acceptsURL("jdbc:splunk:url=https://localhost:8089;app=search"));
        
        // Invalid URLs
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost:3306"));
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432"));
        assertFalse(driver.acceptsURL("jdbc:calcite:"));
        assertFalse(driver.acceptsURL(null));
    }
    
    @Test
    public void testGetPropertyInfo() throws SQLException {
        SplunkDriver driver = new SplunkDriver();
        
        Properties info = new Properties();
        info.setProperty("user", "testuser");
        
        // Test property info retrieval
        DriverPropertyInfo[] propInfo = driver.getPropertyInfo("jdbc:splunk:", info);
        assertNotNull("Property info should not be null", propInfo);
        
        // The actual properties depend on the delegate driver implementation
        // This test mainly ensures the method doesn't throw an exception
    }
    
    @Test
    public void testVersionInfo() throws SQLException {
        SplunkDriver driver = new SplunkDriver();
        
        // Test version methods
        int majorVersion = driver.getMajorVersion();
        int minorVersion = driver.getMinorVersion();
        boolean jdbcCompliant = driver.jdbcCompliant();
        
        // These should return reasonable values
        assertTrue("Major version should be positive", majorVersion >= 0);
        assertTrue("Minor version should be positive", minorVersion >= 0);
        
        // JDBC compliance depends on the underlying implementation
        // Just ensure the method doesn't throw an exception
        assertNotNull("JDBC compliance check should complete", Boolean.valueOf(jdbcCompliant));
    }
}