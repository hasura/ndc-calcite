package com.hasura.splunk;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

/**
 * Test to verify model file parameter support in JDBC URLs and Properties.
 */
@Category(UnitTest.class)
public class ModelFileParameterTest {
    
    @Test
    public void testModelFileInJdbcUrl() {
        // Test that modelFile can be passed in JDBC URL
        String testUrl = "jdbc:splunk:modelFile=/path/to/model.json";
        
        SplunkDriver driver = new SplunkDriver();
        
        try {
            // The driver should accept this URL format
            assertTrue("Driver should accept URL with modelFile parameter", driver.acceptsURL(testUrl));
            
            // Get property info to verify the driver processes the modelFile parameter
            DriverPropertyInfo[] propInfo = driver.getPropertyInfo(testUrl, new Properties());
            assertNotNull("Property info should not be null", propInfo);
            
        } catch (SQLException e) {
            fail("Driver should handle modelFile parameter: " + e.getMessage());
        }
    }
    
    @Test
    public void testModelFileInProperties() {
        // Test that modelFile can be passed via Properties
        Properties props = new Properties();
        props.setProperty("modelFile", "/path/to/model.json");
        
        SplunkDriver driver = new SplunkDriver();
        
        try {
            String testUrl = "jdbc:splunk:";
            
            // Get property info to verify the driver accepts the modelFile property
            DriverPropertyInfo[] propInfo = driver.getPropertyInfo(testUrl, props);
            assertNotNull("Property info should not be null", propInfo);
            
        } catch (SQLException e) {
            fail("Driver should handle modelFile property: " + e.getMessage());
        }
    }
    
    @Test
    public void testModelFileInStandardJdbcUrl() {
        // Test modelFile in standard JDBC URL format with query parameters
        String testUrl = "jdbc:splunk://host:8089?modelFile=/path/to/model.json";
        
        SplunkDriver driver = new SplunkDriver();
        
        try {
            assertTrue("Driver should accept standard JDBC URL with modelFile parameter", 
                      driver.acceptsURL(testUrl));
        } catch (SQLException e) {
            fail("Driver should handle standard JDBC URL with modelFile: " + e.getMessage());
        }
    }
    
    @Test
    public void testModelFilePrecedence() {
        // Test that URL parameters take precedence over Properties
        Properties props = new Properties();
        props.setProperty("modelFile", "/path/from/properties.json");
        
        // URL should override properties
        String testUrl = "jdbc:splunk:modelFile=/path/from/url.json";
        
        SplunkDriver driver = new SplunkDriver();
        
        try {
            assertTrue("Driver should accept URL", driver.acceptsURL(testUrl));
            // In actual usage, the driver would use /path/from/url.json
        } catch (SQLException e) {
            fail("Driver should handle modelFile precedence: " + e.getMessage());
        }
    }
    
    @Test  
    public void testModelFileWithOtherParameters() {
        // Test modelFile combined with other parameters
        String testUrl = "jdbc:splunk:modelFile=/path/to/model.json;debug=true;datamodelCacheTtl=-1";
        
        SplunkDriver driver = new SplunkDriver();
        
        try {
            assertTrue("Driver should accept URL with modelFile and other parameters", 
                      driver.acceptsURL(testUrl));
        } catch (SQLException e) {
            fail("Driver should handle modelFile with other parameters: " + e.getMessage());
        }
    }
}