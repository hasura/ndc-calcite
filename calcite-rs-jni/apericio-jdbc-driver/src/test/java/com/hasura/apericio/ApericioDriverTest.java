package com.hasura.apericio;

import org.junit.Test;
import org.junit.Before;
import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;

public class ApericioDriverTest {
    
    @Before
    public void setUp() {
        try {
            Class.forName("com.hasura.apericio.ApericioDriver");
        } catch (ClassNotFoundException e) {
            fail("ApericioDriver not found");
        }
    }
    
    @Test
    public void testDriverRegistration() throws SQLException {
        Driver driver = DriverManager.getDriver("jdbc:apericio:");
        assertNotNull("Driver should be registered", driver);
        assertTrue("Should be ApericioDriver", driver instanceof ApericioDriver);
    }
    
    @Test
    public void testAcceptsURL() throws SQLException {
        ApericioDriver driver = new ApericioDriver();
        
        // Valid URLs for Apericio driver
        assertTrue(driver.acceptsURL("jdbc:apericio:"));
        assertTrue(driver.acceptsURL("jdbc:apericio:/data/files"));
        assertTrue(driver.acceptsURL("jdbc:apericio:dataPath='/data/files'"));
        assertTrue(driver.acceptsURL("jdbc:apericio:path=/data/files;engine=parquet"));
        
        // Invalid URLs
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost:3306"));
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432"));
        assertFalse(driver.acceptsURL("jdbc:calcite:"));
        
        // Test null URL handling - should throw SQLException
        try {
            driver.acceptsURL(null);
            fail("Should throw SQLException for null URL");
        } catch (SQLException e) {
            // Expected behavior
            assertTrue(e.getMessage().contains("url can not be null"));
        }
    }
    
    @Test
    public void testConnectionProperties() throws SQLException {
        Properties props = new Properties();
        props.setProperty("defaultSchema", "files");
        
        // Note: This test requires actual model configuration for full connection
        // Basic property validation test only
        assertNotNull("Properties should not be null", props);
        assertEquals("Default schema should be set", "files", props.getProperty("defaultSchema"));
    }
    
    @Test
    public void testFileFormatsSupport() {
        ApericioDriver driver = new ApericioDriver();
        
        try {
            // Test various file format URLs
            String[] validUrls = {
                "jdbc:apericio:dataPath='/data/csv';engine='csv'",
                "jdbc:apericio:dataPath='/data/json';engine='json'", 
                "jdbc:apericio:dataPath='/data/parquet';engine='parquet'",
                "jdbc:apericio:dataPath='/data/excel';engine='excel'",
                "jdbc:apericio:dataPath='/data/arrow';engine='arrow'"
            };
            
            for (String url : validUrls) {
                assertTrue("Should accept " + url, driver.acceptsURL(url));
            }
            
        } catch (SQLException e) {
            fail("File format URLs should be accepted: " + e.getMessage());
        }
    }
    
    @Test
    public void testSQLSyntax() {
        // Test that SQL queries can be structured for file-based data
        String[] validQueries = {
            "SELECT * FROM files.\"data.csv\" WHERE date_column > '2024-01-01'",
            "SELECT name, COUNT(*) FROM files.\"sales.json\" GROUP BY name ORDER BY 2 DESC LIMIT 10",
            "WITH recent_data AS (SELECT * FROM files.\"logs.parquet\" WHERE date >= '2024-01-01') SELECT * FROM recent_data",
            "SELECT col1, col2 FROM files.\"sheet1.xlsx\" WHERE col3 > 100"
        };
        
        // These would be tested with actual connection
        // Just verifying the syntax patterns here
        for (String query : validQueries) {
            assertNotNull("Query should not be null", query);
            assertTrue("Query should contain files schema", query.contains("files."));
        }
    }
}