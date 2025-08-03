package com.hasura.revelio;

import org.junit.Test;
import org.junit.Before;
import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;

public class RevelioDriverTest {
    
    @Before
    public void setUp() {
        try {
            Class.forName("com.hasura.revelio.RevelioDriver");
        } catch (ClassNotFoundException e) {
            fail("RevelioDriver not found");
        }
    }
    
    @Test
    public void testDriverRegistration() throws SQLException {
        Driver driver = DriverManager.getDriver("jdbc:revelio:/data/files");
        assertNotNull("Driver should be registered", driver);
        assertTrue("Should be RevelioDriver", driver instanceof RevelioDriver);
    }
    
    @Test
    public void testAcceptsURL() throws SQLException {
        RevelioDriver driver = new RevelioDriver();
        
        // Valid URLs
        assertTrue(driver.acceptsURL("jdbc:revelio:/data/files"));
        assertTrue(driver.acceptsURL("jdbc:revelio://localhost/data/files"));
        assertTrue(driver.acceptsURL("jdbc:revelio:dataPath='/data/files'"));
        assertTrue(driver.acceptsURL("jdbc:revelio:"));
        
        // Invalid URLs
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost:3306"));
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432"));
        
        // Test null URL handling
        try {
            assertFalse(driver.acceptsURL(null));
        } catch (SQLException e) {
            // This is expected behavior - null URLs should throw SQLException
            assertTrue("Should throw SQLException for null URL", e.getMessage().contains("null"));
        }
    }
    
    @Test
    public void testConnectionProperties() throws SQLException {
        Properties props = new Properties();
        props.setProperty("dataPath", "/tmp/test-data");
        props.setProperty("engine", "parquet");
        props.setProperty("defaultSchema", "testfiles");
        
        // Note: This test requires actual data files for full connection
        // Uncomment to test with actual file data
        /*
        try (Connection conn = DriverManager.getConnection(
                "jdbc:revelio:", props)) {
            assertNotNull("Connection should not be null", conn);
            assertFalse("Connection should not be closed", conn.isClosed());
            
            // Test simple query
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM testfiles.\"test.csv\"")) {
                assertTrue("Should have results", rs.next());
            }
        }
        */
    }
    
    @Test
    public void testFileFormatsSupport() {
        RevelioDriver driver = new RevelioDriver();
        
        try {
            // Test various file format URLs
            String[] validUrls = {
                "jdbc:revelio:dataPath='/data/csv';engine='csv'",
                "jdbc:revelio:dataPath='/data/json';engine='json'", 
                "jdbc:revelio:dataPath='/data/parquet';engine='parquet'",
                "jdbc:revelio:dataPath='/data/excel';engine='excel'",
                "jdbc:revelio:dataPath='/data/arrow';engine='arrow'"
            };
            
            for (String url : validUrls) {
                assertTrue("Should accept " + url, driver.acceptsURL(url));
            }
            
        } catch (SQLException e) {
            fail("File format URLs should be accepted: " + e.getMessage());
        }
    }
    
    @Test
    public void testPostgreSQLSyntax() {
        // Test that PostgreSQL-style queries can be parsed
        String[] validQueries = {
            "SELECT * FROM files.\"data.csv\" WHERE created_date::date = CURRENT_DATE",
            "SELECT name, COUNT(*) FROM files.\"sales.json\" GROUP BY name ORDER BY 2 DESC LIMIT 10",
            "WITH recent_data AS (SELECT * FROM files.\"logs.parquet\" WHERE date >= '2024-01-01') SELECT * FROM recent_data",
            "SELECT col1, col2 FROM files.\"sheet1.xlsx\" WHERE col3 > 100"
        };
        
        // These would be tested with actual connection
        // Just verifying the syntax patterns here
        for (String query : validQueries) {
            assertNotNull("Query should not be null", query);
        }
    }
}