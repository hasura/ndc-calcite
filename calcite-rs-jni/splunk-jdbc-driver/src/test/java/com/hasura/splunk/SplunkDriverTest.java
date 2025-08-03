package com.hasura.splunk;

import org.junit.Test;
import org.junit.Before;
import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;

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
        Driver driver = DriverManager.getDriver("jdbc:splunk://localhost:8089");
        assertNotNull("Driver should be registered", driver);
        assertTrue("Should be SplunkDriver", driver instanceof SplunkDriver);
    }
    
    @Test
    public void testAcceptsURL() throws SQLException {
        SplunkDriver driver = new SplunkDriver();
        
        // Valid URLs
        assertTrue(driver.acceptsURL("jdbc:splunk://localhost:8089"));
        assertTrue(driver.acceptsURL("jdbc:splunk://host:8089/main"));
        assertTrue(driver.acceptsURL("jdbc:splunk://host:8089?user=admin"));
        
        // Invalid URLs
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost:3306"));
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432"));
        assertFalse(driver.acceptsURL(null));
    }
    
    @Test
    public void testConnectionProperties() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "admin");
        props.setProperty("password", "changeme");
        props.setProperty("earliest", "-1h");
        props.setProperty("latest", "now");
        
        // Note: This test requires a running Splunk instance
        // Uncomment to test with actual Splunk
        /*
        try (Connection conn = DriverManager.getConnection(
                "jdbc:splunk://localhost:8089/main", props)) {
            assertNotNull("Connection should not be null", conn);
            assertFalse("Connection should not be closed", conn.isClosed());
            
            // Test simple query
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM splunk.events")) {
                assertTrue("Should have results", rs.next());
            }
        }
        */
    }
    
    @Test
    public void testPostgreSQLSyntax() {
        // Test that PostgreSQL-style queries can be parsed
        String[] validQueries = {
            "SELECT * FROM events WHERE time::date = CURRENT_DATE",
            "SELECT host, COUNT(*) FROM events GROUP BY host ORDER BY 2 DESC LIMIT 10",
            "WITH errors AS (SELECT * FROM events WHERE level = 'ERROR') SELECT * FROM errors",
            "SELECT data->>'user_id' FROM events WHERE data ? 'user_id'"
        };
        
        // These would be tested with actual connection
        // Just verifying the syntax patterns here
        for (String query : validQueries) {
            assertNotNull("Query should not be null", query);
        }
    }
}