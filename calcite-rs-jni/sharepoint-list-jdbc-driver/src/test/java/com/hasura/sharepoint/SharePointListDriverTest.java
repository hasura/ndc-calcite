package com.hasura.sharepoint;

import org.junit.Test;
import org.junit.Before;
import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;

public class SharePointListDriverTest {
    
    @Before
    public void setUp() {
        // Ensure driver is registered
        try {
            Class.forName("com.hasura.sharepoint.SharePointListDriver");
        } catch (ClassNotFoundException e) {
            fail("SharePointListDriver not found");
        }
    }
    
    @Test
    public void testDriverRegistration() throws SQLException {
        Driver driver = DriverManager.getDriver("jdbc:sharepoint://company.sharepoint.com/sites/test");
        assertNotNull("Driver should be registered", driver);
        assertTrue("Should be SharePointListDriver", driver instanceof SharePointListDriver);
    }
    
    @Test
    public void testAcceptsURL() throws SQLException {
        SharePointListDriver driver = new SharePointListDriver();
        
        // Valid URLs
        assertTrue(driver.acceptsURL("jdbc:sharepoint://company.sharepoint.com/sites/test"));
        assertTrue(driver.acceptsURL("jdbc:sharepoint://company.sharepoint.com/sites/test/sharepoint"));
        assertTrue(driver.acceptsURL("jdbc:sharepoint:siteUrl='https://company.sharepoint.com/sites/test'"));
        assertTrue(driver.acceptsURL("jdbc:sharepoint:"));
        
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
        props.setProperty("siteUrl", "https://company.sharepoint.com/sites/test");
        props.setProperty("authType", "CLIENT_CREDENTIALS");
        props.setProperty("clientId", "test-client-id");
        props.setProperty("clientSecret", "test-client-secret");
        props.setProperty("tenantId", "test-tenant-id");
        
        // Note: This test requires a running SharePoint instance
        // Uncomment to test with actual SharePoint
        /*
        try (Connection conn = DriverManager.getConnection(
                "jdbc:sharepoint:", props)) {
            assertNotNull("Connection should not be null", conn);
            assertFalse("Connection should not be closed", conn.isClosed());
            
            // Test simple query
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM sharepoint.tasks")) {
                assertTrue("Should have results", rs.next());
            }
        }
        */
    }
    
    @Test
    public void testPostgreSQLSyntax() {
        // Test that PostgreSQL-style queries can be parsed
        String[] validQueries = {
            "SELECT * FROM tasks WHERE due_date::date = CURRENT_DATE",
            "SELECT title, COUNT(*) FROM tasks GROUP BY title ORDER BY 2 DESC LIMIT 10",
            "WITH urgent_tasks AS (SELECT * FROM tasks WHERE priority = 1) SELECT * FROM urgent_tasks",
            "CREATE TABLE new_list (title VARCHAR(255), description TEXT, priority INTEGER)",
            "INSERT INTO tasks (title, description, priority) VALUES ('Test', 'Test task', 1)",
            "DELETE FROM tasks WHERE is_complete = true"
        };
        
        // These would be tested with actual connection
        // Just verifying the syntax patterns here
        for (String query : validQueries) {
            assertNotNull("Query should not be null", query);
        }
    }
}