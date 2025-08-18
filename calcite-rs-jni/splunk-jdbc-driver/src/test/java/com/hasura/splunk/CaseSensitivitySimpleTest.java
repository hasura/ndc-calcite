package com.hasura.splunk;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

/**
 * Simple test to verify case sensitivity configuration.
 */
@Category(UnitTest.class)
public class CaseSensitivitySimpleTest {
    
    @Test
    public void testConnectionConfiguration() {
        Properties props = new Properties();
        props.setProperty("url", "mock");
        props.setProperty("user", "test");
        props.setProperty("password", "test");
        
        try (Connection conn = DriverManager.getConnection("jdbc:splunk:", props)) {
            // Get the connection config
            org.apache.calcite.jdbc.CalciteConnection calciteConn = 
                conn.unwrap(org.apache.calcite.jdbc.CalciteConnection.class);
            
            org.apache.calcite.config.CalciteConnectionConfig config = calciteConn.config();
            
            // Check configuration
            System.out.println("Lex: " + config.lex());
            System.out.println("Unquoted casing: " + config.unquotedCasing());
            System.out.println("Quoted casing: " + config.quotedCasing());
            System.out.println("Case sensitive: " + config.caseSensitive());
            
            // Verify PostgreSQL-compatible settings
            assertEquals("Lex should be ORACLE", org.apache.calcite.config.Lex.ORACLE, config.lex());
            assertEquals("Unquoted casing should be TO_LOWER", 
                org.apache.calcite.avatica.util.Casing.TO_LOWER, config.unquotedCasing());
            assertTrue("Should be case sensitive", config.caseSensitive());
            
        } catch (SQLException e) {
            fail("Connection failed: " + e.getMessage());
        }
    }
}