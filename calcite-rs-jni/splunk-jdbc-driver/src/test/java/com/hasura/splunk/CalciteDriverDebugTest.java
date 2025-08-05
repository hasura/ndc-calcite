package com.hasura.splunk;

import org.apache.calcite.jdbc.Driver;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Test to debug Calcite driver behavior
 */
@Category(UnitTest.class)
public class CalciteDriverDebugTest {
    
    @Test
    public void testCalciteDriverDirectly() throws SQLException {
        System.out.println("=== Testing Calcite Driver Directly ===");
        
        Driver calciteDriver = new Driver();
        
        // Test basic connection acceptance
        String url = "jdbc:calcite:";
        System.out.println("Testing URL: " + url);
        System.out.println("acceptsURL: " + calciteDriver.acceptsURL(url));
        
        // Just test basic URL acceptance, don't try to connect without a proper model
        assertTrue("Calcite driver should accept jdbc:calcite: URLs", calciteDriver.acceptsURL(url));
        assertFalse("Calcite driver should not accept non-calcite URLs", calciteDriver.acceptsURL("jdbc:mysql://localhost"));
    }
    
    @Test
    public void testSplunkDriverUrlAcceptance() {
        System.out.println("=== Testing SplunkDriver URL Acceptance ===");
        
        SplunkDriver splunkDriver = new SplunkDriver();
        
        try {
            // Test various URL formats for acceptance (without credentials)
            String[] testUrls = {
                "jdbc:splunk:",
                "jdbc:splunk:url=https://localhost:8089",
                "jdbc:splunk:url=https://example.com:8089;ssl=true",
                "jdbc:splunk:url=https://localhost:8089;app=search",
                "jdbc:calcite:",
                "jdbc:mysql://localhost:3306/test"
            };
            
            for (String url : testUrls) {
                System.out.println("Testing URL: " + url);
                boolean accepts = splunkDriver.acceptsURL(url);
                System.out.println("acceptsURL: " + accepts);
                System.out.println();
            }
            
        } catch (Exception e) {
            System.out.println("URL acceptance test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}