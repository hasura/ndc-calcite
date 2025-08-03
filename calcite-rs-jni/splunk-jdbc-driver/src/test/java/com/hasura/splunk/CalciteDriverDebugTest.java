package com.hasura.splunk;

import org.apache.calcite.jdbc.Driver;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Test to debug Calcite driver behavior
 */
public class CalciteDriverDebugTest {
    
    @Test
    public void testCalciteDriverDirectly() throws SQLException {
        System.out.println("=== Testing Calcite Driver Directly ===");
        
        Driver calciteDriver = new Driver();
        
        // Test basic connection
        String url = "jdbc:calcite:";
        System.out.println("Testing URL: " + url);
        System.out.println("acceptsURL: " + calciteDriver.acceptsURL(url));
        
        Properties props = new Properties();
        
        // Test with minimal model
        String inlineModel = "{\n" +
            "  \"version\": \"1.0\",\n" +
            "  \"defaultSchema\": \"test\",\n" +
            "  \"schemas\": [\n" +
            "    {\n" +
            "      \"name\": \"test\",\n" +
            "      \"type\": \"map\",\n" +
            "      \"tables\": {\n" +
            "        \"dummy\": {\n" +
            "          \"type\": \"table\",\n" +
            "          \"rows\": [[1, \"test\"]]\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";
        
        props.setProperty("model", "inline:" + inlineModel);
        
        try {
            Connection connection = calciteDriver.connect(url, props);
            System.out.println("connect result: " + (connection != null ? "SUCCESS" : "NULL"));
            
            if (connection != null) {
                System.out.println("connection type: " + connection.getClass().getName());
                
                // Test basic functionality
                System.out.println("Database Product: " + connection.getMetaData().getDatabaseProductName());
                
                connection.close();
            } else {
                System.out.println("Connection was NULL!");
            }
        } catch (Exception e) {
            System.out.println("connect failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    @Test
    public void testSplunkDriverParseProperties() {
        System.out.println("=== Testing SplunkDriver Property Parsing ===");
        
        SplunkDriver splunkDriver = new SplunkDriver();
        
        try {
            // Test URL parsing
            String url = "jdbc:splunk://kentest.xyz:8089/main?user=admin&password=test";
            System.out.println("Testing URL: " + url);
            System.out.println("acceptsURL: " + splunkDriver.acceptsURL(url));
            
            Properties info = new Properties();
            info.setProperty("ssl", "true");
            
            // This will fail on connection but we want to see the property parsing
            try {
                Connection conn = splunkDriver.connect(url, info);
                System.out.println("Connection result: " + (conn != null ? "SUCCESS" : "NULL"));
            } catch (SQLException e) {
                System.out.println("Expected failure: " + e.getMessage());
                // Check if it's a connection issue vs property parsing issue
                if (e.getMessage().contains("Failed to create Calcite connection - returned null")) {
                    System.out.println("Issue is with Calcite connection creation!");
                } else if (e.getMessage().contains("Failed to create Splunk schema")) {
                    System.out.println("Issue is with Splunk schema creation!");
                } else {
                    System.out.println("Different issue: " + e.getMessage());
                }
            }
            
        } catch (Exception e) {
            System.out.println("URL parsing failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}