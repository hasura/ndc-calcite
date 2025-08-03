package com.hasura.splunk;

import org.junit.Test;
import org.junit.BeforeClass;
import java.sql.*;
import java.util.Properties;

public class VerifyActualSplunkData {
    
    @BeforeClass
    public static void setUpClass() {
        try {
            Class.forName("com.hasura.splunk.SplunkDriver");
        } catch (ClassNotFoundException e) {
            System.err.println("SplunkDriver not found: " + e.getMessage());
        }
    }
    
    private Properties loadLocalProperties() {
        Properties props = new Properties();
        try (java.io.FileInputStream fis = new java.io.FileInputStream("local-properties.settings")) {
            props.load(fis);
        } catch (java.io.IOException e) {
            System.out.println("Could not load local-properties.settings: " + e.getMessage());
        }
        return props;
    }
    
    @Test
    public void verifyWebTableData() throws Exception {
        Properties localProps = loadLocalProperties();
        
        // Build URL from properties - use CIM model
        String splunkUrl = localProps.getProperty("splunk.url", "https://kentest.xyz:8089");
        java.net.URI uri = new java.net.URI(splunkUrl);
        String url = String.format("jdbc:splunk://%s:%d/splunk", uri.getHost(), uri.getPort());
        
        Properties props = new Properties();
        props.setProperty("user", localProps.getProperty("splunk.username", "admin"));
        props.setProperty("password", localProps.getProperty("splunk.password", ""));
        props.setProperty("ssl", "true");
        props.setProperty("disableSslValidation", localProps.getProperty("splunk.ssl.insecure", "true"));
        props.setProperty("earliest", "-24h");  // Look back 24 hours
        props.setProperty("latest", "now");
        props.setProperty("model", "cim");  // Use CIM model to get web table
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            System.out.println("\n==================== ACTUAL SPLUNK DATA VERIFICATION ====================");
            
            // First, let's see what tables are available
            DatabaseMetaData dbMeta = conn.getMetaData();
            System.out.println("\nAvailable tables:");
            try (ResultSet tables = dbMeta.getTables(null, null, "%", null)) {
                while (tables.next()) {
                    System.out.println("  - " + tables.getString("TABLE_NAME"));
                }
            }
            
            // Query 1: Get some web data
            System.out.println("\n\n=== Query 1: SELECT * FROM web WHERE bytes IS NOT NULL LIMIT 10 ===");
            String sql1 = "SELECT * FROM web WHERE bytes IS NOT NULL LIMIT 10";
            System.out.println("SQL: " + sql1);
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql1)) {
                
                ResultSetMetaData meta = rs.getMetaData();
                int count = 0;
                
                while (rs.next()) {
                    count++;
                    System.out.println("\nRow " + count + ":");
                    
                    // Show all columns but focus on bytes
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        String colName = meta.getColumnName(i);
                        if (colName.equals("bytes") || colName.contains("bytes")) {
                            Object value = rs.getObject(i);
                            System.out.println(String.format("  %-20s = %-20s [%s]", 
                                colName, 
                                value, 
                                value != null ? value.getClass().getName() : "NULL"));
                        }
                    }
                }
                
                System.out.println("\nTotal rows with non-null bytes: " + count);
            }
            
            // Query 2: Look for specific bytes value
            System.out.println("\n\n=== Query 2: SELECT bytes FROM web WHERE bytes = 950 ===");
            String sql2 = "SELECT bytes FROM web WHERE bytes = 950";
            System.out.println("SQL: " + sql2);
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql2)) {
                
                int count = 0;
                while (rs.next()) {
                    count++;
                    Object bytes = rs.getObject("bytes");
                    System.out.println("Row " + count + ": bytes = " + bytes + " [" + 
                        (bytes != null ? bytes.getClass().getName() : "NULL") + "]");
                }
                System.out.println("Found " + count + " rows with bytes = 950");
            }
            
            // Query 3: Get a sample of data to see what's actually there
            System.out.println("\n\n=== Query 3: SELECT bytes, status, uri FROM web LIMIT 20 ===");
            String sql3 = "SELECT bytes, status, uri FROM web LIMIT 20";
            System.out.println("SQL: " + sql3);
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql3)) {
                
                int count = 0;
                while (rs.next()) {
                    count++;
                    Object bytes = rs.getObject("bytes");
                    Object status = rs.getObject("status");
                    Object uriValue = rs.getObject("uri");
                    
                    System.out.println(String.format("Row %2d: bytes=%-10s status=%-5s uri=%s", 
                        count, bytes, status, uriValue));
                }
            }
        }
    }
}