package com.hasura.splunk;

import org.junit.Test;
import org.junit.BeforeClass;
import static org.junit.Assert.*;
import java.sql.*;
import java.util.Properties;

public class ShowWebDataWithRealConnection {
    
    @BeforeClass
    public static void setUpClass() {
        try {
            Class.forName("com.hasura.splunk.SplunkDriver");
        } catch (ClassNotFoundException e) {
            fail("SplunkDriver not found: " + e.getMessage());
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
    public void showWebData() throws Exception {
        // Try to connect with SplunkDriver using real settings from local properties
        Properties localProps = loadLocalProperties();
        
        // Build URL from properties
        String splunkUrl = localProps.getProperty("splunk.url", "https://kentest.xyz:8089");
        java.net.URI uri = new java.net.URI(splunkUrl);
        String url = String.format("jdbc:splunk://%s:%d/main", uri.getHost(), uri.getPort());
        
        Properties props = new Properties();
        props.setProperty("user", localProps.getProperty("splunk.username", "admin"));
        props.setProperty("password", localProps.getProperty("splunk.password", ""));
        props.setProperty("ssl", "true");
        props.setProperty("disableSslValidation", localProps.getProperty("splunk.ssl.insecure", "true"));
        props.setProperty("earliest", "-1h");
        props.setProperty("latest", "now");
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            System.out.println("\n==================== WEB TABLE DATA (Multiple Fields) ====================");
            
            // Query multiple fields
            String sql = "SELECT _time, host, bytes, bytes_in, bytes_out, status, uri, src_ip, dest_ip FROM web LIMIT 10";
            System.out.println("SQL: " + sql);
            System.out.println();
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                
                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();
                
                // Print column info
                System.out.println("Columns:");
                for (int i = 1; i <= columnCount; i++) {
                    System.out.println(String.format("  %d: %-15s (%s)", i, meta.getColumnName(i), meta.getColumnTypeName(i)));
                }
                
                System.out.println("\n----- DATA -----");
                int row = 0;
                while (rs.next()) {
                    row++;
                    System.out.println("\nRow " + row + ":");
                    
                    for (int i = 1; i <= columnCount; i++) {
                        String colName = meta.getColumnName(i);
                        Object value = rs.getObject(i);
                        String valueStr = (value != null) ? value.toString() : "NULL";
                        String typeStr = (value != null) ? value.getClass().getSimpleName() : "null";
                        
                        // Special handling for bytes to show the actual numeric value
                        if (colName.equals("bytes") && value != null) {
                            System.out.println(String.format("  %-15s = %-30s [%s] (raw: %s)", 
                                colName, valueStr, typeStr, value));
                        } else {
                            System.out.println(String.format("  %-15s = %-30s [%s]", 
                                colName, valueStr, typeStr));
                        }
                    }
                }
                
                System.out.println("\n\nTotal rows: " + row);
            }
            
            // Look for specific bytes value
            System.out.println("\n\n==================== LOOKING FOR BYTES = 950 ====================");
            sql = "SELECT bytes, status, uri FROM web WHERE bytes = 950 LIMIT 5";
            System.out.println("SQL: " + sql);
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                
                int count = 0;
                while (rs.next()) {
                    count++;
                    Object bytes = rs.getObject("bytes");
                    Object status = rs.getObject("status");
                    Object uriValue = rs.getObject("uri");
                    
                    System.out.println(String.format("Row %d: bytes=%s [%s], status=%s, uri=%s", 
                        count, bytes, 
                        bytes != null ? bytes.getClass().getName() : "null",
                        status, uriValue));
                }
                
                System.out.println("\nFound " + count + " rows with bytes = 950");
            }
            
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            throw e;
        }
    }
}