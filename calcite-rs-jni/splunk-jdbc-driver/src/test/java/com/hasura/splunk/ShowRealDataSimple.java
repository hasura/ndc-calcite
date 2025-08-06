package com.hasura.splunk;

import org.junit.Test;
import org.junit.BeforeClass;
import java.sql.*;
import java.util.Properties;

public class ShowRealDataSimple {
    
    @BeforeClass
    public static void setUpClass() {
        try {
            Class.forName("com.hasura.splunk.SplunkDriver");
        } catch (ClassNotFoundException e) {
            System.err.println("SplunkDriver not found: " + e.getMessage());
        }
    }
    
    @Test
    public void showWebData() throws Exception {
        System.out.println("\n==================== WEB TABLE DATA ====================");
        
        // Direct connection setup to avoid recursion
        Properties props = new Properties();
        props.setProperty("user", "admin");
        props.setProperty("password", "changed");
        props.setProperty("ssl", "true");
        props.setProperty("disableSslValidation", "true");
        props.setProperty("earliest", "-1h");
        props.setProperty("latest", "now");
        
        String url = "jdbc:splunk://kentest.xyz:8089/main";
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            // Simple query - just get the data
            String sql = "SELECT * FROM web LIMIT 5";
            System.out.println("SQL: " + sql);
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                
                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();
                
                // Print column names
                System.out.println("\nColumns:");
                for (int i = 1; i <= columnCount; i++) {
                    System.out.println(i + ": " + meta.getColumnName(i) + " (" + meta.getColumnTypeName(i) + ")");
                }
                
                System.out.println("\n----- ROW DATA -----");
                int row = 0;
                while (rs.next()) {
                    row++;
                    System.out.println("\nROW " + row + ":");
                    for (int i = 1; i <= columnCount; i++) {
                        String colName = meta.getColumnName(i);
                        Object value = rs.getObject(i);
                        System.out.println("  " + colName + " = " + value + 
                                         " [" + (value != null ? value.getClass().getSimpleName() : "NULL") + "]");
                    }
                }
                
                System.out.println("\nTotal rows returned: " + row);
            }
            
            // Now specifically look at bytes field
            System.out.println("\n\n==================== BYTES FIELD SPECIFICALLY ====================");
            sql = "SELECT bytes FROM web WHERE bytes IS NOT NULL LIMIT 10";
            System.out.println("SQL: " + sql);
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                
                int count = 0;
                while (rs.next()) {
                    count++;
                    Object bytes = rs.getObject("bytes");
                    System.out.println("Row " + count + ": bytes = " + bytes + 
                                     " [" + (bytes != null ? bytes.getClass().getName() : "NULL") + "]");
                }
                
                if (count == 0) {
                    System.out.println("NO ROWS WITH NON-NULL BYTES FOUND!");
                }
            }
            
            // Now test CAST directly 
            System.out.println("\n\n==================== TESTING CAST ====================");
            sql = "SELECT bytes, CAST(bytes AS VARCHAR) AS bytes_cast FROM web WHERE bytes = 950 LIMIT 1";
            System.out.println("SQL: " + sql);
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                
                if (rs.next()) {
                    Object bytes = rs.getObject("bytes");
                    Object bytesCast = rs.getObject("bytes_cast");
                    System.out.println("bytes = " + bytes + " [" + (bytes != null ? bytes.getClass().getName() : "NULL") + "]");
                    System.out.println("bytes_cast = " + bytesCast + " [" + (bytesCast != null ? bytesCast.getClass().getName() : "NULL") + "]");
                    
                    if (bytesCast == null) {
                        System.out.println("❌ CAST returned NULL when it should have returned '950'");
                    } else {
                        System.out.println("✅ CAST worked correctly!");
                    }
                } else {
                    System.out.println("No rows found with bytes = 950");
                }
            }
        }
    }
}