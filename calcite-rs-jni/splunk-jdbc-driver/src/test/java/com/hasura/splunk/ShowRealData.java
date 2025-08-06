package com.hasura.splunk;

import org.junit.Test;
import java.sql.*;
import java.util.Properties;

public class ShowRealData {
    
    @Test
    public void showWebData() throws Exception {
        // Use the same connection setup as the working tests
        Properties props = new Properties();
        props.setProperty("model", "cim");
        String url = "jdbc:splunk://localhost:8089/test";
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            System.out.println("\n==================== WEB TABLE DATA ====================");
            
            // Simple query - just get the data
            String sql = "SELECT * FROM web LIMIT 5";
            
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
        }
    }
}