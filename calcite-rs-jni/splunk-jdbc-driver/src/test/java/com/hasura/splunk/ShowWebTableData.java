package com.hasura.splunk;

import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import java.sql.*;
import java.util.Properties;

public class ShowWebTableData {
    private Connection connection;
    
    @BeforeClass
    public static void setUpClass() {
        try {
            Class.forName("com.hasura.splunk.SplunkDriver");
        } catch (ClassNotFoundException e) {
            System.err.println("SplunkDriver not found: " + e.getMessage());
        }
    }
    
    @Before
    public void setUp() throws Exception {
        Properties props = new Properties();
        props.setProperty("model", "cim");
        String url = "jdbc:splunk://localhost:8089/test";
        connection = DriverManager.getConnection(url, props);
    }
    
    @Test
    public void showWebTableData() throws SQLException {
        System.out.println("\n==================== WEB TABLE DATA (No CAST) ====================");
        
        // Query multiple fields from web table
        String sql = "SELECT _time, host, bytes, bytes_in, bytes_out, status, uri, src_ip, dest_ip FROM web LIMIT 10";
        System.out.println("SQL: " + sql);
        System.out.println();
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            
            // Print column headers
            System.out.println("Columns:");
            for (int i = 1; i <= columnCount; i++) {
                System.out.println(String.format("  %d: %-15s (%s)", i, meta.getColumnName(i), meta.getColumnTypeName(i)));
            }
            
            System.out.println("\n----- DATA -----");
            int row = 0;
            while (rs.next()) {
                row++;
                System.out.println("\nRow " + row + ":");
                
                // Print each field with its value and type
                for (int i = 1; i <= columnCount; i++) {
                    String colName = meta.getColumnName(i);
                    Object value = rs.getObject(i);
                    String valueStr = (value != null) ? value.toString() : "NULL";
                    String typeStr = (value != null) ? value.getClass().getSimpleName() : "null";
                    
                    System.out.println(String.format("  %-15s = %-30s [%s]", colName, valueStr, typeStr));
                }
            }
            
            System.out.println("\n\nTotal rows: " + row);
        }
        
        // Now specifically look for non-null bytes values
        System.out.println("\n\n==================== LOOKING FOR NON-NULL BYTES ====================");
        sql = "SELECT bytes, status, uri FROM web WHERE bytes IS NOT NULL LIMIT 10";
        System.out.println("SQL: " + sql);
        System.out.println();
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            int count = 0;
            while (rs.next()) {
                count++;
                Object bytes = rs.getObject("bytes");
                Object status = rs.getObject("status");
                Object uri = rs.getObject("uri");
                
                System.out.println(String.format("Row %d: bytes=%-10s status=%-5s uri=%s", 
                    count, bytes, status, uri));
            }
            
            if (count == 0) {
                System.out.println("No rows found with non-null bytes!");
            } else {
                System.out.println("\nFound " + count + " rows with non-null bytes");
            }
        }
    }
}