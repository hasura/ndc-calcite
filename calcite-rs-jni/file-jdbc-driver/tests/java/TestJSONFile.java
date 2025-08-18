package com.hasura.file;

import java.sql.*;
import java.io.File;

public class TestJSONFile {
    public static void main(String[] args) {
        System.out.println("=== Testing JSON File with Shaded Jackson ===\n");
        
        try {
            String url = "jdbc:file://" + new File("tests/data/json/users.json").getAbsolutePath();
            System.out.println("URL: " + url);
            
            try (Connection conn = DriverManager.getConnection(url)) {
                System.out.println("✓ Connection successful");
                
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("\nTables found:");
                
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  - " + tableName);
                        
                        // Test query
                        System.out.println("\nQuerying " + tableName + ":");
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery("SELECT * FROM files." + tableName + " LIMIT 2")) {
                            
                            ResultSetMetaData rsmd = rs.getMetaData();
                            int columnCount = rsmd.getColumnCount();
                            
                            // Print headers
                            for (int i = 1; i <= columnCount; i++) {
                                System.out.print(rsmd.getColumnName(i) + "\t");
                            }
                            System.out.println();
                            
                            // Print data
                            while (rs.next()) {
                                for (int i = 1; i <= columnCount; i++) {
                                    System.out.print(rs.getString(i) + "\t");
                                }
                                System.out.println();
                            }
                            System.out.println("✓ JSON parsing successful with shaded Jackson!");
                            
                        } catch (SQLException e) {
                            System.out.println("✗ Query failed: " + e.getMessage());
                            if (e.getMessage().contains("getNumberTypeFP")) {
                                System.out.println("  → Jackson shading did not resolve the conflict");
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("✗ Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}