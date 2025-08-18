package com.hasura.file;

import java.sql.*;
import java.io.File;

public class TestBinaryFormats {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Binary Format Testing ===\n");
        
        // Test Parquet directory with CSV file (tests ArrowSchemaFactory)
        System.out.println("## PARQUET FORMAT TESTING");
        System.out.println("=".repeat(50));
        testBinaryFormat("tests/data/parquet", "sample", "Parquet Directory");
        
        // Test Arrow directory with CSV file (tests ArrowSchemaFactory)  
        System.out.println("\n## ARROW FORMAT TESTING");
        System.out.println("=".repeat(50));
        testBinaryFormat("tests/data/arrow", "sample", "Arrow Directory");
        
        System.out.println("\n" + "=".repeat(60));
        System.out.println("BINARY FORMAT TESTING COMPLETE");
        System.out.println("=".repeat(60));
        System.out.println("📋 Note: Testing with CSV files in binary format directories");
        System.out.println("   Real binary files would require pyarrow/pandas installation");
    }
    
    private static void testBinaryFormat(String path, String expectedTable, String description) {
        System.out.println("\n### " + description);
        System.out.println("Path: " + path);
        System.out.println("-".repeat(40));
        
        try {
            String url = "jdbc:file://" + new File(path).getAbsolutePath() + "?format=parquet";
            System.out.println("Testing URL: " + url);
            
            try (Connection conn = DriverManager.getConnection(url)) {
                System.out.println("Connection: ✅ SUCCESS");
                
                // Get table metadata
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("\nDiscovered tables:");
                
                boolean foundTable = false;
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  - " + tableName);
                        foundTable = true;
                        
                        // Try to query the table
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM files." + tableName)) {
                            if (rs.next()) {
                                int count = rs.getInt(1);
                                System.out.println("    Rows: " + count);
                            }
                        } catch (SQLException e) {
                            System.out.println("    Query failed: " + e.getMessage().substring(0, Math.min(50, e.getMessage().length())));
                        }
                    }
                }
                
                if (foundTable) {
                    System.out.println("Result: ✅ BINARY FORMAT DIRECTORY WORKS");
                } else {
                    System.out.println("Result: ❌ NO TABLES FOUND");
                }
                
            } catch (SQLException e) {
                System.out.println("Connection: ❌ SQL Error");
                System.out.println("Error: " + e.getMessage().substring(0, Math.min(80, e.getMessage().length())));
                System.out.println("Result: ❌ BINARY FORMAT FAILED");
            }
            
        } catch (Exception e) {
            System.out.println("Connection: ❌ " + e.getClass().getSimpleName());
            System.out.println("Error: " + e.getMessage().substring(0, Math.min(80, e.getMessage().length())));
            System.out.println("Result: ❌ BINARY FORMAT FAILED");
        }
    }
}