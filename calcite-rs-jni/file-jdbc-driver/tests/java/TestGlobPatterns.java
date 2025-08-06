package com.hasura.file;

import java.sql.*;
import java.io.File;

public class TestGlobPatterns {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Glob Pattern Testing ===\n");
        
        // Test different glob patterns
        String basePath = "tests/data";
        
        testGlobPattern(basePath, "**/*.csv", "All CSV files recursively");
        testGlobPattern(basePath, "csv/*.csv", "CSV files in csv directory");
        testGlobPattern(basePath, "nested/**/*.csv", "CSV files in nested subdirectories");
        testGlobPattern(basePath, "*/sales.csv", "sales.csv in any direct subdirectory");
        testGlobPattern(basePath, "**/*.json", "All JSON files recursively");
        
        System.out.println("\n" + "=".repeat(60));
        System.out.println("GLOB PATTERN TESTING COMPLETE");
        System.out.println("=".repeat(60));
    }
    
    private static void testGlobPattern(String basePath, String pattern, String description) {
        System.out.println("\n### " + description);
        System.out.println("Pattern: " + pattern);
        System.out.println("-".repeat(50));
        
        try {
            // Use the glob URL format as mentioned in FileDriver
            String url = "jdbc:file://multi?path=" + new File(basePath).getAbsolutePath() + 
                         "&glob=" + pattern;
            
            System.out.println("Testing URL: " + url);
            
            try (Connection conn = DriverManager.getConnection(url)) {
                System.out.println("Connection: ✅ SUCCESS");
                
                // Get discovered tables
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("\nDiscovered tables:");
                
                int tableCount = 0;
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  - " + tableName);
                        tableCount++;
                        
                        // Try to query the table
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM files." + tableName)) {
                            if (rs.next()) {
                                int count = rs.getInt(1);
                                System.out.println("    Rows: " + count);
                            }
                        } catch (SQLException e) {
                            System.out.println("    Query failed: " + e.getMessage().substring(0, Math.min(40, e.getMessage().length())));
                        }
                    }
                }
                
                System.out.println("Total tables found: " + tableCount);
                System.out.println("Result: " + (tableCount > 0 ? "✅ GLOB WORKING" : "❌ NO MATCHES"));
                
            } catch (SQLException e) {
                System.out.println("Connection: ❌ SQL Error");
                System.out.println("Error: " + e.getMessage().substring(0, Math.min(60, e.getMessage().length())));
                System.out.println("Result: ❌ GLOB FAILED");
            }
            
        } catch (Exception e) {
            System.out.println("Connection: ❌ " + e.getClass().getSimpleName());
            System.out.println("Error: " + e.getMessage().substring(0, Math.min(60, e.getMessage().length())));
            System.out.println("Result: ❌ GLOB FAILED");
        }
    }
}