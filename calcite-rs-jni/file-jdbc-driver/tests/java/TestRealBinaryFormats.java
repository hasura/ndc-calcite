package com.hasura.file;

import java.sql.*;
import java.io.File;

public class TestRealBinaryFormats {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Real Binary Format Testing ===\n");
        
        // Test Parquet with real binary file
        System.out.println("## PARQUET FORMAT TESTING");
        System.out.println("=".repeat(50));
        testBinaryFormat("tests/data/parquet/sample.parquet", "sample", 
            "Parquet Single File", "parquet");
        testBinaryFormat("tests/data/parquet", "sample", 
            "Parquet Directory", "parquet");
        
        // Test Arrow with real binary file
        System.out.println("\n## ARROW FORMAT TESTING");
        System.out.println("=".repeat(50));
        testBinaryFormat("tests/data/arrow/sample.arrow", "sample", 
            "Arrow Single File", "arrow");
        testBinaryFormat("tests/data/arrow", "sample", 
            "Arrow Directory", "arrow");
        
        System.out.println("\n" + "=".repeat(70));
        System.out.println("REAL BINARY FORMAT TESTING COMPLETE");
        System.out.println("=".repeat(70));
        System.out.println("📊 Testing with actual Parquet and Arrow binary files");
        System.out.println("🔧 Arrow dependencies added to Maven POM");
    }
    
    private static void testBinaryFormat(String path, String expectedTable, 
                                       String description, String format) {
        System.out.println("\n### " + description);
        System.out.println("Path: " + path);
        System.out.println("Format: " + format);
        System.out.println("-".repeat(50));
        
        try {
            String url = "jdbc:file://" + new File(path).getAbsolutePath() + 
                         "?format=" + format;
            System.out.println("Testing URL: " + url);
            
            try (Connection conn = DriverManager.getConnection(url)) {
                System.out.println("Connection: ✅ SUCCESS");
                
                // Get table metadata
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("\nDiscovered tables:");
                
                boolean foundTable = false;
                int tableCount = 0;
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  - " + tableName);
                        tableCount++;
                        
                        if (tableName.equals(expectedTable)) {
                            foundTable = true;
                        }
                        
                        // Try to query the table
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM files." + tableName)) {
                            if (rs.next()) {
                                int count = rs.getInt(1);
                                System.out.println("    Rows: " + count);
                                
                                // Try to get first few rows to show data
                                try (ResultSet rs2 = stmt.executeQuery("SELECT * FROM files." + tableName + " LIMIT 2")) {
                                    ResultSetMetaData rsmd = rs2.getMetaData();
                                    int columnCount = rsmd.getColumnCount();
                                    
                                    System.out.println("    Columns: " + columnCount);
                                    for (int i = 1; i <= columnCount; i++) {
                                        System.out.println("      " + i + ": " + 
                                            rsmd.getColumnName(i) + " (" + rsmd.getColumnTypeName(i) + ")");
                                    }
                                    
                                    System.out.println("    Sample data:");
                                    int rowNum = 1;
                                    while (rs2.next() && rowNum <= 2) {
                                        System.out.print("      Row " + rowNum + ": ");
                                        for (int i = 1; i <= columnCount; i++) {
                                            Object value = rs2.getObject(i);
                                            System.out.print(value + (i < columnCount ? ", " : ""));
                                        }
                                        System.out.println();
                                        rowNum++;
                                    }
                                }
                            }
                        } catch (SQLException e) {
                            System.out.println("    Query failed: " + 
                                e.getMessage().substring(0, Math.min(60, e.getMessage().length())));
                        }
                    }
                }
                
                if (tableCount > 0) {
                    System.out.println("Result: ✅ " + format.toUpperCase() + " FORMAT WORKING (" + tableCount + " tables)");
                } else {
                    System.out.println("Result: ❌ NO TABLES FOUND");
                }
                
            } catch (SQLException e) {
                System.out.println("Connection: ❌ SQL Error");
                System.out.println("Error: " + e.getMessage().substring(0, Math.min(100, e.getMessage().length())));
                System.out.println("Result: ❌ " + format.toUpperCase() + " FORMAT FAILED");
            }
            
        } catch (Exception e) {
            System.out.println("Connection: ❌ " + e.getClass().getSimpleName());
            if (e.getMessage() != null) {
                System.out.println("Error: " + e.getMessage().substring(0, Math.min(100, e.getMessage().length())));
            }
            System.out.println("Result: ❌ " + format.toUpperCase() + " FORMAT FAILED");
        }
    }
}