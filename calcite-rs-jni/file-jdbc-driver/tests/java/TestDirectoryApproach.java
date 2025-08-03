package com.hasura.file;

import java.sql.*;
import java.io.File;

public class TestDirectoryApproach {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Testing Directory Approach for Problematic Formats ===\n");
        
        // Test directory approach for the formats that failed with single file
        String[][] tests = {
            {"", "tests/data/", "logs"},
            {"XLSX", "tests/data/xlsx", "company_data"}
        };
        
        for (String[] test : tests) {
            String format = test[0];
            String dirPath = test[1];
            String expectedTable = test[2];
            
            System.out.println("### Testing " + format + " Directory Approach");
            System.out.println("Directory: " + dirPath);
            System.out.println("-".repeat(40));
            
            try {
                String url = "jdbc:file://" + new File(dirPath).getAbsolutePath();
                
                try (Connection conn = DriverManager.getConnection(url)) {
                    System.out.println("  Connection: ✅ SUCCESS");
                    
                    // List all tables found
                    DatabaseMetaData meta = conn.getMetaData();
                    boolean foundAnyTable = false;
                    boolean foundExpectedTable = false;
                    
                    try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                        System.out.println("  Tables found:");
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");
                            System.out.println("    - " + tableName);
                            foundAnyTable = true;
                            
                            if (tableName.equals(expectedTable)) {
                                foundExpectedTable = true;
                            }
                        }
                    }
                    
                    if (!foundAnyTable) {
                        System.out.println("    ❌ No tables found");
                        continue;
                    }
                    
                    if (!foundExpectedTable) {
                        System.out.println("  Expected table '" + expectedTable + "' not found");
                        continue;
                    }
                    
                    // Try to query the expected table
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT * FROM files." + expectedTable + " LIMIT 1")) {
                        
                        if (rs.next()) {
                            System.out.println("  Query: ✅ SUCCESS");
                            
                            ResultSetMetaData rsmd = rs.getMetaData();
                            int columnCount = rsmd.getColumnCount();
                            System.out.println("  Columns: " + columnCount);
                            
                            System.out.print("  Sample data: ");
                            for (int i = 1; i <= columnCount; i++) {
                                Object value = rs.getObject(i);
                                System.out.print(value + (i < columnCount ? ", " : ""));
                            }
                            System.out.println();
                            
                            System.out.println("  Result: ✅ " + format + " WORKS WITH DIRECTORY APPROACH!");
                            
                        } else {
                            System.out.println("  Query: ❌ No data returned");
                        }
                        
                    } catch (SQLException e) {
                        System.out.println("  Query: ❌ FAILED");
                        System.out.println("  Error: " + e.getMessage().substring(0, Math.min(80, e.getMessage().length())));
                    }
                    
                } catch (SQLException e) {
                    System.out.println("  Connection: ❌ FAILED");
                    System.out.println("  Error: " + e.getMessage().substring(0, Math.min(80, e.getMessage().length())));
                }
                
            } catch (Exception e) {
                System.out.println("  Test: ❌ FAILED");
                System.out.println("  Error: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            
            System.out.println();
        }
        
        // Also test if YAML directory approach works
        System.out.println("### Testing YAML Directory Approach (for completeness)");
        System.out.println("Directory: tests/data/yaml");
        System.out.println("-".repeat(40));
        
        try {
            String url = "jdbc:file://" + new File("tests/data/yaml").getAbsolutePath();
            
            try (Connection conn = DriverManager.getConnection(url)) {
                DatabaseMetaData meta = conn.getMetaData();
                
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    System.out.println("  Tables found:");
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("    - " + tableName);
                        
                        // Test query
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery("SELECT * FROM files." + tableName + " LIMIT 1")) {
                            
                            if (rs.next()) {
                                System.out.println("      Query: ✅ Works");
                            }
                        } catch (SQLException e) {
                            System.out.println("      Query: ❌ Failed");
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("  YAML directory test failed: " + e.getMessage());
        }
    }
}