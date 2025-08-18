package com.hasura.file;

import java.sql.*;
import java.io.File;

public class TestJacksonFix {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Testing Jackson Conflict Fix ===\n");
        
        // Test the problematic formats that had Jackson conflicts
        String[][] tests = {
            {"", "tests/data//logs.", "logs"},
            {"YAML", "tests/data/yaml/config.yaml", "config"},
            {"XLSX", "tests/data/xlsx/company_data.xlsx", "company_data"}
        };
        
        int passed = 0;
        int failed = 0;
        
        for (String[] test : tests) {
            String format = test[0];
            String filePath = test[1];
            String expectedTable = test[2];
            
            System.out.println("### Testing " + format + " Format");
            System.out.println("File: " + filePath);
            System.out.println("-".repeat(40));
            
            try {
                String url = "jdbc:file://" + new File(filePath).getAbsolutePath();
                
                try (Connection conn = DriverManager.getConnection(url)) {
                    System.out.println("  Connection: ✅ SUCCESS");
                    
                    // Check table exists
                    DatabaseMetaData meta = conn.getMetaData();
                    boolean tableFound = false;
                    
                    try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");
                            if (tableName.equals(expectedTable)) {
                                tableFound = true;
                                System.out.println("  Table Discovery: ✅ Found '" + tableName + "'");
                                break;
                            }
                        }
                    }
                    
                    if (!tableFound) {
                        System.out.println("  Table Discovery: ❌ Table not found");
                        failed++;
                        continue;
                    }
                    
                    // Try simple query - this is where Jackson conflict occurred
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT * FROM files." + expectedTable + " LIMIT 1")) {
                        
                        if (rs.next()) {
                            System.out.println("  Query Execution: ✅ SUCCESS");
                            
                            // Show some data
                            ResultSetMetaData rsmd = rs.getMetaData();
                            int columnCount = rsmd.getColumnCount();
                            System.out.println("  Columns: " + columnCount);
                            
                            System.out.print("  Data: ");
                            for (int i = 1; i <= columnCount; i++) {
                                Object value = rs.getObject(i);
                                System.out.print(value + (i < columnCount ? ", " : ""));
                            }
                            System.out.println();
                            
                            System.out.println("  Result: ✅ " + format + " JACKSON CONFLICT FIXED!");
                            passed++;
                        } else {
                            System.out.println("  Query Execution: ❌ No data returned");
                            failed++;
                        }
                        
                    } catch (SQLException e) {
                        System.out.println("  Query Execution: ❌ FAILED");
                        System.out.println("  Error: " + e.getMessage().substring(0, Math.min(60, e.getMessage().length())));
                        
                        if (e.getMessage().contains("getNumberTypeFP")) {
                            System.out.println("  Cause: ❌ JACKSON CONFLICT STILL PRESENT");
                        } else {
                            System.out.println("  Cause: Different error (not Jackson)");
                        }
                        failed++;
                    }
                    
                } catch (SQLException e) {
                    System.out.println("  Connection: ❌ FAILED");
                    System.out.println("  Error: " + e.getMessage().substring(0, Math.min(60, e.getMessage().length())));
                    failed++;
                }
                
            } catch (Exception e) {
                System.out.println("  Test: ❌ FAILED");
                System.out.println("  Error: " + e.getClass().getSimpleName());
                failed++;
            }
            
            System.out.println();
        }
        
        System.out.println("=" .repeat(50));
        System.out.println("JACKSON FIX TEST RESULTS");
        System.out.println("=" .repeat(50));
        System.out.println("Passed: " + passed);
        System.out.println("Failed: " + failed);
        
        if (passed == 3) {
            System.out.println("🎉 SUCCESS: All Jackson conflicts resolved!");
        } else if (passed > 0) {
            System.out.println("⚠️ PARTIAL: Some Jackson conflicts resolved");
        } else {
            System.out.println("❌ FAILED: Jackson conflicts still present");
        }
    }
}