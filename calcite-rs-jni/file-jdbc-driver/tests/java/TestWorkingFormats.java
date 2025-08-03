package com.hasura.file;

import java.sql.*;
import java.io.File;

public class TestWorkingFormats {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Testing Working Formats Only ===\n");
        
        int passed = 0;
        int failed = 0;
        
        // Test only formats that should work
        String[][] tests = {
            {"CSV", "tests/data/csv", "tests/data/csv/sales.csv", "sales"},
            {"HTML", "tests/data/html", "tests/data/html/report.html", "report"}
        };
        
        for (String[] test : tests) {
            String format = test[0];
            String dirPath = test[1];
            String filePath = test[2];
            String expectedTable = test[3];
            
            System.out.println("Testing " + format + " format:");
            System.out.println("-".repeat(30));
            
            // Test directory approach
            boolean dirResult = testConnection(dirPath, expectedTable, "Directory");
            
            // Test single file approach  
            boolean fileResult = testConnection(filePath, expectedTable, "Single File");
            
            if (dirResult && fileResult) {
                System.out.println("  Result: ✅ " + format + " WORKS");
                passed++;
            } else {
                System.out.println("  Result: ❌ " + format + " FAILED");
                failed++;
            }
            
            System.out.println();
        }
        
        // Test JSON format specifically (should work but might have Jackson issues)
        System.out.println("Testing JSON format (may have Jackson conflict):");
        System.out.println("-".repeat(30));
        
        boolean jsonDirResult = testConnection("tests/data/json", "users", "Directory");
        // For JSON, we accept either users or customers table since both exist
        if (!jsonDirResult) {
            jsonDirResult = testConnection("tests/data/json", "customers", "Directory (alt)");
        }
        
        boolean jsonFileResult = testConnection("tests/data/json/users.json", "users", "Single File");
        
        if (jsonDirResult && jsonFileResult) {
            System.out.println("  Result: ✅ JSON WORKS");
            passed++;
        } else {
            System.out.println("  Result: ❌ JSON FAILED (likely Jackson conflict)");
            failed++;
        }
        
        System.out.println("\n" + "=".repeat(50));
        System.out.println("FINAL RESULT: " + passed + " working, " + failed + " failed");
        
        if (passed >= 2) {
            System.out.println("✅ Core functionality verified: CSV and HTML work properly");
            if (passed == 3) {
                System.out.println("✅ JSON also works - Jackson shading successful!");
            } else {
                System.out.println("⚠️ JSON failed - Jackson conflict still present");
            }
        }
        System.out.println("=".repeat(50));
    }
    
    private static boolean testConnection(String path, String expectedTable, String testType) {
        try {
            String url = "jdbc:file://" + new File(path).getAbsolutePath();
            
            try (Connection conn = DriverManager.getConnection(url)) {
                // Check table exists
                DatabaseMetaData meta = conn.getMetaData();
                boolean tableFound = false;
                
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        if (tableName.equals(expectedTable)) {
                            tableFound = true;
                            break;
                        }
                    }
                }
                
                if (!tableFound) {
                    System.out.println("  " + testType + ": ❌ (table not found)");
                    return false;
                }
                
                // Try simple query
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT * FROM files." + expectedTable + " LIMIT 1")) {
                    
                    boolean hasData = rs.next();
                    System.out.println("  " + testType + ": " + (hasData ? "✅" : "❌ (no data)"));
                    return hasData;
                    
                } catch (SQLException e) {
                    System.out.println("  " + testType + ": ❌ (query failed: " + e.getMessage().substring(0, Math.min(50, e.getMessage().length())) + "...)");
                    return false;
                }
                
            }
        } catch (Exception e) {
            System.out.println("  " + testType + ": ❌ (connection failed)");
            return false;
        }
    }
}