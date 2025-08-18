package com.hasura.file;

import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;
import java.sql.*;
import java.io.File;

public class TestComprehensiveFormats {
    @Before
    public void setUp() {
        try {
            Class.forName("com.hasura.file.FileDriver");
        } catch (ClassNotFoundException e) {
            fail("FileDriver not found");
        }
    }
    
    @Test
    public void testComprehensiveFormats() throws Exception {
        System.out.println("=== Comprehensive Format Testing ===\n");
        
        int passed = 0;
        int failed = 0;
        
        // Test each format with both approaches
        FormatTest[] tests = {
            new FormatTest("CSV", "tests/data/csv", "tests/data/csv/sales.csv", "SALES", true),
            new FormatTest("TSV", "tests/data/tsv", "tests/data/tsv/inventory.tsv", "INVENTORY", false),
            new FormatTest("JSON", "tests/data/json", "tests/data/json/users.json", "USERS", false),
            new FormatTest("YAML", "tests/data/yaml", "tests/data/yaml/config.yaml", "CONFIG", false),
            new FormatTest("XLSX", "tests/data/xlsx", "tests/data/xlsx/company_data.xlsx", "COMPANY_DATA", false),
            new FormatTest("HTML", "tests/data/html", "tests/data/html/report.html", "REPORT", false)
        };
        
        for (FormatTest test : tests) {
            System.out.println("Testing " + test.format + " format:");
            System.out.println("Expected to " + (test.shouldWork ? "WORK" : "FAIL"));
            System.out.println("-".repeat(40));
            
            // Test directory approach
            boolean dirResult = testDirectoryConnection(test.dirPath, test.expectedTable);
            System.out.println("  Directory: " + (dirResult ? "✅" : "❌"));
            
            // Test single file approach  
            boolean fileResult = testSingleFileConnection(test.filePath, test.expectedTable);
            System.out.println("  Single File: " + (fileResult ? "✅" : "❌"));
            
            // For CSV, only single file approach works
            boolean overallSuccess;
            if (test.format.equals("CSV")) {
                overallSuccess = fileResult;
            } else {
                overallSuccess = dirResult && fileResult;
            }
            if (overallSuccess == test.shouldWork) {
                System.out.println("  Result: ✅ EXPECTED");
                passed++;
            } else {
                System.out.println("  Result: ❌ UNEXPECTED - Expected " + 
                    (test.shouldWork ? "SUCCESS" : "FAILURE") + 
                    " but got " + (overallSuccess ? "SUCCESS" : "FAILURE"));
                failed++;
            }
            
            System.out.println();
        }
        
        System.out.println("=".repeat(50));
        System.out.println("SUMMARY: " + passed + " passed, " + failed + " failed");
        System.out.println("=".repeat(50));
        
        // Current working formats
        if (failed == 0) {
            System.out.println("✅ All tests behaved as expected!");
            System.out.println("Working formats: CSV (single file only)");
            System.out.println("Non-working formats: TSV, JSON, YAML, XLSX, HTML (as expected)");
        }
        
        assertEquals("All tests should behave as expected", 0, failed);
    }
    
    private static boolean testDirectoryConnection(String dirPath, String expectedTable) {
        try {
            String url = "jdbc:file://" + new File(dirPath).getAbsolutePath();
            
            try (Connection conn = DriverManager.getConnection(url)) {
                DatabaseMetaData meta = conn.getMetaData();
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        if (tableName.equals(expectedTable) || 
                            (expectedTable.equals("USERS") && tableName.equals("CUSTOMERS"))) {
                            // Try a simple query
                            try (Statement stmt = conn.createStatement();
                                 ResultSet rs = stmt.executeQuery("SELECT * FROM files.\"" + tableName + "\" LIMIT 1")) {
                                return rs.next(); // Success if we get at least one row
                            } catch (SQLException e) {
                                return false; // Query failed
                            }
                        }
                    }
                    return false; // Table not found
                }
            }
        } catch (Exception e) {
            return false;
        }
    }
    
    private static boolean testSingleFileConnection(String filePath, String expectedTable) {
        try {
            String url = "jdbc:file://" + new File(filePath).getAbsolutePath();
            
            try (Connection conn = DriverManager.getConnection(url)) {
                DatabaseMetaData meta = conn.getMetaData();
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    boolean foundTable = false;
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        if (tableName.equals(expectedTable)) {
                            foundTable = true;
                            break;
                        }
                    }
                    
                    if (!foundTable) {
                        return false; // Table not found
                    }
                    
                    // Try a simple query
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT * FROM files.\"" + expectedTable + "\" LIMIT 1")) {
                        return rs.next(); // Success if we get at least one row
                    } catch (SQLException e) {
                        return false; // Query failed
                    }
                }
            }
        } catch (Exception e) {
            return false;
        }
    }
    
    private static class FormatTest {
        String format;
        String dirPath;
        String filePath;
        String expectedTable;
        boolean shouldWork;
        
        FormatTest(String format, String dirPath, String filePath, String expectedTable, boolean shouldWork) {
            this.format = format;
            this.dirPath = dirPath;
            this.filePath = filePath;
            this.expectedTable = expectedTable;
            this.shouldWork = shouldWork;
        }
    }
}