package com.hasura.file;

import java.sql.*;
import java.io.File;

public class TestAllFormatsAndTransports {
    public static void main(String[] args) throws Exception {
        System.out.println("=== COMPREHENSIVE File Format and Transport Testing ===\n");
        
        int totalTests = 0;
        int passedTests = 0;
        int failedTests = 0;
        
        // Test all local file formats
        System.out.println("## LOCAL FILE FORMATS");
        System.out.println("=".repeat(50));
        
        FormatTest[] localFormats = {
            new FormatTest("CSV", "tests/data/csv", "tests/data/csv/sales.csv", "sales"),
            new FormatTest("TSV", "tests/data/tsv", "tests/data/tsv/inventory.tsv", "inventory"), 
            new FormatTest("JSON", "tests/data/json", "tests/data/json/users.json", "users"),
            new FormatTest("", "tests/data/", "tests/data//logs.jsonl", "logs"),
            new FormatTest("YAML", "tests/data/yaml", "tests/data/yaml/config.yaml", "config"),
            new FormatTest("XLSX", "tests/data/xlsx", "tests/data/xlsx/company_data.xlsx", "company_data"),
            new FormatTest("HTML", "tests/data/html", "tests/data/html/report.html", "report"),
            new FormatTest("PARQUET", "tests/data/parquet", "tests/data/parquet/sample.parquet", "sample"),
            new FormatTest("ARROW", "tests/data/arrow", "tests/data/arrow/sample.arrow", "sample")
        };
        
        for (FormatTest test : localFormats) {
            System.out.println("\n### Testing " + test.format + " Format");
            System.out.println("-".repeat(30));
            
            // Test directory approach (if directory exists)
            boolean dirResult = false;
            File dirFile = new File(test.dirPath);
            if (dirFile.exists() && dirFile.isDirectory()) {
                dirResult = testConnection(test.dirPath, test.expectedTable, "Directory");
                totalTests++;
                if (dirResult) passedTests++; else failedTests++;
            } else {
                System.out.println("  Directory: ⏭️ SKIPPED (no test data)");
            }
            
            // Test single file approach (if file exists)
            boolean fileResult = false;
            File singleFile = new File(test.filePath);  
            if (singleFile.exists()) {
                fileResult = testConnection(test.filePath, test.expectedTable, "Single File");
                totalTests++;
                if (fileResult) passedTests++; else failedTests++;
            } else {
                System.out.println("  Single File: ⏭️ SKIPPED (no test data)");
            }
            
            // Overall result
            if (dirFile.exists() || singleFile.exists()) {
                boolean hasAnySuccess = dirResult || fileResult;
                System.out.println("  Overall: " + (hasAnySuccess ? "✅ PARTIAL/FULL" : "❌ FAILED"));
            } else {
                System.out.println("  Overall: ⏭️ NO TEST DATA");
            }
        }
        
        // Test HTTP transports
        System.out.println("\n\n## HTTP TRANSPORT TESTING");
        System.out.println("=".repeat(50));
        
        HttpTest[] httpTests = {
            new HttpTest("CSV over HTTP", 
                "https://raw.githubusercontent.com/plotly/datasets/master/tips.csv", 
                "tips"),
            new HttpTest("JSON over HTTP", 
                "https://jsonplaceholder.typicode.com/users", 
                "users"),
            new HttpTest("CSV from GitHub", 
                "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv", 
                "titanic")
        };
        
        for (HttpTest test : httpTests) {
            System.out.println("\n### Testing " + test.name);
            System.out.println("-".repeat(30));
            
            boolean result = testHttpConnection(test.url, test.expectedTable);
            totalTests++;
            if (result) {
                passedTests++;
                System.out.println("  Result: ✅ SUCCESS");
            } else {
                failedTests++;
                System.out.println("  Result: ❌ FAILED");
            }
        }
        
        // Test S3 transport (will likely fail due to credentials, but shows capability)
        System.out.println("\n\n## S3 TRANSPORT TESTING");
        System.out.println("=".repeat(50));
        
        System.out.println("\n### Testing S3 CSV Access");
        System.out.println("-".repeat(30));
        
        // Example S3 URL (will fail without proper credentials/setup)
        String s3Url = "s3://example-bucket/data.csv";
        boolean s3Result = testS3Connection(s3Url, "data");
        totalTests++;
        if (s3Result) {
            passedTests++;  
            System.out.println("  Result: ✅ SUCCESS");
        } else {
            failedTests++;
            System.out.println("  Result: ❌ FAILED (expected - no credentials)");
        }
        
        // Final summary
        System.out.println("\n" + "=".repeat(60));
        System.out.println("FINAL SUMMARY");
        System.out.println("=".repeat(60));
        System.out.println("Total Tests: " + totalTests);
        System.out.println("Passed: " + passedTests);
        System.out.println("Failed: " + failedTests);
        System.out.println("Success Rate: " + String.format("%.1f%%", (passedTests * 100.0 / totalTests)));
        
        System.out.println("\n📋 KEY FINDINGS:");
        System.out.println("- Working formats can be identified from success patterns");
        System.out.println("- Failed formats need alternative configuration OR code fix");  
        System.out.println("- Transport capabilities tested (HTTP/S3)");
        System.out.println("- Missing test data files noted for future creation");
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
                        if (tableName.equals(expectedTable) || 
                            (expectedTable.equals("users") && tableName.equals("customers"))) {
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
                    System.out.println("  " + testType + ": ❌ (query failed: " + 
                        e.getMessage().substring(0, Math.min(50, e.getMessage().length())) + "...)");
                    return false;
                }
            }
        } catch (Exception e) {
            System.out.println("  " + testType + ": ❌ (connection failed: " + 
                e.getClass().getSimpleName() + ")");
            return false;
        }
    }
    
    private static boolean testHttpConnection(String httpUrl, String expectedTable) {
        try {
            String url = "jdbc:file://" + httpUrl;
            
            try (Connection conn = DriverManager.getConnection(url)) {
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
                    System.out.println("  HTTP: ❌ (table not found)");
                    return false;
                }
                
                // Try simple query
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT * FROM files." + expectedTable + " LIMIT 1")) {
                    
                    boolean hasData = rs.next();
                    System.out.println("  HTTP: " + (hasData ? "✅" : "❌ (no data)"));
                    return hasData;
                    
                } catch (SQLException e) {
                    System.out.println("  HTTP: ❌ (query failed: " + 
                        e.getMessage().substring(0, Math.min(50, e.getMessage().length())) + "...)");
                    return false;
                }
            }
        } catch (Exception e) {
            System.out.println("  HTTP: ❌ (" + e.getClass().getSimpleName() + ")");
            return false;
        }
    }
    
    private static boolean testS3Connection(String s3Url, String expectedTable) {
        try {
            String url = "jdbc:file://" + s3Url;
            
            try (Connection conn = DriverManager.getConnection(url)) {
                // If we get here, connection worked
                DatabaseMetaData meta = conn.getMetaData();
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    boolean hasAnyTable = tables.next();
                    System.out.println("  S3: " + (hasAnyTable ? "✅" : "❌ (no tables)"));
                    return hasAnyTable;
                }
            }
        } catch (Exception e) {
            System.out.println("  S3: ❌ (" + e.getClass().getSimpleName() + " - " + 
                e.getMessage().substring(0, Math.min(30, e.getMessage().length())) + "...)");
            return false;
        }
    }
    
    private static class FormatTest {
        String format;
        String dirPath;
        String filePath;
        String expectedTable;
        
        FormatTest(String format, String dirPath, String filePath, String expectedTable) {
            this.format = format;
            this.dirPath = dirPath;
            this.filePath = filePath;
            this.expectedTable = expectedTable;
        }
    }
    
    private static class HttpTest {
        String name;
        String url;
        String expectedTable;
        
        HttpTest(String name, String url, String expectedTable) {
            this.name = name;
            this.url = url;
            this.expectedTable = expectedTable;
        }
    }
}