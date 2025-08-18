package com.hasura.file;

import java.sql.*;
import java.io.File;
import java.nio.file.*;

public class TestTSVApproaches {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Testing TSV File: Directory vs Single File Approach ===\n");
        
        // Test with original .tsv extension
        System.out.println("TEST 1: Original .tsv extension");
        System.out.println("================================");
        
        System.out.println("\nAPPROACH 1A: Directory containing .tsv file");
        testDirectoryApproach("tests/data/tsv", "inventory");
        
        System.out.println("\nAPPROACH 1B: Single .tsv file");
        testSingleFileApproach("tests/data/tsv/inventory.tsv", "inventory");
        
        // Test with renamed .csv extension
        System.out.println("\n\nTEST 2: Renamed to .csv extension");
        System.out.println("===================================");
        
        // Copy TSV file with CSV extension
        Files.copy(
            Paths.get("tests/data/tsv/inventory.tsv"),
            Paths.get("tests/data/tsv/inventory_renamed.csv"),
            StandardCopyOption.REPLACE_EXISTING
        );
        
        System.out.println("\nAPPROACH 2A: Directory containing renamed .csv file");
        testDirectoryApproach("tests/data/tsv", "inventory_renamed");
        
        System.out.println("\nAPPROACH 2B: Single renamed .csv file");
        testSingleFileApproach("tests/data/tsv/inventory_renamed.csv", "inventory_renamed");
        
        // Test with format override
        System.out.println("\n\nTEST 3: Using format=csv parameter");
        System.out.println("====================================");
        testWithFormatOverride();
        
        // Clean up
        Files.deleteIfExists(Paths.get("tests/data/tsv/inventory_renamed.csv"));
        
        System.out.println("\n\nCONCLUSION:");
        System.out.println("- TSV files with .tsv extension: NOT recognized");
        System.out.println("- TSV files renamed to .csv: Work perfectly");
        System.out.println("- Format override: Works for directories but not single files");
    }
    
    private static void testDirectoryApproach(String dirPath, String expectedTable) {
        try {
            String url = "jdbc:file://" + new File(dirPath).getAbsolutePath();
            System.out.println("URL: " + url);
            
            try (Connection conn = DriverManager.getConnection(url)) {
                System.out.println("Connected successfully!");
                
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("Tables found:");
                boolean foundExpected = false;
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  - " + tableName);
                        if (tableName.equals(expectedTable)) {
                            foundExpected = true;
                        }
                    }
                }
                
                if (foundExpected) {
                    testTSVQuery(conn, expectedTable);
                } else {
                    System.out.println("✗ Expected table '" + expectedTable + "' not found");
                }
            }
        } catch (Exception e) {
            System.out.println("✗ Connection failed: " + e.getMessage());
        }
    }
    
    private static void testSingleFileApproach(String filePath, String expectedTable) {
        try {
            String url = "jdbc:file://" + new File(filePath).getAbsolutePath();
            System.out.println("URL: " + url);
            
            try (Connection conn = DriverManager.getConnection(url)) {
                System.out.println("Connected successfully!");
                
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("Tables found:");
                boolean foundExpected = false;
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  - " + tableName);
                        if (tableName.equals(expectedTable)) {
                            foundExpected = true;
                        }
                    }
                }
                
                if (foundExpected) {
                    testTSVQuery(conn, expectedTable);
                } else {
                    System.out.println("✗ Expected table '" + expectedTable + "' not found");
                }
            }
        } catch (Exception e) {
            System.out.println("✗ Connection failed: " + e.getMessage());
        }
    }
    
    private static void testWithFormatOverride() {
        System.out.println("\nDirectory with format=csv:");
        try {
            String url = "jdbc:file://" + new File("tests/data/tsv").getAbsolutePath() + "?format=csv";
            System.out.println("URL: " + url);
            
            try (Connection conn = DriverManager.getConnection(url)) {
                System.out.println("Connected successfully!");
                
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("Tables found:");
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    while (tables.next()) {
                        System.out.println("  - " + tables.getString("TABLE_NAME"));
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("✗ Connection failed: " + e.getMessage());
        }
        
        System.out.println("\nSingle file with format=csv:");
        try {
            String url = "jdbc:file://" + new File("tests/data/tsv/inventory.tsv").getAbsolutePath() + "?format=csv";
            System.out.println("URL: " + url);
            
            try (Connection conn = DriverManager.getConnection(url)) {
                System.out.println("Connected successfully!");
                
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("Tables found:");
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    while (tables.next()) {
                        System.out.println("  - " + tables.getString("TABLE_NAME"));
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("✗ Connection failed: " + e.getMessage());
        }
    }
    
    private static void testTSVQuery(Connection conn, String tableName) {
        System.out.println("Querying table " + tableName + ":");
        
        // First check what columns exist
        try {
            DatabaseMetaData meta = conn.getMetaData();
            System.out.println("  Columns in table:");
            try (ResultSet columns = meta.getColumns(null, "files", tableName, "%")) {
                while (columns.next()) {
                    System.out.println("    - " + columns.getString("COLUMN_NAME") + 
                        " (" + columns.getString("TYPE_NAME") + ")");
                }
            }
        } catch (SQLException e) {
            System.out.println("  Could not get column info: " + e.getMessage());
        }
        
        // Try query with actual columns
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM files." + tableName + " LIMIT 3")) {
            
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            
            // Print column headers
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
            System.out.println("✓ Query successful!");
            
        } catch (SQLException e) {
            System.out.println("✗ Query failed: " + e.getMessage());
        }
    }
}