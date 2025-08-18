package com.hasura.file;

import java.sql.*;
import java.io.File;

public class TestXLSXApproaches {
    public static void main(String[] args) {
        System.out.println("=== Testing XLSX File: Directory vs Single File Approach ===\n");
        
        // First, let's see what happens with directory approach
        System.out.println("APPROACH 1: Connecting to directory containing XLSX file");
        System.out.println("=========================================================");
        testDirectoryApproach();
        
        System.out.println("\n\nAPPROACH 2: Connecting directly to XLSX file");
        System.out.println("=============================================");
        testSingleFileApproach();
        
        System.out.println("\n\nCONCLUSION:");
        System.out.println("- Directory approach: Should auto-convert XLSX to JSON files");
        System.out.println("- Single file approach: Creates single table but doesn't handle sheets properly");
    }
    
    private static void testDirectoryApproach() {
        try {
            String dirPath = new File("tests/data/xlsx").getAbsolutePath();
            String url = "jdbc:file://" + dirPath;
            
            System.out.println("URL: " + url);
            System.out.println("\nChecking directory contents before connection:");
            File dir = new File("tests/data/xlsx");
            for (File f : dir.listFiles()) {
                System.out.println("  - " + f.getName());
            }
            
            try (Connection conn = DriverManager.getConnection(url)) {
                System.out.println("\nConnected successfully!");
                
                // Check what files exist after connection
                System.out.println("\nChecking directory contents after connection:");
                for (File f : dir.listFiles()) {
                    System.out.println("  - " + f.getName());
                }
                
                // List tables
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("\nTables found in 'files' schema:");
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    boolean foundTables = false;
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  - " + tableName);
                        foundTables = true;
                    }
                    if (!foundTables) {
                        System.out.println("  (No tables found)");
                    }
                }
                
                // Try to query if we have the expected tables
                tryQuery(conn, "CompanyData__employees", 
                    "SELECT * FROM files.CompanyData__employees WHERE CAST(salary AS INTEGER) > 70000");
                tryQuery(conn, "CompanyData__departments", 
                    "SELECT * FROM files.CompanyData__departments");
                tryQuery(conn, "CompanyData__projects", 
                    "SELECT * FROM files.CompanyData__projects WHERE status = 'Active'");
            }
            
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testSingleFileApproach() {
        try {
            String filePath = new File("tests/data/xlsx/company_data.xlsx").getAbsolutePath();
            String url = "jdbc:file://" + filePath;
            
            System.out.println("URL: " + url);
            
            try (Connection conn = DriverManager.getConnection(url)) {
                System.out.println("\nConnected successfully!");
                
                // List tables
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("\nTables found in 'files' schema:");
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    boolean foundTables = false;
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  - " + tableName);
                        foundTables = true;
                        
                        // Try to get table info
                        try (ResultSet columns = meta.getColumns(null, "files", tableName, "%")) {
                            System.out.println("    Columns:");
                            while (columns.next()) {
                                System.out.println("      - " + columns.getString("COLUMN_NAME") + 
                                    " (" + columns.getString("TYPE_NAME") + ")");
                            }
                        }
                    }
                    if (!foundTables) {
                        System.out.println("  (No tables found)");
                    }
                }
                
                // Try to query the single table
                tryQuery(conn, "company_data", "SELECT * FROM files.company_data LIMIT 5");
            }
            
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void tryQuery(Connection conn, String tableName, String query) {
        System.out.println("\nTrying to query " + tableName + ":");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            
            // Print column headers
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(rsmd.getColumnName(i) + "\t");
            }
            System.out.println();
            
            // Print first few rows
            int rowCount = 0;
            while (rs.next() && rowCount < 3) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i) + "\t");
                }
                System.out.println();
                rowCount++;
            }
            System.out.println("✓ Query successful!");
            
        } catch (SQLException e) {
            System.out.println("✗ Query failed: " + e.getMessage());
        }
    }
}