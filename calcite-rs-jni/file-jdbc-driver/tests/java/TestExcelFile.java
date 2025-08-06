package com.hasura.file;

import java.sql.*;
import java.io.File;

public class TestExcelFile {
    public static void main(String[] args) {
        System.out.println("=== Testing Excel File with Multiple Sheets ===\n");
        
        // Test 1: Connect to directory containing XLSX file
        System.out.println("1. Directory connection (Excel auto-converts to JSON):");
        testDirectory();
        
        // Test 2: Try single file connection
        System.out.println("\n2. Single file connection:");
        testSingleFile();
        
        // Test 3: Query the converted JSON tables
        System.out.println("\n3. Querying Excel data:");
        queryExcelData();
    }
    
    private static void testDirectory() {
        try {
            String url = "jdbc:file://" + new File("tests/data/xlsx").getAbsolutePath();
            
            try (Connection conn = DriverManager.getConnection(url)) {
                System.out.println("Connected to: " + url);
                
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("\nTables found (Excel sheets converted to JSON):");
                
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    while (tables.next()) {
                        System.out.println("  - " + tables.getString("TABLE_NAME"));
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
    
    private static void testSingleFile() {
        try {
            String url = "jdbc:file://" + new File("tests/data/xlsx/company_data.xlsx").getAbsolutePath();
            
            try (Connection conn = DriverManager.getConnection(url)) {
                System.out.println("Connected to: " + url);
                
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("\nTables found:");
                
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    while (tables.next()) {
                        System.out.println("  - " + tables.getString("TABLE_NAME"));
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
    
    private static void queryExcelData() {
        try {
            // Connect to the directory (not the file directly)
            String url = "jdbc:file://" + new File("tests/data/xlsx").getAbsolutePath();
            
            try (Connection conn = DriverManager.getConnection(url)) {
                // Query employees
                System.out.println("\nEmployees with salary > 70000:");
                System.out.println("ID | Name         | Department   | Salary");
                System.out.println("---|--------------|--------------|--------");
                
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT id, name, department, salary " +
                         "FROM files.CompanyData__employees " +
                         "WHERE CAST(salary AS INTEGER) > 70000")) {
                    
                    while (rs.next()) {
                        System.out.printf("%-2s | %-12s | %-12s | %s%n",
                            rs.getString("id"),
                            rs.getString("name"),
                            rs.getString("department"),
                            rs.getString("salary"));
                    }
                }
                
                // Query departments
                System.out.println("\nDepartments:");
                System.out.println("ID | Name         | Manager      | Budget");
                System.out.println("---|--------------|--------------|--------");
                
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT dept_id, dept_name, manager, budget " +
                         "FROM files.CompanyData__departments")) {
                    
                    while (rs.next()) {
                        System.out.printf("%-2s | %-12s | %-12s | %s%n",
                            rs.getString("dept_id"),
                            rs.getString("dept_name"),
                            rs.getString("manager"),
                            rs.getString("budget"));
                    }
                }
                
                // Join query
                System.out.println("\nActive Projects with Department Info:");
                System.out.println("Project      | Department   | Manager");
                System.out.println("-------------|--------------|---------------");
                
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT p.project_name, d.dept_name, d.manager " +
                         "FROM files.CompanyData__projects p " +
                         "JOIN files.CompanyData__departments d ON p.dept_id = d.dept_id " +
                         "WHERE p.status = 'Active'")) {
                    
                    while (rs.next()) {
                        System.out.printf("%-12s | %-12s | %s%n",
                            rs.getString("project_name"),
                            rs.getString("dept_name"),
                            rs.getString("manager"));
                    }
                }
                
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}