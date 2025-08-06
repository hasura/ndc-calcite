package com.hasura.file;

import java.sql.*;
import java.io.File;

public class TestRemainingIssues {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Investigating Remaining Issues ===\n");
        
        // Test  with more detailed error reporting
        System.out.println("##  Investigation");
        System.out.println("=".repeat(40));
        testDetailedFormat("tests/data//logs.", "logs", "");
        
        System.out.println("\n## XLSX Investigation");
        System.out.println("=".repeat(40));
        testDetailedFormat("tests/data/xlsx/company_data.xlsx", "company_data", "XLSX");
        
        // Test if different format parameter helps
        System.out.println("\n## Alternative Format Parameters");
        System.out.println("=".repeat(40));
        testWithFormatParameter("tests/data//logs.", "logs", "", "json");
        testWithFormatParameter("tests/data/xlsx/company_data.xlsx", "company_data", "XLSX", "excel");
    }
    
    private static void testDetailedFormat(String filePath, String expectedTable, String formatName) {
        try {
            String url = "jdbc:file://" + new File(filePath).getAbsolutePath();
            
            try (Connection conn = DriverManager.getConnection(url)) {
                // Check table structure
                DatabaseMetaData meta = conn.getMetaData();
                
                try (ResultSet columns = meta.getColumns(null, "files", expectedTable, "%")) {
                    System.out.println("  Table Columns:");
                    while (columns.next()) {
                        String columnName = columns.getString("COLUMN_NAME");
                        String dataType = columns.getString("TYPE_NAME");
                        System.out.println("    " + columnName + " (" + dataType + ")");
                    }
                }
                
                // Try different query approaches
                try (Statement stmt = conn.createStatement()) {
                    // Try COUNT query first
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM files." + expectedTable)) {
                        if (rs.next()) {
                            int count = rs.getInt(1);
                            System.out.println("  Row count: " + count);
                        }
                    } catch (SQLException e) {
                        System.out.println("  COUNT query failed: " + e.getMessage());
                    }
                    
                    // Try SELECT * with different approaches
                    try (ResultSet rs = stmt.executeQuery("SELECT * FROM files." + expectedTable + " LIMIT 1")) {
                        if (rs.next()) {
                            System.out.println("  ✅ " + formatName + " query successful!");
                        }
                    } catch (SQLException e) {
                        System.out.println("  ❌ " + formatName + " query failed:");
                        System.out.println("    Full error: " + e.getMessage());
                        
                        // Check if it's a specific error type
                        if (e.getMessage().contains("JsonMappingException")) {
                            System.out.println("    Issue: JSON mapping problem");
                        } else if (e.getMessage().contains("No suitable driver")) {
                            System.out.println("    Issue: Driver problem");
                        } else if (e.getMessage().contains("FileReader")) {
                            System.out.println("    Issue: File reading problem");
                        }
                    }
                }
                
            } catch (SQLException e) {
                System.out.println("  Connection failed: " + e.getMessage());
            }
            
        } catch (Exception e) {
            System.out.println("  Test failed: " + e.getMessage());
        }
    }
    
    private static void testWithFormatParameter(String filePath, String expectedTable, String formatName, String formatParam) {
        System.out.println("\n  Testing " + formatName + " with format=" + formatParam);
        
        try {
            String url = "jdbc:file://" + new File(filePath).getAbsolutePath() + "?format=" + formatParam;
            
            try (Connection conn = DriverManager.getConnection(url)) {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT * FROM files." + expectedTable + " LIMIT 1")) {
                    
                    if (rs.next()) {
                        System.out.println("    ✅ " + formatName + " with format parameter works!");
                        
                        // Show data
                        ResultSetMetaData rsmd = rs.getMetaData();
                        int columnCount = rsmd.getColumnCount();
                        System.out.print("    Data: ");
                        for (int i = 1; i <= columnCount; i++) {
                            Object value = rs.getObject(i);
                            System.out.print(value + (i < columnCount ? ", " : ""));
                        }
                        System.out.println();
                    }
                    
                } catch (SQLException e) {
                    System.out.println("    ❌ " + formatName + " with format parameter still fails:");
                    System.out.println("      " + e.getMessage().substring(0, Math.min(80, e.getMessage().length())));
                }
                
            } catch (SQLException e) {
                System.out.println("    Connection with format parameter failed: " + e.getMessage());
            }
            
        } catch (Exception e) {
            System.out.println("    Test with format parameter failed: " + e.getMessage());
        }
    }
}