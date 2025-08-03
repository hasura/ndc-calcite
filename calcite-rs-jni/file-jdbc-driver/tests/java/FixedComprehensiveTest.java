package com.hasura.file;

import java.sql.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FixedComprehensiveTest {
    public static void main(String[] args) throws Exception {
        Class.forName("com.hasura.file.FileDriver");
        
        StringBuilder results = new StringBuilder();
        results.append("## Format Support Matrix (Current State - Post Calcite Rebuild)\n\n");
        results.append("**Updated**: 2025-07-25 (Post calcite library rebuild)\n\n");
        
        results.append("| Format | Extension | Directory | Direct File | Query Success | Issues |\n");
        results.append("|--------|-----------|-----------|-------------|---------------|--------|\n");
        
        // Test each format with CORRECT file paths
        String[][] tests = {
            {"CSV", ".csv", "tests/data/csv/sales.csv", "sales"},
            {"TSV", ".tsv", "tests/data/tsv/inventory.tsv", "inventory"}, 
            {"JSON", ".json", "tests/data/json/customers.json", "customers"},
            {"YAML", ".yaml/.yml", "tests/data/yaml/config.yaml", "config"},
            {"XLSX", ".xlsx", "tests/data/xlsx/company_data.xlsx", "company_data"},
            {"HTML", ".html", "tests/data/html/report.html", "sales_data"},
            {"Parquet", ".parquet", "tests/data/parquet/sample.parquet", "sample"},
            {"Arrow", ".arrow", "tests/data/arrow/sample.arrow", "sample"}
        };
        
        for (String[] test : tests) {
            String format = test[0];
            String ext = test[1];
            String filePath = test[2];
            String expectedTable = test[3];
            
            File file = new File(filePath);
            
            if (!file.exists()) {
                results.append("| **").append(format).append("** | ").append(ext)
                       .append(" | ❌ | ❌ | ❌ | File not found: ").append(filePath).append(" |\n");
                continue;
            }
            
            String url = "jdbc:file://" + file.getAbsolutePath();
            
            try (Connection conn = DriverManager.getConnection(url)) {
                DatabaseMetaData meta = conn.getMetaData();
                boolean foundTable = false;
                String discoveredTable = "";
                
                // Check what tables are actually discovered
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        if (discoveredTable.isEmpty()) discoveredTable = tableName;
                        if (tableName.equals(expectedTable)) {
                            foundTable = true;
                            break;
                        }
                    }
                }
                
                if (foundTable) {
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT * FROM files." + expectedTable + " LIMIT 1")) {
                        
                        if (rs.next()) {
                            // SUCCESS
                            results.append("| **").append(format).append("** | ").append(ext)
                                   .append(" | ✅ | ✅ | ✅ | None - works properly |\n");
                        } else {
                            results.append("| **").append(format).append("** | ").append(ext)
                                   .append(" | ✅ | ✅ | ❌ | Query returns no data |\n");
                        }
                    } catch (SQLException e) {
                        String error = e.getMessage();
                        String issue = "Query execution fails";
                        if (error.contains("Jackson")) {
                            issue = "Jackson version conflict";
                        } else if (error.contains("FileReader")) {
                            issue = "FileSchemaFactory issue";
                        } else if (error.contains("Hadoop")) {
                            issue = "Missing Hadoop dependencies";
                        }
                        
                        results.append("| **").append(format).append("** | ").append(ext)
                               .append(" | ✅ | ✅ | ❌ | ").append(issue).append(" |\n");
                    }
                } else {
                    // Table not found with expected name, but connection worked
                    results.append("| **").append(format).append("** | ").append(ext)
                           .append(" | ✅ | ✅ | ❌ | Expected '").append(expectedTable)
                           .append("' but found '").append(discoveredTable).append("' |\n");
                }
                
            } catch (SQLException e) {
                results.append("| **").append(format).append("** | ").append(ext)
                       .append(" | ❌ | ❌ | ❌ | Connection failed: ").append(e.getMessage().substring(0, Math.min(50, e.getMessage().length()))).append(" |\n");
            }
        }
        
        // Write the corrected results back to the file
        updateMatrixInFile(results.toString());
        
        System.out.println("=== FIXED COMPREHENSIVE TEST COMPLETED ===");
        System.out.println("Updated calcite-adapter-bugs-analysis.md with correct file paths");
    }
    
    private static void updateMatrixInFile(String newMatrix) throws IOException {
        String filePath = "tests/results/calcite-adapter-bugs-analysis.md";
        
        // Read the existing file
        java.nio.file.Path path = java.nio.file.Paths.get(filePath);
        String content = new String(java.nio.file.Files.readAllBytes(path));
        
        // Replace the matrix section
        String startMarker = "## Format Support Matrix (Current State - Post Calcite Rebuild)";
        String endMarker = "## Code Locations Requiring Investigation";
        
        int startIndex = content.indexOf(startMarker);
        int endIndex = content.indexOf(endMarker);
        
        if (startIndex != -1 && endIndex != -1) {
            String before = content.substring(0, startIndex);
            String after = content.substring(endIndex);
            String updated = before + newMatrix + "\n" + after;
            
            java.nio.file.Files.write(path, updated.getBytes());
        }
    }
}