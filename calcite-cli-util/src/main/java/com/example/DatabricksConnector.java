package com.example;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.commons.cli.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.nio.file.Files;
import java.sql.*;
import java.util.Properties;
import java.util.Scanner;
import java.util.List;
import java.util.ArrayList;

public class DatabricksConnector {
    public static void main(String[] args) {
        Options options = setupCommandLineOptions();
        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("DatabricksConnector", options);
                return;
            }

            String modelFile = cmd.getOptionValue("model");
            String accessToken = System.getenv("DATABRICKS_ACCESS_TOKEN");
            if (accessToken == null || accessToken.trim().isEmpty()) {
                throw new RuntimeException("DATABRICKS_ACCESS_TOKEN environment variable is not set");
            }

            // Register the Databricks JDBC driver
            Class.forName("com.databricks.client.jdbc.Driver");

            // Process the model file to replace the token
            String processedModel = processModelFile(modelFile, accessToken);
            File tempModelFile = createTempModelFile(processedModel);

            Connection connection = setupCalciteConnection(tempModelFile.getAbsolutePath());
            runCliLoop(connection);

            // Cleanup
            tempModelFile.delete();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static String processModelFile(String modelFile, String accessToken) throws Exception {
        String content = new String(Files.readAllBytes(new File(modelFile).toPath()));
        return content.replace("${DATABRICKS_ACCESS_TOKEN}", accessToken);
    }

    private static File createTempModelFile(String content) throws Exception {
        File tempFile = File.createTempFile("calcite-model", ".json");
        tempFile.deleteOnExit();
        Files.write(tempFile.toPath(), content.getBytes());
        return tempFile;
    }

    private static Options setupCommandLineOptions() {
        Options options = new Options();
        options.addOption(Option.builder("m")
                .longOpt("model")
                .hasArg()
                .required()
                .desc("Calcite model.json file path")
                .build());
        options.addOption("h", "help", false, "Show help");
        return options;
    }

    private static Connection setupCalciteConnection(String modelFile) throws SQLException {
        Properties info = new Properties();
        info.setProperty("model", new File(modelFile).getAbsolutePath());
        info.setProperty("lex", "JAVA");

        return DriverManager.getConnection("jdbc:calcite:", info);
    }

private static void runCliLoop(Connection connection) {
    try (Scanner scanner = new Scanner(System.in)) {
        Statement stmt = connection.createStatement();
        StringBuilder queryBuilder = new StringBuilder();

        System.out.println("Connected to Databricks. Type your SQL queries (terminate with semicolon, type 'exit;' to quit):");
        System.out.print("sql> ");

        while (true) {
            String line = scanner.nextLine();

            // Handle the case where someone hits enter multiple times
            if (line.trim().isEmpty()) {
                if (queryBuilder.length() == 0) {
                    System.out.print("sql> ");
                } else {
                    System.out.print("   -> ");
                }
                continue;
            }

            queryBuilder.append(" ").append(line);

            // Check for exit command (allowing for spaces before/after)
            if (line.trim().equalsIgnoreCase("exit;") || line.trim().equalsIgnoreCase("quit;")) {
                break;
            }

            // If we find a semicolon, execute the statement
            if (line.trim().endsWith(";")) {
                String sql = queryBuilder.toString().trim();
                // Remove the final semicolon before execution
                sql = sql.substring(0, sql.length() - 1).trim();

                if (!sql.isEmpty()) {
                    try {
                        boolean isQuery = stmt.execute(sql);
                        if (isQuery) {
                            printResultSet(stmt.getResultSet());
                        } else {
                            System.out.println("Statement executed successfully.");
                        }
                    } catch (SQLException e) {
                        System.err.println("Error executing SQL: " + e.getMessage());
                    }
                }

                // Reset the buffer and print the prompt
                queryBuilder.setLength(0);
                System.out.print("sql> ");
            } else {
                // Continue reading more lines
                System.out.print("   -> ");
            }
        }
    } catch (SQLException e) {
        System.err.println("Error in CLI loop: " + e.getMessage());
    }
}



         private static void printResultSet(ResultSet rs) throws SQLException {
             ResultSetMetaData metadata = rs.getMetaData();
             int columnCount = metadata.getColumnCount();

             // Store column names and their max widths
             String[] columnNames = new String[columnCount];
             int[] columnWidths = new int[columnCount];

             // Initialize with column names and their lengths
             for (int i = 0; i < columnCount; i++) {
                 columnNames[i] = metadata.getColumnName(i + 1);
                 columnWidths[i] = columnNames[i].length();
             }

             // Store all rows and find maximum widths
             List<String[]> rows = new ArrayList<>();
             while (rs.next()) {
                 String[] row = new String[columnCount];
                 for (int i = 0; i < columnCount; i++) {
                     String value = rs.getString(i + 1);
                     row[i] = value == null ? "NULL" : value;
                     columnWidths[i] = Math.max(columnWidths[i], row[i].length());
                 }
                 rows.add(row);
             }

             // Print top border
             printBorder(columnWidths);

             // Print column headers
             System.out.print("│");
             for (int i = 0; i < columnCount; i++) {
                 System.out.print(" " + padRight(columnNames[i], columnWidths[i]) + " │");
             }
             System.out.println();

             // Print separator
             printBorder(columnWidths);

             // Print data rows
             for (String[] row : rows) {
                 System.out.print("│");
                 for (int i = 0; i < columnCount; i++) {
                     System.out.print(" " + padRight(row[i], columnWidths[i]) + " │");
                 }
                 System.out.println();
             }

             // Print bottom border
             printBorder(columnWidths);

             // Print row count
             System.out.println(rows.size() + " row(s) returned");
         }

         private static void printBorder(int[] columnWidths) {
             System.out.print("├");
             for (int i = 0; i < columnWidths.length; i++) {
                 System.out.print("─".repeat(columnWidths[i] + 2));
                 System.out.print(i < columnWidths.length - 1 ? "┼" : "┤");
             }
             System.out.println();
         }

         private static String padRight(String s, int n) {
             return String.format("%-" + n + "s", s);
         }
}
