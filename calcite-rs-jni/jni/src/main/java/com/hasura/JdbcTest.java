package com.hasura;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JdbcTest {
    public static void printDatabaseMetadata(Connection conn) throws SQLException {
        // Previous metadata code remains the same
        DatabaseMetaData metaData = conn.getMetaData();

        System.out.println("\n=== Schemas ===");
        ResultSet schemas = metaData.getSchemas();
        while (schemas.next()) {
            String schemaName = schemas.getString("TABLE_SCHEM");
            System.out.println("\nSchema: " + schemaName);

            ResultSet tables = metaData.getTables(null, schemaName, null, new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                System.out.println("\n  Table: " + tableName);

                ResultSet columns = metaData.getColumns(null, schemaName, tableName, null);
                System.out.println("  Columns:");
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    String dataType = columns.getString("TYPE_NAME");
                    int size = columns.getInt("COLUMN_SIZE");
                    boolean nullable = columns.getInt("NULLABLE") == DatabaseMetaData.columnNullable;

                    System.out.printf("    - %s (%s, size: %d, nullable: %s)%n",
                            columnName, dataType, size, nullable);
                }

                ResultSet primaryKeys = metaData.getPrimaryKeys(null, schemaName, tableName);
                System.out.println("  Primary Keys:");
                while (primaryKeys.next()) {
                    String columnName = primaryKeys.getString("COLUMN_NAME");
                    String pkName = primaryKeys.getString("PK_NAME");
                    System.out.printf("    - %s (constraint: %s)%n", columnName, pkName);
                }

                ResultSet foreignKeys = metaData.getImportedKeys(null, schemaName, tableName);
                System.out.println("  Foreign Keys:");
                while (foreignKeys.next()) {
                    String fkColumnName = foreignKeys.getString("FKCOLUMN_NAME");
                    String pkTableName = foreignKeys.getString("PKTABLE_NAME");
                    String pkColumnName = foreignKeys.getString("PKCOLUMN_NAME");
                    System.out.printf("    - %s -> %s.%s%n",
                            fkColumnName, pkTableName, pkColumnName);
                }
            }
        }
    }

    public static void printAlbumsTable(Connection conn) throws SQLException {
        System.out.println("\n=== Albums Table Data ===");

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM \"Albums\"")) {

            // Get metadata for columns
            ResultSetMetaData rsMetaData = rs.getMetaData();
            int columnCount = rsMetaData.getColumnCount();

            // Store column names and initialize widths
            String[] columnNames = new String[columnCount];
            int[] columnWidths = new int[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                columnNames[i-1] = rsMetaData.getColumnName(i);
                columnWidths[i-1] = columnNames[i-1].length();
            }

            // Store all rows while calculating column widths
            List<String[]> rows = new ArrayList<>();
            while (rs.next()) {
                String[] row = new String[columnCount];
                for (int i = 1; i <= columnCount; i++) {
                    String value = rs.getString(i);
                    row[i-1] = (value != null ? value : "NULL");
                    columnWidths[i-1] = Math.max(columnWidths[i-1], row[i-1].length());
                }
                rows.add(row);
            }

            // Print headers
            for (int i = 0; i < columnCount; i++) {
                System.out.printf("| %-" + columnWidths[i] + "s ", columnNames[i]);
            }
            System.out.println("|");

            // Print separator line
            for (int width : columnWidths) {
                System.out.print("+");
                System.out.print("-".repeat(width + 2));
            }
            System.out.println("+");

            // Print data
            for (String[] row : rows) {
                for (int i = 0; i < columnCount; i++) {
                    System.out.printf("| %-" + columnWidths[i] + "s ", row[i]);
                }
                System.out.println("|");
            }
        }
    }

    public static void test() {
        String url = "jdbc:graphql:http://localhost:3000/graphql";
        String role = "admin";

        try {
            Class.forName("com.hasura.GraphQLDriver");

            Properties properties = new Properties();
            properties.setProperty("role", role);

            System.out.println("Attempting to connect to database...");
            try (Connection conn = DriverManager.getConnection(url, properties)) {
                if (conn != null) {
                    System.out.println("Database connection successful!");
                    System.out.println("Driver: " + conn.getMetaData().getDriverName());
                    System.out.println("Driver Version: " + conn.getMetaData().getDriverVersion());

                    // Print database metadata
                    printDatabaseMetadata(conn);

                    // Print albums table data
                    printAlbumsTable(conn);
                }
            }

        } catch (ClassNotFoundException e) {
            System.out.println("JDBC Driver not found.");
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("Database connection failed!");
            e.printStackTrace();
        }
    }
}