package com.hasura;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import java.sql.*;
import java.util.Properties;

public class CalciteVerboseDebugger {
    public static void debugVerbose(String modelPath) throws Exception {
        Properties info = new Properties();
        info.put("model", modelPath);

        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();

            System.out.println("=== Calcite Configuration Debug ===");
            System.out.println("Model path: " + modelPath);

            // Print connection info
            System.out.println("\nConnection Properties:");
            info.forEach((k, v) -> System.out.println(k + ": " + v));

            // Print schema details
            System.out.println("\nSchema Structure:");
            rootSchema.getSubSchemaNames().forEach(schemaName -> {
                System.out.println("\nSchema: " + schemaName);
                SchemaPlus schema = rootSchema.getSubSchema(schemaName);

                System.out.println("Tables:");
                schema.getTableNames().forEach(tableName -> {
                    Table table = schema.getTable(tableName);
                    System.out.println("  - " + tableName);
                    System.out.println("    Type: " + table.getClass().getName());
                    System.out.println("    Rowtype: " + table.getRowType(calciteConnection.getTypeFactory()));
                });
            });

            // Try to get database metadata
            DatabaseMetaData metaData = connection.getMetaData();
            System.out.println("\nDatabase Metadata:");
            try (ResultSet tables = metaData.getTables(null, null, null, new String[]{"TABLE"})) {
                while (tables.next()) {
                    String tableSchema = tables.getString("TABLE_SCHEM");
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("Found table: " + tableSchema + "." + tableName);
                }
            }
        }
    }
}