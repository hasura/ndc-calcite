package org.kenstott;

import com.google.gson.*;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RuleSets;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


import java.util.ArrayList;
import java.util.List;
import java.sql.*;
import java.util.*;

import static java.util.Map.entry;

class ExportedKey {
    String pkTableCatalog;
    String pkTableSchema;
    String pkTableName;
    String pkColumnName;
    String pkName;
    String fkTableCatalog;
    String fkTableSchema;
    String fkTableName;
    String fkColumnName;
    String fkName;

    public ExportedKey(String pk_table_catalog, String pk_table_schema, String pk_table_name, String pk_column_name, String pk_name,
                       String fk_table_catalog, String fk_table_schema, String fk_table_name, String fk_column_name, String fk_name) {
        this.pkTableCatalog = pk_table_catalog;
        this.pkTableSchema = pk_table_schema;
        this.pkTableName = pk_table_name;
        this.pkColumnName = pk_column_name;
        this.pkName = pk_name;
        this.fkTableCatalog = fk_table_catalog;
        this.fkTableSchema = fk_table_schema;
        this.fkTableName = fk_table_name;
        this.fkColumnName = fk_column_name;
        this.fkName = fk_name;
    }
}

class ColumnMetadata {
    String name;
    String scalarType;
    Boolean nullable;
    String description;

    ColumnMetadata(String name, String scalarType, Boolean nullable, String description) {
        this.name = name;
        this.scalarType = scalarType;
        this.nullable = nullable;
        this.description = description;
    }
}

class TableMetadata {
    public TableMetadata(String catalog, String schema, String name, String description, ArrayList<String> primaryKeys, ArrayList<ExportedKey> exportedKeys) {
        this.catalog = catalog;
        this.schema = schema;
        this.name = name;
        this.description = description;
        this.primaryKeys = primaryKeys;
        this.exportedKeys = exportedKeys;
    }

    String catalog;
    String schema;
    String name;
    String description;
    ArrayList<String> primaryKeys = new ArrayList<>();
    ArrayList<ExportedKey> exportedKeys = new ArrayList<>();
    Map<String, ColumnMetadata> columns = new HashMap<>();
}

/**
 * Represents a class that interacts with a Calcite database using JDBC.
 */
public class CalciteQuery {

    private static Logger logger = LogManager.getLogger(CalciteQuery.class);

    static {
        System.setProperty("log4j.configurationFile", "classpath:log4j2-config.xml");
        logger = LogManager.getLogger(CalciteQuery.class);
    }

    static {
        // This block runs when the class is loaded
        Thread.currentThread().setContextClassLoader(CalciteQuery.class.getClassLoader());
    }

    public static void setClassLoader() {
        Thread.currentThread().setContextClassLoader(CalciteQuery.class.getClassLoader());
    }

    private static final OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
    private static final Tracer tracer = openTelemetry.getTracer("calcite-driver");
    private static final Gson gson = new Gson();

    Connection connection;
    CalciteSchema rootSchema;

    public static void noOpMethod() {
        Span span = tracer.spanBuilder("noOpMethod").startSpan();
        span.end();
    }

    /**
     * Creates a Calcite connection using the provided model file.
     *
     * @param modelPath The path to the model file.
     * @return The created Calcite connection.
     */
    public Connection createCalciteConnection(String modelPath) {
        CalciteQuery.setClassLoader();
        Span span = tracer.spanBuilder("createCalciteConnection").startSpan();
        span.setAttribute("modelPath", modelPath);
        Properties info = new Properties();
        info.setProperty("model", modelPath);
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
//            FrameworkConfig config = Frameworks.newConfigBuilder()
//                    .ruleSets(RuleSets.ofList()) // or an empty rule set to disable all optimizations
//                    .build();
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            rootSchema = connection.unwrap(CalciteConnection.class).getRootSchema().unwrap(CalciteSchema.class);
//            Planner planner = Frameworks.getPlanner(config);
            span.setStatus(StatusCode.OK);
        } catch (SQLException | ClassNotFoundException e) {
            span.setAttribute("error", e.toString());
            span.setStatus(StatusCode.ERROR);
            throw new RuntimeException(e);
        } finally {
            span.end();
        }


        return connection;
    }

    private Collection<TableMetadata> getTables() {
        Span span = tracer.spanBuilder("getTables").startSpan();
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            List<TableMetadata> list = new ArrayList<>();
            try (ResultSet catalogs = metaData.getCatalogs()) {
                while (catalogs.next()) {
                    ArrayList<String> path = new ArrayList<>();
                    String catalog = catalogs.getString("TABLE_CAT");
                    try (ResultSet schemas = metaData.getSchemas()) {
                        while (schemas.next()) {
                            String schemaName = schemas.getString(1);
                            CalciteSchema schemaPlus = rootSchema.getSubSchema(schemaName, true);
                            assert schemaPlus != null;
                            Schema schema = schemaPlus.schema;
                            DatabaseMetaData metaData1;

                            if (schema instanceof JdbcSchema) {
                                metaData1 = ((JdbcSchema) schema).getDataSource().getConnection().getMetaData();
                            } else {
                                metaData1 = metaData;
                            }
                            final List<String> TABLE_TYPES = Arrays.asList("INDEX", "SEQUENCE", "SYSTEM INDEX", "SYSTEM TABLE", "SYSTEM TOAST INDEX");
                            List<String> tableTypeList = new ArrayList<>();
                            try (ResultSet tableTypes = metaData.getTableTypes()) {
                                while (tableTypes.next()) {
                                    String tableType = tableTypes.getString(1);
                                    if (!TABLE_TYPES.contains(tableType)) {
                                        tableTypeList.add(tableType);
                                    }
                                }
                            } catch (SQLException e) {
                                logger.error(e.toString());
                                throw new RuntimeException(e);
                            }
                            tableTypeList.add("STREAM");
                            tableTypeList.add("BASE_TABLE");
                            String[] tableTypeArray = tableTypeList.toArray(new String[0]);
                            try (ResultSet tables = metaData.getTables(catalog, schemaName, null, tableTypeArray)) {
                                while (tables.next()) {
                                    String tableName = tables.getString("TABLE_NAME");
                                    String remarks = tables.getString("REMARKS");
                                    ArrayList<String> primaryKeys = new ArrayList<>();
                                    ArrayList<ExportedKey> exportedKeys = new ArrayList<ExportedKey>();
                                    try (ResultSet pks = metaData1.getPrimaryKeys(catalog, schemaName, tableName)) {
                                        while (pks.next()) {
                                            primaryKeys.add(pks.getString("COLUMN_NAME"));
                                        }
                                    }
                                    try {
                                        try (ResultSet eks = metaData1.getExportedKeys(catalog, schemaName, tableName)) {
                                            while (eks.next()) {
                                                exportedKeys.add(
                                                        new ExportedKey(
                                                                eks.getString("PKTABLE_CAT"),
                                                                eks.getString("PKTABLE_SCHEM"),
                                                                eks.getString("PKTABLE_NAME"),
                                                                eks.getString("PKCOLUMN_NAME"),
                                                                eks.getString("PK_NAME"),
                                                                eks.getString("FKTABLE_CAT"),
                                                                eks.getString("FKTABLE_SCHEM"),
                                                                eks.getString("FKTABLE_NAME"),
                                                                eks.getString("FKCOLUMN_NAME"),
                                                                eks.getString("FK_NAME")
                                                        )
                                                );
                                            }
                                        }
                                    } catch (SQLException e) { /* ignore */ }
                                    list.add(new TableMetadata(catalog, schemaName, tableName, remarks, primaryKeys, exportedKeys));
                                }
                            } catch (Exception e) {
                                span.setAttribute("Error", e.toString());
                            }
                        }
                    }
                }
            }
            span.setAttribute("Number of Tables", list.size());
            span.setStatus(StatusCode.OK);
            return list;
        } catch (SQLException e) {
            span.setStatus(StatusCode.ERROR);
            throw new RuntimeException(e);
        } finally {
            span.end();
        }
    }

    private Map<String, ColumnMetadata> getTableColumnInfo(TableMetadata table) {
        Span span = tracer.spanBuilder("getTables").startSpan();
        Map<String, ColumnMetadata> columns = new HashMap<>();
        ResultSet columnsSet;
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            String schemaName = table.schema;
            CalciteSchema schemaPlus = rootSchema.getSubSchema(schemaName, true);
            Schema schema = schemaPlus.schema;
            boolean sqliteFlag = false;
            if (schema instanceof JdbcSchema) {
                sqliteFlag = ((JdbcSchema) schema).dialect instanceof SQLiteSqlDialect;
            }
            columnsSet = metaData.getColumns(table.catalog, table.schema, table.name, null);
            while (columnsSet.next()) {
                String columnName = columnsSet.getString("COLUMN_NAME");
                String description = columnsSet.getString("REMARKS");
                String dataTypeName = columnsSet.getString("TYPE_NAME");
                boolean nullable = columnsSet.getBoolean("NULLABLE");
                Map<String, String> remapTypes = Map.ofEntries(
                        entry("CHAR", "CHAR"),
                        entry("VARCHAR", "VARCHAR"),
                        entry("VARCHAR(65536)", "VARCHAR"),
                        entry("VARCHAR(65536) NOT NULL", "VARCHAR"),
                        entry("VARCHAR NOT NULL", "VARCHAR"),
                        entry("JavaType(class java.util.ArrayList)", "LIST"),
                        entry("JavaType(class org.apache.calcite.adapter.file.ComparableArrayList)", "LIST"),
                        entry("JavaType(class java.util.LinkedHashMap)", "MAP"),
                        entry("JavaType(class org.apache.calcite.adapter.file.ComparableLinkedHashMap)", "MAP"),
                        entry("JavaType(class java.lang.String)", "VARCHAR"),
                        entry("JavaType(class java.lang.Integer)", "INTEGER"),
                        entry("INTEGER NOT NULL", "INTEGER"),
                        entry("INTEGER", "INTEGER"),
                        entry("SMALLINT NOT NULL", "INTEGER"),
                        entry("SMALLINT", "INTEGER"),
                        entry("TINYINT NOT NULL", "INTEGER"),
                        entry("TINYINT", "INTEGER"),
                        entry("BIGINT NOT NULL", "INTEGER"),
                        entry("BIGINT", "INTEGER"),
                        entry("FLOAT NOT NULL", "FLOAT"),
                        entry("FLOAT", "FLOAT"),
                        entry("DOUBLE NOT NULL", "DOUBLE"),
                        entry("DOUBLE", "DOUBLE"),
                        entry("BOOLEAN NOT NULL", "BOOLEAN"),
                        entry("BOOLEAN", "BOOLEAN"),
                        entry("VARBINARY NOT NULL", "VARBINARY"),
                        entry("VARBINARY", "VARBINARY"),
                        entry("BINARY NOT NULL", "BINARY"),
                        entry("BINARY", "BINARY"),
                        entry("DATE NOT NULL", "DATE"),
                        entry("DATE", "DATE"),
                        entry("TIME(0) NOT NULL", "TIME"),
                        entry("TIME(0)", "TIME"),
                        entry("TIMESTAMP(0) NOT NULL", "TIMESTAMP"),
                        entry("TIMESTAMP(0)", "TIMESTAMP"),
                        entry("TIMESTAMP(3) NOT NULL", "TIMESTAMP"),
                        entry("TIMESTAMP(3)", "TIMESTAMP"),
                        entry("TIMESTAMP NOT NULL", "TIMESTAMPTZ"),
                        entry("TIMESTAMP", "TIMESTAMPTZ"),
                        entry("DECIMAL(10,2)", "FLOAT"),
                        entry("DECIMAL(12,2)", "FLOAT")
                );
                String mappedType = remapTypes.get(dataTypeName);
                if (mappedType == null) {
                    if (dataTypeName.toLowerCase().contains("varchar")) {
                        mappedType = "VARCHAR";
                    } else if (dataTypeName.toLowerCase().contains("timestamp")) {
                        mappedType = "TIMESTAMP";
                    } else if (dataTypeName.toLowerCase().contains("decimal")) {
                        mappedType = "FLOAT";
                    } else {
                        span.setAttribute(dataTypeName, "unknown column type");
                        mappedType = "VARCHAR";
                    }
                }
                if (dataTypeName.startsWith("VARCHAR(65536)") && sqliteFlag) {
                    if (columnName.toLowerCase().contains("date")) {
                        mappedType = "TIMESTAMP";
                    }
                }
                columns.put(columnName, new ColumnMetadata(
                        columnName,
                        mappedType,
                        nullable,
                        description
                ));
            }
            span.setAttribute("Number of Columns Mapped", columns.size());
            span.setStatus(StatusCode.OK);
            return columns;
        } catch (SQLException e) {
            span.setAttribute("Error", e.toString());
            span.setStatus(StatusCode.ERROR);
            throw new RuntimeException(e);
        } finally {
            span.end();
        }

    }

    /**
     * Retrieves the models.
     * <p>
     * Note it maps all known column types from all adapters into a simplified
     * list of data types. Specifically, it does not distinguish NOT NULL types.
     * That maybe a useful improvement in a future version. In addition,
     * it's based on a dictionary of known data types - and unknown types default
     * to VARCHAR. Using a fuzzy algorithm to determine the data type could be
     * a future improvement.
     *
     * @return A JSON string representing the models.
     */
    public String getModels() throws SQLException {
        Span span = tracer.spanBuilder("getModels").startSpan();
        try {
            Gson gson = new Gson();
            Map<String, TableMetadata> result = new HashMap<>();
            Collection<TableMetadata> tables = getTables();
            for (TableMetadata table : tables) {
                table.columns = getTableColumnInfo(table);
                span.setAttribute(String.format("Table Name: '%s'", table.name), String.format("Column Count: %d", table.columns.size()));
                result.put(table.name, table);
            }
            span.setStatus(StatusCode.OK);
            return gson.toJson(result);
        } catch (Exception e) {
            span.setAttribute("Error", e.toString());
            span.setStatus(StatusCode.ERROR);
            return "{\"error\":\"" + e + "\"}";
        } finally {
            span.end();
        }
    }

    /**
     * Executes a SQL query on the database and returns the result as a JSON string.
     *
     * @param query The SQL query to execute.
     * @return A JSON string representing the result of the query.
     */
    public String queryModels(String query) {
        Span span = tracer.spanBuilder("queryModels").startSpan();
        try {
            Statement statement = connection.createStatement();
            span.setAttribute("query", query);
            PreparedStatement preparedStatement = StatementPreparer.prepare(query, connection);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (query.toLowerCase().trim().startsWith("select json_object(")) {
                span.setAttribute("Using JSON_OBJECT() method", true);
                ArrayList<String> rows = new ArrayList<>();
                while (resultSet.next()) {
                    rows.add(resultSet.getString(1));
                }
                resultSet.close();
                statement.close();
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                String result = gson.toJson(rows);
                span.setAttribute("Rows returned", rows.size());
                span.setStatus(StatusCode.OK);
                return result;
            } else {
                span.setAttribute("Using JSON_OBJECT() method", false);
                JsonArray jsonArray = new JsonArray();
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (resultSet.next()) {
                    JsonObject jsonObject = new JsonObject();
                    for (int i = 1; i <= columnCount; i++) {
                        String label = metaData.getColumnLabel(i);
                        int columnType = metaData.getColumnType(i);
                        switch (columnType) {
                            case Types.CHAR:
                            case Types.LONGNVARCHAR:
                            case Types.VARCHAR:
                            case Types.LONGVARBINARY:
                            case Types.VARBINARY:
                            case Types.DECIMAL:
                            case Types.BINARY:
                                jsonObject.addProperty(label, resultSet.getString(i));
                                break;
                            case Types.BIGINT:
                            case Types.INTEGER:
                            case Types.SMALLINT:
                            case Types.TINYINT:
                            case Types.BIT:
                                jsonObject.addProperty(label, resultSet.getInt(i));
                                break;
                            case Types.BOOLEAN:
                                jsonObject.addProperty(label, resultSet.getBoolean(i));
                                break;
                            case Types.REAL:
                            case Types.FLOAT:
                                jsonObject.addProperty(label, resultSet.getFloat(i));
                                break;
                            case Types.NUMERIC:
                            case Types.DOUBLE:
                                jsonObject.addProperty(label, resultSet.getDouble(i));
                                break;
                            case Types.DATE:
                            case Types.TIMESTAMP:
                                jsonObject.addProperty(label, String.valueOf(resultSet.getDate(i)));
                                break;
                            default:
                                Object columnValue = resultSet.getObject(i);
                                boolean isArrayList = columnValue instanceof ArrayList;
                                boolean isHashMap = columnValue instanceof HashMap;
                                if (columnValue == null) {
                                    jsonObject.addProperty(label, (String) null);
                                } else if (isArrayList) {
                                    JsonArray nestedArray = gson.toJsonTree(columnValue).getAsJsonArray();
                                    jsonObject.add(label, nestedArray);
                                } else if (isHashMap) {
                                    JsonObject nestedJsonObject = JsonParser.parseString(gson.toJson(columnValue)).getAsJsonObject();
                                    jsonObject.add(label, nestedJsonObject);
                                } else {
                                    jsonObject.addProperty(label, columnValue.toString());
                                }
                                break;
                        }
                    }
                    jsonArray.add(jsonObject.toString());
                }
                resultSet.close();
                statement.close();
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                String result = gson.toJson(jsonArray);
                span.setAttribute("Rows returned", jsonArray.size());
                span.setStatus(StatusCode.OK);
                return result;
            }
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR);
            span.setAttribute("Error", e.toString());
            return "{\"error\":\"" + e + "\"}";
        } finally {
            span.end();
        }
    }

    public String queryPlanModels(String query) {
        Span span = tracer.spanBuilder("queryPlanModels").startSpan();
        try {
            Statement statement = connection.createStatement();
            span.setAttribute("query", query);
            PreparedStatement preparedStatement = StatementPreparer.prepare("explain plan for " + query, connection);
            ResultSet resultSet = preparedStatement.executeQuery();
            JsonArray jsonArray = new JsonArray();
            JsonObject jsonObject = new JsonObject();
            int j = 1;
            while (resultSet.next()) {
                j++;
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) {
                        jsonObject.addProperty(query + j + "." + "." + i, resultSet.getObject(i).toString());
                    } else {
                        jsonObject.addProperty(query, resultSet.getObject(i).toString());
                    }
                }
            }
            resultSet.close();
            statement.close();
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            jsonArray.add(gson.toJson(jsonObject));
            String result = gson.toJson(jsonArray);
            span.setAttribute("plan", result);
            span.setStatus(StatusCode.OK);
            return result;
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR);
            span.setAttribute("Error", e.toString());
            return "{\"error\":\"" + e + "\"}";
        } finally {
            span.end();
        }
    }
}
