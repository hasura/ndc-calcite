package com.hasura;

import com.google.gson.*;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Context;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


import java.util.ArrayList;
import java.util.List;
import java.sql.*;
import java.util.*;

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
    public TableMetadata(String catalog, String schema, String name, String description, ArrayList<String> primaryKeys, ArrayList<ExportedKey> exportedKeys, String physicalCatalog, String physicalSchema) {
        this.catalog = catalog == null ? "" : catalog;
        this.schema = schema;
        this.name = name;
        this.description = description;
        this.primaryKeys = primaryKeys;
        this.exportedKeys = exportedKeys;
        this.physicalSchema = physicalSchema;
        this.physicalCatalog = physicalCatalog;
    }

    String catalog;
    String physicalCatalog;
    String schema;
    String physicalSchema;
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
//        System.setProperty("log4j.configurationFile", "classpath:log4j2-config.xml");
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
    public Connection createCalciteConnection(String modelPath) throws IOException {
        CalciteQuery.setClassLoader();
        Span span = tracer.spanBuilder("createCalciteConnection").startSpan();
        span.setAttribute("modelPath", modelPath);
        Properties info = new Properties();
        info.setProperty("model", ConfigPreprocessor.preprocessConfig(modelPath));
        try {
//            Class.forName("com.simba.googlebigquery.jdbc42.Driver");
            Class.forName("org.apache.calcite.jdbc.Driver");
//            Class.forName("org.apache.parquet.hadoop.api.ReadSupport");
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            rootSchema = connection.unwrap(CalciteConnection.class).getRootSchema().unwrap(CalciteSchema.class);
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.setAttribute("error", e.toString());
            span.setStatus(StatusCode.ERROR);
            throw new RuntimeException(e);
        } finally {
            span.end();
        }


        return connection;
    }

    private Collection<TableMetadata> getTables() {
        Tracer tracer = openTelemetry.getTracer("calcite-driver");
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
                            } catch (Throwable e) {
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
                                    String localCatalogName = catalog;
                                    String localSchemaName = schemaName;
                                    if (schema instanceof JdbcSchema) {
                                        JdbcTable underlyingTable = (JdbcTable) ((JdbcSchema) schema).getTable(tableName);
                                        assert underlyingTable != null;
                                        localCatalogName = underlyingTable.jdbcCatalogName == null ? catalog : underlyingTable.jdbcCatalogName;
                                        localSchemaName = underlyingTable.jdbcSchemaName == null ? schemaName : underlyingTable.jdbcSchemaName;
                                    }
                                    try (ResultSet pks = metaData1.getPrimaryKeys(localCatalogName, localSchemaName, tableName)) {
                                        while (pks.next()) {
                                            primaryKeys.add(pks.getString("COLUMN_NAME"));
                                        }
                                    }
                                    try {
                                        try (ResultSet eks = metaData1.getExportedKeys(localCatalogName, localSchemaName, tableName)) {
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
                                    list.add(new TableMetadata(catalog, schemaName, tableName, remarks, primaryKeys, exportedKeys, localCatalogName, localSchemaName));
                                }
                            } catch (Throwable e) {
                                span.setAttribute("Error", e.toString());
                            }
                        }
                    } catch (Throwable e) {
                        System.err.println(e.toString());
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
        Tracer tracer = openTelemetry.getTracer("calcite-driver");
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
                Map<String, String> remapTypes = new HashMap<>();
                remapTypes.put("CHAR", "CHAR");
                remapTypes.put("CHAR(1)", "VARCHAR");
                remapTypes.put("VARCHAR", "VARCHAR");
                remapTypes.put("VARCHAR(65536)", "VARCHAR");
                remapTypes.put("VARCHAR(65536) NOT NULL", "VARCHAR");
                remapTypes.put("VARCHAR NOT NULL", "VARCHAR");
                remapTypes.put("JavaType(class java.util.ArrayList)", "LIST");
                remapTypes.put("JavaType(class org.apache.calcite.adapter.file.ComparableArrayList)", "LIST");
                remapTypes.put("ANY ARRAY", "LIST");
                remapTypes.put("VARCHAR NOT NULL ARRAY", "LIST");
                remapTypes.put("JavaType(class java.util.LinkedHashMap)", "MAP");
                remapTypes.put("JavaType(class org.apache.calcite.adapter.file.ComparableLinkedHashMap)", "MAP");
                remapTypes.put("JavaType(class java.lang.String)", "VARCHAR");
                remapTypes.put("JavaType(class java.lang.Integer)", "INTEGER");
                remapTypes.put("INTEGER NOT NULL", "INTEGER");
                remapTypes.put("INTEGER", "INTEGER");
                remapTypes.put("MAP NOT NULL", "MAP");
                remapTypes.put("ARRAY NOT NULL", "ARRAY");
                remapTypes.put("JSON", "JSON");
                remapTypes.put("JSONB", "JSON");
                remapTypes.put("SMALLINT NOT NULL", "INTEGER");
                remapTypes.put("SMALLINT", "INTEGER");
                remapTypes.put("TINYINT NOT NULL", "INTEGER");
                remapTypes.put("TINYINT", "INTEGER");
                remapTypes.put("BIGINT NOT NULL", "INTEGER");
                remapTypes.put("BIGINT", "INTEGER");
                remapTypes.put("FLOAT NOT NULL", "FLOAT");
                remapTypes.put("FLOAT", "FLOAT");
                remapTypes.put("DOUBLE NOT NULL", "DOUBLE");
                remapTypes.put("DOUBLE", "DOUBLE");
                remapTypes.put("BOOLEAN NOT NULL", "BOOLEAN");
                remapTypes.put("BOOLEAN", "BOOLEAN");
                remapTypes.put("VARBINARY NOT NULL", "VARBINARY");
                remapTypes.put("VARBINARY", "VARBINARY");
                remapTypes.put("BINARY NOT NULL", "BINARY");
                remapTypes.put("BINARY", "BINARY");
                remapTypes.put("DATE NOT NULL", "DATE");
                remapTypes.put("DATE", "DATE");
                remapTypes.put("TIME(0) NOT NULL", "TIME");
                remapTypes.put("TIME(0)", "TIME");
                remapTypes.put("TIMESTAMP(0) NOT NULL", "TIMESTAMP");
                remapTypes.put("TIMESTAMP(0)", "TIMESTAMP");
                remapTypes.put("TIMESTAMP(3) NOT NULL", "TIMESTAMP");
                remapTypes.put("TIMESTAMP(3)", "TIMESTAMP");
                remapTypes.put("TIMESTAMP NOT NULL", "TIMESTAMPTZ");
                remapTypes.put("TIMESTAMP", "TIMESTAMPTZ");
                remapTypes.put("DECIMAL(10,2)", "FLOAT");
                remapTypes.put("DECIMAL(12,2)", "FLOAT");

                String mappedType = remapTypes.get(dataTypeName);
                if (mappedType == null) {
                    if (dataTypeName.toLowerCase().contains("varchar") && !dataTypeName.toLowerCase().endsWith("map")) {
                        mappedType = "VARCHAR";
                    } else if (dataTypeName.toLowerCase().contains("timestamp")) {
                        mappedType = "TIMESTAMP";
                    } else if (dataTypeName.toLowerCase().contains("decimal")) {
                        mappedType = "FLOAT";
                    } else if (dataTypeName.toLowerCase().startsWith("any")) {
                        mappedType = "VARBINARY";
                    } else if (dataTypeName.toLowerCase().endsWith("map")) {
                        mappedType = "MAP";
                    } else if (dataTypeName.toLowerCase().endsWith("array")) {
                        mappedType = "LIST";
                    } else if (dataTypeName.toLowerCase().contains("for json")) {
                        mappedType = "JSON";
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
        Tracer tracer = openTelemetry.getTracer("calcite-driver");
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
     * @param query        The SQL query to execute.
     * @return A JSON string representing the result of the query.
     */
    public String queryModels(String query) {
        return queryModels(query, "", "");
    }

    /**
     * Executes a SQL query on the database and returns the result as a JSON string.
     *
     * @param query The SQL query to execute.
     * @return A JSON string representing the result of the query.
     */
    public String queryModels(String query, String parentTraceId, String parentSpanId) {
        Tracer tracer = openTelemetry.getTracer("calcite-driver");
        SpanContext parentSpanContext = SpanContext.createFromRemoteParent(
                parentTraceId,
                parentSpanId,
                TraceFlags.getDefault(),
                TraceState.getDefault()
            );
        Context context = Context.current().with(Span.wrap(parentSpanContext));
        Span span = tracer.spanBuilder("queryModels")
                .setParent(context)
                .startSpan();
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
                // Java's inbuilt DateTimeFormatter doesn't have any predefined format for RFC 3339
                DateTimeFormatter rfcFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ENGLISH).withZone(ZoneId.of("UTC"));
                DateTimeFormatter rfcDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH).withZone(ZoneId.of("UTC"));

                List<Map<String, Object>> rows = new ArrayList<>();

                try {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    while (resultSet.next()) {
                        Map<String, Object> columns = new LinkedHashMap<>();
                        for (int i = 1; i <= columnCount; i++) {
                            Object value = resultSet.getObject(i);

                            // handling Dates and Timestamps
                            if (value instanceof java.sql.Date) {
                                java.sql.Date sqlDate = (java.sql.Date) value;
                                java.util.Date utilDate = new java.util.Date(sqlDate.getTime());
                                String rfcDateString = rfcDateFormat.format(utilDate.toInstant());
                                columns.put(metaData.getColumnLabel(i), rfcDateString);
                            } else if (value instanceof java.sql.Timestamp) {
                                java.sql.Timestamp sqlTimestamp = (java.sql.Timestamp) value;
                                java.util.Date utilDate = new java.util.Date(sqlTimestamp.getTime());
                                String rfcDateString = rfcFormat.format(utilDate.toInstant());
                                columns.put(metaData.getColumnLabel(i), rfcDateString);
                            } else if (value instanceof ArrayImpl) {
                                columns.put(metaData.getColumnLabel(i), ((ArrayImpl) value).getArray());
                            }
                            // if it is not date - put the value directly
                            else {
                                columns.put(metaData.getColumnLabel(i), resultSet.getObject(i));
                            }
                        }
                        rows.add(columns);
                    }
                } catch(Throwable e) {
                    e.printStackTrace();
                } finally {
                    resultSet.close();
                }

                statement.close();
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                String result = gson.toJson(rows);
                span.setAttribute("Rows returned", rows.size());
                span.setStatus(StatusCode.OK);
                return result;
            }
        } catch (Throwable e) {
            span.setStatus(StatusCode.ERROR);
            span.setAttribute("Error", e.toString());
            return "{\"error\":\"" + e + "\"}";
        } finally {
            span.end();
        }
    }

    public String queryPlanModels(String query, String parentTraceId, String parentSpanId) {
        Tracer tracer = openTelemetry.getTracer("calcite-driver");
        SpanContext parentSpanContext = SpanContext.createFromRemoteParent(
                parentTraceId,
                parentSpanId,
                TraceFlags.getDefault(),
                TraceState.getDefault()
        );
        Context context = Context.current().with(Span.wrap(parentSpanContext));
        Span span = tracer.spanBuilder("queryPlanModels")
                .setParent(context)
                .startSpan();
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
