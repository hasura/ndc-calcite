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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.units.qual.C;

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
    ArrayList<String> primaryKeys = new ArrayList<String>();
    ArrayList<ExportedKey> exportedKeys = new ArrayList<ExportedKey>();
    Map<String, ColumnMetadata> columns = new HashMap<>();
}

/**
 * Represents a class that interacts with a Calcite database using JDBC.
 */
public class CalciteQuery {

    private static final Logger logger = LogManager.getLogger(CalciteQuery.class);
    Connection connection;
    CalciteSchema rootSchema;
    private static final OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
    private static final Tracer tracer = openTelemetry.getTracer("calcite-driver");
    private static final Gson gson = new Gson();

    /**
     * Creates a Calcite connection using the provided model file.
     *
     * @param modelPath The path to the model file.
     * @return The created Calcite connection.
     */
    public Connection createCalciteConnection(String modelPath) {
        Span span = tracer.spanBuilder("createCalciteConnection").startSpan();
        span.addEvent(modelPath);
        logger.info(String.format("Using this model file: [%s]", modelPath));
        Properties info = new Properties();
        info.setProperty("model", modelPath);
        try {
            connection = DriverManager.getConnection("jdbc:calcite:conformance=ORACLE_12", info);
            rootSchema = connection.unwrap(CalciteConnection.class).getRootSchema().unwrap(CalciteSchema.class);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        span.end();
        return connection;
    }

    private Collection<TableMetadata> getTables() {
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
                            Schema schema = schemaPlus.schema;
                            DatabaseMetaData metaData1;
                            if (schema instanceof JdbcSchema) {
                                metaData1 = ((JdbcSchema) schema).getDataSource().getConnection().getMetaData();
                            } else {
                                metaData1 = metaData;
                            }
                            for( String tableType: new String[]{"TABLE", "VIEW"}) {
                                try (ResultSet tables = metaData.getTables(catalog, schemaName, null, new String[]{tableType})) {
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
                                        list.add(new TableMetadata(catalog, schemaName, tableName, remarks, primaryKeys, exportedKeys));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return list;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, ColumnMetadata> getTableColumnInfo(TableMetadata table) {
        Map<String, ColumnMetadata> columns = new HashMap<>();
        ResultSet columnsSet;
        try {
            DatabaseMetaData metaData = connection.getMetaData();
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
                        entry("JavaType(class java.util.LinkedHashMap)", "MAP"),
                        entry("JavaType(class java.lang.String)", "VARCHAR"),
                        entry("JavaType(class java.lang.Integer)", "INTEGER"),
                        entry("INTEGER NOT NULL", "INTEGER"),
                        entry("INTEGER", "INTEGER"),
                        entry("SMALLINT NOT NULL", "INTEGER"),
                        entry("SMALLINT", "INTEGER"),
                        entry("TINYINT NOT NULL", "INTEGER"),
                        entry("TINYINT", "INTEGER"),
                        entry("BIGINT NOT NULL", "BIGINT"),
                        entry("BIGINT", "BIGINT"),
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
                        entry("TIMESTAMP", "TIMESTAMPTZ")
                );
                String mappedType = remapTypes.get(dataTypeName);
                if (mappedType == null) {
                    mappedType = "VARCHAR";
                }
                columns.put(columnName, new ColumnMetadata(
                        columnName,
                        mappedType,
                        nullable,
                        description
                ));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return columns;
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
        Gson gson = new Gson();
        Span span = tracer.spanBuilder("getModels").startSpan();
        Map<String,TableMetadata> result = new HashMap<>();
        Collection<TableMetadata> tables = getTables();
        for (TableMetadata table : tables) {
            table.columns = getTableColumnInfo(table);
            result.put(table.name, table);
        }
        span.end();
        return gson.toJson(result);
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
            PreparedStatement preparedStatement = StatementPreparer.prepare(query, connection);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (query.toLowerCase().startsWith("select json_object(")) {
                ArrayList<String> rows = new ArrayList<>();
                while (resultSet.next()) {
                    rows.add(resultSet.getString(1));
                }
                resultSet.close();
                statement.close();
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                String result = gson.toJson(rows);
                span.setStatus(StatusCode.OK);
                return result;
            } else {
                JsonArray jsonArray = new JsonArray();
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                ArrayList<String> results = new ArrayList<>();
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
                            case Types.BIGINT:
                            case Types.DECIMAL:
                            case Types.BINARY:
                                jsonObject.addProperty(label, resultSet.getString(i));
                                break;
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
                    jsonArray.add(jsonObject);
                }
                resultSet.close();
                statement.close();
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                String result = gson.toJson(jsonArray);
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
}
