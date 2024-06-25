package org.kenstott;
import com.google.gson.*;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.ArrayList;
import java.util.List;
import java.sql.*;
import java.util.*;
import static java.util.Map.entry;

public class CalciteQuery {

    private static final Logger logger = LogManager.getLogger(CalciteQuery.class);
    Connection connection;
    private static final OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
    private static final Tracer tracer = openTelemetry.getTracer("calcite-driver");
    private static final Gson gson = new Gson();

    public Connection createCalciteConnection(String modelPath) {
        Span span = tracer.spanBuilder("createCalciteConnection").startSpan();
        span.addEvent(modelPath);
        logger.info(String.format("Using this model file: [%s]", modelPath));
        Properties info = new Properties();
        info.setProperty("model", modelPath);
        try {
            connection = DriverManager.getConnection("jdbc:calcite:", info);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        span.end();
        return connection;
    }

    private Collection<String> getTableNames() {
        DatabaseMetaData metaData;
        try {
            metaData = connection.getMetaData();
            List<String> list = new ArrayList<>();
            try (ResultSet tables = metaData.getTables(null, null, null, new String[]{"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    list.add(tableName);
                }
                return list;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> getTableColumnInfo(String tableName) {
        Map<String, String> columns = new HashMap<>();
        ResultSet columnsSet;
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            columnsSet = metaData.getColumns(null, null, tableName, null);
            while (columnsSet.next()) {
                String columnName = columnsSet.getString("COLUMN_NAME");
                String dataTypeName = columnsSet.getString("TYPE_NAME");
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
                columns.put(columnName, mappedType);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return columns;
    }

    public String getModels() {
        Span span = tracer.spanBuilder("getModels").startSpan();
        Map<String, Map<String, String>> result = new HashMap<>();
        Collection<String> tableNames = getTableNames();
        for (String tableName : tableNames) {
            result.put(tableName, getTableColumnInfo(tableName));
        }
        Gson gson = new Gson();
        span.end();
        return gson.toJson(result);
    }


    public String queryModels(String query) {
        Span span = tracer.spanBuilder("queryModels").startSpan();
        try {
            Statement statement = connection.createStatement();
            PreparedStatement preparedStatement = StatementPreparer.prepare(query, connection);
            ResultSet resultSet = preparedStatement.executeQuery();
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
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR);
            span.setAttribute("Error", e.toString());
            return "{\"error\":\"" + e + "\"}";
        } finally {
            span.end();
        }
    }
}
