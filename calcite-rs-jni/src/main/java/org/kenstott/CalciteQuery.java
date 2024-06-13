package org.kenstott;

import com.google.gson.*;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.*;

public class CalciteQuery {

    private static final Logger logger = LogManager.getLogger(CalciteQuery.class);
    Connection connection;
    private static final OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
    private static final Tracer tracer = openTelemetry.getTracer("calcite-driver");
    public Connection createCalciteConnection(String modelPath) {
        Span span = tracer.spanBuilder("createCalciteConnection").startSpan();
        span.addEvent(modelPath);
        logger.info(String.format("Using this model file: [%s]", modelPath));
        Properties info = new Properties();
        info.put("model", modelPath);
        try {
            connection = DriverManager.getConnection("jdbc:calcite:", info);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        span.end();
        return connection;
    }

    public Collection<String> getTableNames() {
        DatabaseMetaData metaData = null;
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

    public Map<String, String> getTableColumnInfo(String tableName) {
        Map<String, String> columns = new HashMap<>();
        ResultSet columnsSet = null;
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            columnsSet = metaData.getColumns(null, null, tableName, null);
            while (columnsSet.next()) {
                String columnName = columnsSet.getString("COLUMN_NAME");
                String dataTypeName = columnsSet.getString("TYPE_NAME");
                columns.put(columnName, dataTypeName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return columns;
    }

    public String getModels() {
        Span span = tracer.spanBuilder("getModels").startSpan();
        Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
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
            ResultSet resultSet = statement.executeQuery(query);
            JsonArray jsonArray = new JsonArray();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                JsonObject jsonObject = new JsonObject();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    int columnType = metaData.getColumnType(i);
                    switch (columnType) {
                        case Types.CHAR:
                        case Types.VARCHAR:
                            jsonObject.addProperty(columnName, resultSet.getString(i));
                            break;
                        case Types.BIGINT:
                        case Types.INTEGER:
                        case Types.SMALLINT:
                        case Types.TINYINT:
                            jsonObject.addProperty(columnName, resultSet.getInt(i));
                            break;
                        case Types.BOOLEAN:
                            jsonObject.addProperty(columnName, resultSet.getBoolean(i));
                            break;
                        case Types.FLOAT:
                            jsonObject.addProperty(columnName, resultSet.getFloat(i));
                            break;
                        case Types.DECIMAL:
                            jsonObject.addProperty(columnName, resultSet.getBigDecimal(i));
                            break;
                        case Types.DATE:
                        case Types.TIMESTAMP:
                            jsonObject.addProperty(columnName, String.valueOf(resultSet.getDate(i)));
                            break;
                        default:
                            Object columnValue = resultSet.getObject(i);
                            jsonObject.addProperty(columnName, columnValue == null ? null : columnValue.toString());
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
            return "[]";
        } finally {
            span.end();
        }
    }
}
