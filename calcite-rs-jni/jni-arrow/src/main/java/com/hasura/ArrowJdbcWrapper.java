package com.hasura;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

public class ArrowJdbcWrapper implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(ArrowJdbcWrapper.class.getName());
    private Connection connection = null;
    private BufferAllocator allocator = null;
    private static final int DEFAULT_BATCH_SIZE = 1000;

    static {
        try {
            logger.info("Loading GraphQLDriver class");
            Class.forName("com.hasura.GraphQLDriver");
            logger.info("Successfully loaded GraphQLDriver");
        } catch (ClassNotFoundException e) {
            logger.severe("Failed to load GraphQLDriver: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public ArrowJdbcWrapper(String jdbcUrl, String username, String password) throws SQLException {
        this();
        setConnection(jdbcUrl, username, password);
    }

    public ArrowJdbcWrapper() {
        logger.info("Creating database connection");
        this.allocator = new RootAllocator();
        logger.info("Created RootAllocator");
    }

    public void setConnection(String jdbcUrl, String username, String password) throws SQLException {
        logger.info("Initializing ArrowJdbcWrapper with URL: " + jdbcUrl);
        try {
            this.connection = DriverManager.getConnection(jdbcUrl, username, password);
            logger.info("Successfully established database connection");
        } catch (SQLException e) {
            logger.severe("Failed to establish connection: " + e.getMessage());
            throw e;
        }
    }

    public ArrowResultSet executeQueryBatched(String query) throws SQLException {
        logger.info("Executing batched query: " + query);
        return executeQueryBatched(query, DEFAULT_BATCH_SIZE);
    }

    public ArrowResultSet executeQueryBatched(String query, int batchSize) throws SQLException {
        logger.info(String.format("Executing batched query with size %d: %s", batchSize, query));
        try {
            Statement stmt = connection.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY
            );

            // Set fetch size for memory efficiency
            stmt.setFetchSize(batchSize);
            logger.info("Set fetch size to: " + batchSize);

            ResultSet rs = stmt.executeQuery(query);
            logger.info("Successfully executed query");

            return new ArrowResultSet(rs, allocator, batchSize);
        } catch (SQLException e) {
            logger.severe("Error executing batched query: " + e.getMessage());
            throw e;
        }
    }

    public VectorSchemaRoot executeQuery(String query) throws Exception {
        logger.info("Executing query: " + query);
        try {
            ArrowResultSet resultSet = executeQueryBatched(query);
            VectorSchemaRoot result = resultSet.nextBatch();
            logger.info("Successfully executed query and got results. Remember to close the vector root to release memory.");
            return result;
        } catch (Exception e) {
            logger.severe("Error executing query: " + e.getMessage());
            throw e;
        }
    }

    public VectorSchemaRoot getTables() throws SQLException {
        logger.info("Getting all tables");
        return getTables(null, null, null, null);
    }

    /**
     * Retrieves information about the tables in the specified database based on the provided criteria.
     *
     * @param catalog The catalog name. Always ignored - catalogs not supported.
     * @param schemaPattern The schema pattern for the tables. If an empty string is provided, it is treated as null.
     * @param tableNamePattern The table name pattern. If an empty string is provided, it is treated as null.
     * @param types An array of table types to include. If empty, defaults to ["TABLE", "VIEW"].
     * @return A VectorSchemaRoot object containing metadata of the retrieved tables.
     * @throws SQLException If a database access error occurs or parameters contain invalid values.
     */
    public VectorSchemaRoot getTables(String catalog, String schemaPattern,
                                      String tableNamePattern, String[] types) throws SQLException {
        if (types.length == 0) {
            types = new String[]{"TABLE", "VIEW"};
        }
        if (schemaPattern != null && schemaPattern.isEmpty()) {
            schemaPattern = null;
        }
        if (tableNamePattern != null && tableNamePattern.isEmpty()) {
            tableNamePattern = null;
        }
        catalog = null;
        logger.info(String.format("Getting tables - catalog: %s, schema: %s, table: %s, types: %s",
                catalog != null ? catalog : "",
                schemaPattern != null ? schemaPattern : "",
                tableNamePattern != null ? tableNamePattern : "",
                types != null ? String.join(",", types) : "null"));

        DatabaseMetaData dbMetaData = connection.getMetaData();
        logger.info("Retrieved database metadata");

        VectorSchemaRoot root;
        ResultSet tables = dbMetaData.getTables(catalog, schemaPattern, tableNamePattern, types);
        ArrowResultSet results = new ArrowResultSet(tables, allocator, 1000000);
        return results.nextBatch();
    }

    public VectorSchemaRoot getTablesAndViews(String catalog, String schemaPattern,
                                              String tableNamePattern) throws SQLException {
        logger.info(String.format("Getting tables and views - catalog: %s, schema: %s, table: %s",
                catalog, schemaPattern, tableNamePattern));
        return getTables(catalog, schemaPattern, tableNamePattern,
                new String[]{"TABLE", "VIEW"});
    }

    public VectorSchemaRoot getColumns(String tableName) throws SQLException {
        logger.info("Getting columns for table: " + tableName);
        return getColumns(null, null, tableName, null);
    }

    public VectorSchemaRoot getColumns(String catalog, String schemaPattern,
                                       String tableNamePattern, String columnNamePattern) throws SQLException {

        if (schemaPattern != null && schemaPattern.isEmpty()) {
            schemaPattern = null;
        }
        if (tableNamePattern != null && tableNamePattern.isEmpty()) {
            tableNamePattern = null;
        }
        if (columnNamePattern != null && columnNamePattern.isEmpty()) {
            columnNamePattern = null;
        }
        catalog = null;

        logger.info(String.format("Getting columns - catalog: %s, schema: %s, table: %s, column: %s",
                catalog, schemaPattern, tableNamePattern, columnNamePattern));

        DatabaseMetaData metaData = connection.getMetaData();
        logger.info("Retrieved database metadata");

        ResultSet columns = metaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        ArrowResultSet results = new ArrowResultSet(columns, allocator, 1000000);
        return results.nextBatch();
    }

    @Override
    public void close() throws Exception {
        logger.info("Closing ArrowJdbcWrapper");
        if (connection != null && !connection.isClosed()) {
            connection.close();
            logger.info("Closed database connection");
        }
        if (allocator != null) {
            allocator.close();
            logger.info("Closed allocator");
        }
    }

    public boolean healthCheck() {
        logger.info("Performing health check");
        return true;
    }

    public static String[][] getMetadataFromField(Field field) {
        FieldType fieldType = field.getFieldType();
        Map<String, String> metadata = fieldType.getMetadata();

        if (metadata == null || metadata.isEmpty()) {
            return new String[0][0];
        }

        return metadata.entrySet().stream()
                .map(entry -> new String[]{entry.getKey(), entry.getValue()})
                .toArray(String[][]::new);
    }

    public static Map<String, String> getMetadataMap(ResultSetMetaData metaData, int columnIndex) throws SQLException {
        Map<String, String> map = new HashMap<>();
        int jdbcType = metaData.getColumnType(columnIndex);

        // Basic boolean attributes
        map.put("AutoIncrement", String.valueOf(metaData.isAutoIncrement(columnIndex)));
        map.put("CaseSensitive", String.valueOf(metaData.isCaseSensitive(columnIndex)));
        map.put("Currency", String.valueOf(metaData.isCurrency(columnIndex)));
        map.put("DefinitelyWritable", String.valueOf(metaData.isDefinitelyWritable(columnIndex)));
        map.put("ReadOnly", String.valueOf(metaData.isReadOnly(columnIndex)));
        map.put("Searchable", String.valueOf(metaData.isSearchable(columnIndex)));
        map.put("Signed", String.valueOf(metaData.isSigned(columnIndex)));
        map.put("Writable", String.valueOf(metaData.isWritable(columnIndex)));

        // Enhanced type name handling
        String typeName = getProperTypeName(jdbcType, metaData.getColumnTypeName(columnIndex));
        map.put("ColumnTypeName", typeName);

        // Enhanced nullable handling
        int nullableValue = getProperNullable(metaData.isNullable(columnIndex));
        map.put("Nullable", String.valueOf(nullableValue));

        // Column size and display size handling
        int columnSize = getProperColumnSize(jdbcType, metaData.getColumnDisplaySize(columnIndex),
                metaData.getPrecision(columnIndex));
        map.put("ColumnSize", String.valueOf(columnSize));
        map.put("ColumnDisplaySize", String.valueOf(getProperDisplaySize(jdbcType, columnSize,
                metaData.getPrecision(columnIndex),
                metaData.getScale(columnIndex))));

        // Enhanced precision and scale handling
        map.put("Precision", String.valueOf(getProperPrecision(jdbcType, metaData.getPrecision(columnIndex))));
        map.put("Scale", String.valueOf(getProperScale(jdbcType, metaData.getScale(columnIndex))));

        // Standard naming attributes
        map.put("CatalogName", metaData.getCatalogName(columnIndex));
        map.put("ColumnClassName", metaData.getColumnClassName(columnIndex));
        map.put("ColumnLabel", metaData.getColumnLabel(columnIndex));
        map.put("ColumnName", metaData.getColumnName(columnIndex));
        map.put("ColumnType", String.valueOf(jdbcType));
        map.put("SchemaName", metaData.getSchemaName(columnIndex));
        map.put("TableName", metaData.getTableName(columnIndex));

        // Add ODBC-specific length information
        map.put("OctetLength", String.valueOf(getOctetLength(jdbcType, columnSize)));

        return map;
    }

    private static String getProperTypeName(int jdbcType, String defaultTypeName) {
        switch (jdbcType) {
            case Types.INTEGER: return "INTEGER";
            case Types.BIGINT: return "BIGINT";
            case Types.SMALLINT: return "SMALLINT";
            case Types.TINYINT: return "TINYINT";
            case Types.REAL: return "REAL";
            case Types.FLOAT:
            case Types.DOUBLE: return "DOUBLE";
            case Types.DECIMAL:
            case Types.NUMERIC: return "DECIMAL";
            case Types.CHAR: return "CHAR";
            case Types.VARCHAR:
            case Types.LONGVARCHAR: return "VARCHAR";
            case Types.DATE: return "DATE";
            case Types.TIME: return "TIME";
            case Types.TIMESTAMP: return "TIMESTAMP";
            case Types.BIT:
            case Types.BOOLEAN: return "BIT";
            case Types.BINARY: return "BINARY";
            case Types.VARBINARY: return "VARBINARY";
            case Types.LONGVARBINARY: return "LONGVARBINARY";
            default: return defaultTypeName;
        }
    }

    private static int getProperNullable(int reportedNullable) {
        switch (reportedNullable) {
            case ResultSetMetaData.columnNoNulls: return 0;    // SQL_NO_NULLS
            case ResultSetMetaData.columnNullable: return 1;   // SQL_NULLABLE
            default: return 2;                                 // SQL_NULLABLE_UNKNOWN
        }
    }

    private static int getProperColumnSize(int jdbcType, int reportedSize, int precision) {
        switch (jdbcType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return reportedSize > 0 ? reportedSize : 255;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return reportedSize > 0 ? reportedSize : 255;

            case Types.INTEGER: return 4;
            case Types.SMALLINT: return 2;
            case Types.BIGINT: return 8;
            case Types.TINYINT: return 1;

            case Types.DECIMAL:
            case Types.NUMERIC:
                return precision > 0 ? precision : 38;

            case Types.BIT:
            case Types.BOOLEAN: return 1;

            case Types.REAL: return 4;
            case Types.FLOAT:
            case Types.DOUBLE: return 8;

            case Types.DATE: return 10;        // YYYY-MM-DD
            case Types.TIME: return 8;         // HH:MM:SS
            case Types.TIMESTAMP: return 23;   // YYYY-MM-DD HH:MM:SS.fff

            default: return reportedSize > 0 ? reportedSize : 255;
        }
    }

    private static int getProperDisplaySize(int jdbcType, int columnSize, int precision, int scale) {
        switch (jdbcType) {
            case Types.INTEGER: return 11;     // -2147483648
            case Types.SMALLINT: return 6;     // -32768
            case Types.TINYINT: return 4;      // -128
            case Types.BIGINT: return 20;      // -9223372036854775808

            case Types.DECIMAL:
            case Types.NUMERIC:
                return precision + 2;          // Add space for sign and decimal point

            case Types.REAL: return 14;        // -1.23457e+12
            case Types.FLOAT:
            case Types.DOUBLE: return 24;      // -1.2345678901234567e+123

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return columnSize;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return columnSize * 2;         // Each byte needs 2 hex chars

            case Types.BIT:
            case Types.BOOLEAN: return 1;

            case Types.DATE: return 10;        // YYYY-MM-DD
            case Types.TIME: return 8;         // HH:MM:SS
            case Types.TIMESTAMP: return 23;   // YYYY-MM-DD HH:MM:SS.fff

            default: return columnSize;
        }
    }

    private static int getProperPrecision(int jdbcType, int reportedPrecision) {
        switch (jdbcType) {
            case Types.DECIMAL:
            case Types.NUMERIC:
                return reportedPrecision > 0 ? reportedPrecision : 38;

            case Types.INTEGER: return 10;
            case Types.SMALLINT: return 5;
            case Types.BIGINT: return 19;
            case Types.TINYINT: return 3;

            case Types.REAL: return 7;
            case Types.FLOAT:
            case Types.DOUBLE: return 15;

            default: return 0;
        }
    }

    private static int getProperScale(int jdbcType, int reportedScale) {
        switch (jdbcType) {
            case Types.DECIMAL:
            case Types.NUMERIC:
                return reportedScale >= 0 ? reportedScale : 0;

            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.BIGINT:
            case Types.TINYINT:
                return 0;

            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return 6;  // Default scale for floating point

            default: return 0;
        }
    }

    private static int getOctetLength(int jdbcType, int columnSize) {
        switch (jdbcType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return columnSize * 2;  // Assuming UTF-16 encoding

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return columnSize;

            case Types.INTEGER: return 4;
            case Types.SMALLINT: return 2;
            case Types.BIGINT: return 8;
            case Types.TINYINT: return 1;

            case Types.REAL: return 4;
            case Types.FLOAT:
            case Types.DOUBLE: return 8;

            case Types.DATE: return 6;
            case Types.TIME: return 6;
            case Types.TIMESTAMP: return 16;

            default: return columnSize;
        }
    }

    public static ArrowType mapJdbcToArrowType(int jdbcType) {
        switch (jdbcType) {
            case Types.INTEGER:
                return new ArrowType.Int(32, true);
            case Types.BIGINT:
                return new ArrowType.Int(64, true);
            case Types.DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case Types.FLOAT:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case Types.VARCHAR:
            case Types.CHAR:
                return new ArrowType.Utf8();
            case Types.DATE:
                return new ArrowType.Date(DateUnit.DAY);
            case Types.TIMESTAMP:
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
            case Types.BOOLEAN:
                return new ArrowType.Bool();
            case Types.BINARY:
                return new ArrowType.Binary();
            default:
                return new ArrowType.Utf8(); // Default to string for unsupported types
        }
    }

    public static FieldVector createVector(Field field, BufferAllocator allocator) {
        ArrowType type = field.getType();
        if (type instanceof ArrowType.Int) {
            return ((ArrowType.Int) type).getBitWidth() == 64 ?
                    new BigIntVector(field, allocator) :
                    new IntVector(field, allocator);
        } else if (type instanceof ArrowType.FloatingPoint) {
            return ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.DOUBLE ?
                    new Float8Vector(field, allocator) :
                    new Float4Vector(field, allocator);
        } else if (type instanceof ArrowType.Utf8) {
            return new LargeVarCharVector(field, allocator);
        } else if (type instanceof ArrowType.Date) {
            return new DateDayVector(field, allocator);
        } else if (type instanceof ArrowType.Timestamp) {
            return new TimeStampMicroVector(field, allocator);
        } else if (type instanceof ArrowType.Bool) {
            return new BitVector(field, allocator);
        } else if (type instanceof ArrowType.Binary) {
            return new LargeVarBinaryVector(field, allocator);
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
}