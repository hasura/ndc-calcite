package com.hasura;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class ArrowJdbcWrapper implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(ArrowJdbcWrapper.class.getName());
    private final Connection connection;
    private final BufferAllocator allocator;
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
        logger.info("Initializing ArrowJdbcWrapper with URL: " + jdbcUrl);
        try {
            this.connection = DriverManager.getConnection(jdbcUrl, username, password);
            logger.info("Successfully established database connection");
            this.allocator = new RootAllocator();
            logger.info("Created RootAllocator");
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

    public VectorSchemaRoot getTables(String catalog, String schemaPattern,
                                      String tableNamePattern, String[] types) throws SQLException {
        logger.info(String.format("Getting tables - catalog: %s, schema: %s, table: %s, types: %s",
                catalog,
                schemaPattern,
                tableNamePattern,
                types != null ? String.join(",", types) : "null"));

        DatabaseMetaData metaData = connection.getMetaData();
        logger.info("Retrieved database metadata");

        // Create schema for table metadata
        List<Field> fields = Arrays.asList(
                new Field("TABLE_CAT", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("TABLE_SCHEM", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("TABLE_NAME", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("TABLE_TYPE", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("REMARKS", FieldType.nullable(new ArrowType.Utf8()), null)
        );
        logger.info("Created Arrow fields schema");

        // Create vectors
        List<FieldVector> vectors = Arrays.asList(
                new VarCharVector("TABLE_CAT", allocator),
                new VarCharVector("TABLE_SCHEM", allocator),
                new VarCharVector("TABLE_NAME", allocator),
                new VarCharVector("TABLE_TYPE", allocator),
                new VarCharVector("REMARKS", allocator)
        );
        logger.info("Created Arrow vectors");

        VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);
        root.allocateNew();

        try (ResultSet tables = metaData.getTables(catalog, schemaPattern, tableNamePattern, types)) {
            logger.info("Retrieved table metadata ResultSet");
            int rowCount = 0;
            while (tables.next()) {
                logger.fine(String.format("Processing row %d", rowCount));
                setVarCharValue((VarCharVector) vectors.get(0), rowCount, tables.getString("TABLE_CAT"));
                setVarCharValue((VarCharVector) vectors.get(1), rowCount, tables.getString("TABLE_SCHEM"));
                setVarCharValue((VarCharVector) vectors.get(2), rowCount, tables.getString("TABLE_NAME"));
                setVarCharValue((VarCharVector) vectors.get(3), rowCount, tables.getString("TABLE_TYPE"));
                setVarCharValue((VarCharVector) vectors.get(4), rowCount, tables.getString("REMARKS"));
                rowCount++;
            }
            root.setRowCount(rowCount);
            logger.info("Processed " + rowCount + " rows");
        }

        return root;
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
        logger.info(String.format("Getting columns - catalog: %s, schema: %s, table: %s, column: %s",
                catalog, schemaPattern, tableNamePattern, columnNamePattern));

        DatabaseMetaData metaData = connection.getMetaData();
        logger.info("Retrieved database metadata");

        // Define schema for column metadata
        List<Field> fields = Arrays.asList(
                new Field("TABLE_CAT", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("TABLE_SCHEM", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("TABLE_NAME", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("COLUMN_NAME", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("DATA_TYPE", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("TYPE_NAME", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("COLUMN_SIZE", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("BUFFER_LENGTH", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("DECIMAL_DIGITS", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("NUM_PREC_RADIX", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("NULLABLE", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("REMARKS", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("COLUMN_DEF", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("SQL_DATA_TYPE", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("SQL_DATETIME_SUB", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("CHAR_OCTET_LENGTH", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("ORDINAL_POSITION", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("IS_NULLABLE", FieldType.nullable(new ArrowType.Utf8()), null)
        );
        logger.info("Created Arrow fields schema for columns");

        // Create vectors
        List<FieldVector> vectors = Arrays.asList(
                new VarCharVector("TABLE_CAT", allocator),
                new VarCharVector("TABLE_SCHEM", allocator),
                new VarCharVector("TABLE_NAME", allocator),
                new VarCharVector("COLUMN_NAME", allocator),
                new IntVector("DATA_TYPE", allocator),
                new VarCharVector("TYPE_NAME", allocator),
                new IntVector("COLUMN_SIZE", allocator),
                new IntVector("BUFFER_LENGTH", allocator),
                new IntVector("DECIMAL_DIGITS", allocator),
                new IntVector("NUM_PREC_RADIX", allocator),
                new IntVector("NULLABLE", allocator),
                new VarCharVector("REMARKS", allocator),
                new VarCharVector("COLUMN_DEF", allocator),
                new IntVector("SQL_DATA_TYPE", allocator),
                new IntVector("SQL_DATETIME_SUB", allocator),
                new IntVector("CHAR_OCTET_LENGTH", allocator),
                new IntVector("ORDINAL_POSITION", allocator),
                new VarCharVector("IS_NULLABLE", allocator)
        );
        logger.info("Created Arrow vectors for columns");

        VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);
        root.allocateNew();
        logger.info("Created and allocated VectorSchemaRoot");

        try (ResultSet columns = metaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern)) {
            logger.info("Retrieved column metadata ResultSet");
            int rowCount = 0;

            while (columns.next()) {
                logger.fine(String.format("Processing column row %d", rowCount));

                setVarCharValue((VarCharVector) vectors.get(0), rowCount, columns.getString("TABLE_CAT"));
                setVarCharValue((VarCharVector) vectors.get(1), rowCount, columns.getString("TABLE_SCHEM"));
                setVarCharValue((VarCharVector) vectors.get(2), rowCount, columns.getString("TABLE_NAME"));
                setVarCharValue((VarCharVector) vectors.get(3), rowCount, columns.getString("COLUMN_NAME"));
                setIntValue((IntVector) vectors.get(4), rowCount, columns.getInt("DATA_TYPE"));
                setVarCharValue((VarCharVector) vectors.get(5), rowCount, columns.getString("TYPE_NAME"));
                setIntValue((IntVector) vectors.get(6), rowCount, columns.getInt("COLUMN_SIZE"));
                // setIntValue invocation with default value for BUFFER_LENGTH
                setIntValue((IntVector) vectors.get(7), rowCount, columns.getObject("BUFFER_LENGTH") != null ? columns.getInt("BUFFER_LENGTH") : -1);
                setIntValue((IntVector) vectors.get(8), rowCount, columns.getInt("DECIMAL_DIGITS"));
                setIntValue((IntVector) vectors.get(9), rowCount, columns.getInt("NUM_PREC_RADIX"));
                setIntValue((IntVector) vectors.get(10), rowCount, columns.getInt("NULLABLE"));
                setVarCharValue((VarCharVector) vectors.get(11), rowCount, columns.getString("REMARKS"));
                setVarCharValue((VarCharVector) vectors.get(12), rowCount, columns.getString("COLUMN_DEF"));
                // setIntValue invocation with default value for SQL_DATA_TYPE
                setIntValue((IntVector) vectors.get(13), rowCount, columns.getObject("SQL_DATA_TYPE") != null ? columns.getInt("SQL_DATA_TYPE") : -1);
                // setIntValue invocation with default value for SQL_DATETIME_SUB
                setIntValue((IntVector) vectors.get(14), rowCount, columns.getObject("SQL_DATETIME_SUB") != null ? columns.getInt("SQL_DATETIME_SUB") : -1);
                // setIntValue invocation with default value for CHAR_OCTET_LENGTH
                setIntValue((IntVector) vectors.get(15), rowCount, columns.getObject("CHAR_OCTET_LENGTH") != null ? columns.getInt("CHAR_OCTET_LENGTH") : -1);
                setIntValue((IntVector) vectors.get(16), rowCount, columns.getInt("ORDINAL_POSITION"));
                setVarCharValue((VarCharVector) vectors.get(17), rowCount, columns.getString("IS_NULLABLE"));

                rowCount++;
            }
            root.setRowCount(rowCount);
            logger.info("Processed " + rowCount + " columns");
        }

        return root;
    }

    private void setIntValue(IntVector vector, int index, int value) throws SQLException {
        try {
            if (value == 0 && vector.isNull(index)) {
                logger.fine(String.format("Setting NULL for IntVector at index %d", index));
                vector.setNull(index);
            } else {
                logger.fine(String.format("Setting value %d for IntVector at index %d", value, index));
                vector.set(index, value);
            }
        } catch (Exception e) {
            logger.severe(String.format("Error setting int value %d at index %d: %s",
                    value, index, e.getMessage()));
            throw new SQLException("Failed to set int value", e);
        }
    }

    private void setVarCharValue(VarCharVector vector, int index, String value) {
        try {
            if (value == null) {
                logger.fine(String.format("Setting NULL for VarCharVector at index %d", index));
                vector.setNull(index);
            } else {
                logger.fine(String.format("Setting value '%s' for VarCharVector at index %d", value, index));
                vector.set(index, value.getBytes());
            }
        } catch (Exception e) {
            logger.severe(String.format("Error setting varchar value '%s' at index %d: %s",
                    value, index, e.getMessage()));
            throw new RuntimeException("Failed to set varchar value", e);
        }
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
}