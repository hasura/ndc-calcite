package com.hasura;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.DateUnit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static com.hasura.ArrowJdbcWrapper.*;

public class ArrowResultSet implements AutoCloseable {
    private final ResultSet resultSet;
    private final BufferAllocator allocator;
    private final List<Field> fields;
    private final List<FieldVector> vectors;
    private final int batchSize;
    private boolean hasMoreData;
    private VectorSchemaRoot currentBatch;
    private static final Logger logger = Logger.getLogger(ArrowJdbcWrapper.class.getName());

    public ArrowResultSet(ResultSet resultSet, BufferAllocator allocator, int batchSize) throws SQLException {
        this.resultSet = resultSet;
        this.allocator = allocator;
        this.batchSize = batchSize;
        this.hasMoreData = true;

        // Initialize schema and vectors
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        logger.info("Debug ResultSetMetaData:");
        for (int i = 1; i <= columnCount; i++) {
            logger.info("Column " + i + ":");
            logger.info("  Name: " + metaData.getColumnName(i));
            logger.info("  Type: " + metaData.getColumnType(i));
            logger.info("  TypeName: " + metaData.getColumnTypeName(i));
            logger.info("  ClassName: " + metaData.getColumnClassName(i));
            logger.info("  Precision: " + metaData.getPrecision(i));
            logger.info("  Scale: " + metaData.getScale(i));
            logger.info("  Nullable: " + metaData.isNullable(i));
        }

        this.fields = new ArrayList<>();
        this.vectors = new ArrayList<>();

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            int dataType = metaData.getColumnType(i);
            ArrowType arrowType = mapJdbcToArrowType(dataType);
            Field field = new Field(columnName, new FieldType(true, arrowType, null, getMetadataMap(metaData, i)), null);
            fields.add(field);
            FieldVector vector = createVector(field, allocator);
            vectors.add(vector);
        }

        this.currentBatch = new VectorSchemaRoot(fields, vectors);
    }

    public Schema getSchema() {
        return new Schema(fields);
    }

    public boolean hasNext() {
        return hasMoreData;
    }

    public VectorSchemaRoot nextBatch() throws SQLException {
        // Clear the previous batch
        currentBatch.clear();
        currentBatch.allocateNew();

        int rowCount = 0;
        while (rowCount < batchSize && resultSet.next()) {
            for (int i = 0; i < vectors.size(); i++) {
                populateVector(vectors.get(i), resultSet, i + 1, rowCount);
            }
            rowCount++;
        }

        // Check if we've reached the end
        hasMoreData = rowCount == batchSize;

        currentBatch.setRowCount(rowCount);
        return currentBatch;
    }

    private void populateVector(FieldVector vector, ResultSet rs, int columnIndex, int rowIndex) throws SQLException {

        try {
            if (rs.getObject(columnIndex) == null) {
                vector.setNull(rowIndex);
                return;
            }
        } catch (Exception ignore) {
            // do nothing
        }

        if (vector instanceof IntVector) {
            ((IntVector) vector).set(rowIndex, rs.getInt(columnIndex));
        } else if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).set(rowIndex, rs.getLong(columnIndex));
        } else if (vector instanceof Float8Vector) {
            try {
                ((Float8Vector) vector).set(rowIndex, rs.getDouble(columnIndex));
            } catch (Exception e) {
                try {
                    ((Float8Vector) vector).set(rowIndex, rs.getLong(columnIndex));
                } catch (Exception e1) {
                    try {
                        int value = rs.getInt(columnIndex);
                        ((Float8Vector) vector).set(rowIndex, (double) value);
                    } catch (Exception e2) {
                        // Handle error when all attempts fail.
                        throw new RuntimeException("Failed to get a float, long or int value.", e2);
                    }
                }
            }
        } else if (vector instanceof Float4Vector) {
            try {
                ((Float4Vector) vector).set(rowIndex, rs.getFloat(columnIndex));
            } catch (Exception e) {
                try {
                    int value = rs.getInt(columnIndex);
                    ((Float4Vector) vector).set(rowIndex, (float) value);
                } catch (Exception e1) {
                    // handle error when all attempts fail
                    throw new RuntimeException("Failed to acquire a float or int value.", e1);
                }
            }
        } else if (vector instanceof LargeVarCharVector) {
            String str = rs.getString(columnIndex);
            if (str != null) {
                byte[] strBytes = str.getBytes(StandardCharsets.UTF_8); // Specify the appropriate charset if necessary
                ((LargeVarCharVector) vector).setSafe(rowIndex, strBytes, 0, strBytes.length);
            } else {
                vector.setNull(rowIndex);
            }
        } else if (vector instanceof LargeVarBinaryVector) {
            try {
                byte[] strBytes = rs.getBytes(columnIndex);
                if (strBytes != null) {
                    ((LargeVarBinaryVector) vector).setSafe(rowIndex, strBytes, 0, strBytes.length);
                } else {
                    vector.setNull(rowIndex);
                }
            } catch(Exception ignore) {
                vector.setNull(rowIndex);
            }
        } else if (vector instanceof DateDayVector) {
            Date date = rs.getDate(columnIndex);
            ((DateDayVector) vector).set(rowIndex, (int) (date.getTime() / (86400000L)));
        } else if (vector instanceof TimeStampMicroVector) {
            Timestamp timestamp = rs.getTimestamp(columnIndex);
            ((TimeStampMicroVector) vector).set(rowIndex, timestamp.getTime() * 1000);
        } else if (vector instanceof BitVector) {
            ((BitVector) vector).set(rowIndex, rs.getBoolean(columnIndex) ? 1 : 0);
        }
    }

    @Override
    public void close() throws Exception {
        if (currentBatch != null) {
            currentBatch.close();
        }
        if (resultSet != null && !resultSet.isClosed()) {
            resultSet.close();
        }
    }
}