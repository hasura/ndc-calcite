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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ArrowResultSet implements AutoCloseable {
    private final ResultSet resultSet;
    private final BufferAllocator allocator;
    private final List<Field> fields;
    private final List<FieldVector> vectors;
    private final int batchSize;
    private boolean hasMoreData;
    private VectorSchemaRoot currentBatch;

    public ArrowResultSet(ResultSet resultSet, BufferAllocator allocator, int batchSize) throws SQLException {
        this.resultSet = resultSet;
        this.allocator = allocator;
        this.batchSize = batchSize;
        this.hasMoreData = true;

        // Initialize schema and vectors
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        this.fields = new ArrayList<>();
        this.vectors = new ArrayList<>();

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            int dataType = metaData.getColumnType(i);
            ArrowType arrowType = mapJdbcToArrowType(dataType);

            Field field = new Field(columnName, FieldType.nullable(arrowType), null);
            fields.add(field);

            FieldVector vector = createVector(field);
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

    private ArrowType mapJdbcToArrowType(int jdbcType) {
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
            default:
                return new ArrowType.Utf8(); // Default to string for unsupported types
        }
    }

    private FieldVector createVector(Field field) {
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
            return new VarCharVector(field, allocator);
        } else if (type instanceof ArrowType.Date) {
            return new DateDayVector(field, allocator);
        } else if (type instanceof ArrowType.Timestamp) {
            return new TimeStampMicroVector(field, allocator);
        } else if (type instanceof ArrowType.Bool) {
            return new BitVector(field, allocator);
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    private void populateVector(FieldVector vector, ResultSet rs, int columnIndex, int rowIndex) throws SQLException {
        if (rs.getObject(columnIndex) == null) {
            vector.setNull(rowIndex);
            return;
        }

        if (vector instanceof IntVector) {
            ((IntVector) vector).set(rowIndex, rs.getInt(columnIndex));
        } else if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).set(rowIndex, rs.getLong(columnIndex));
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).set(rowIndex, rs.getDouble(columnIndex));
        } else if (vector instanceof Float4Vector) {
            ((Float4Vector) vector).set(rowIndex, rs.getFloat(columnIndex));
        } else if (vector instanceof VarCharVector) {
            ((VarCharVector) vector).set(rowIndex, rs.getString(columnIndex).getBytes());
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