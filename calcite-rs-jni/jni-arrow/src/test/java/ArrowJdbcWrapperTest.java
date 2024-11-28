import com.hasura.ArrowJdbcWrapper;
import com.hasura.ArrowResultSet;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ArrowJdbcWrapperTest {

    private static final String JDBC_URL = "jdbc:graphql:http://localhost:3280/graphql?role=admin";
    private static final String USERNAME = null;
    private static final String PASSWORD = "";
    private ArrowJdbcWrapper wrapper;

    @BeforeAll
    public void setup() throws SQLException {
        wrapper = new ArrowJdbcWrapper(JDBC_URL, USERNAME, PASSWORD);
    }

    @Test
    public void testGetTablesAndViews() {
        try {
            System.out.println("Testing getTables and getTablesAndViews...");

            // Test getting all tables and views
            System.out.println("\n1. Getting all tables and views:");
            try (VectorSchemaRoot tablesAndViews = wrapper.getTablesAndViews(null, "GRAPHQL", null)) {
                printTableResults(tablesAndViews);
            }

            // Test getting tables matching a pattern
            System.out.println("\n2. Getting tables starting with 'a':");
            try (VectorSchemaRoot aTables = wrapper.getTables(null, "GRAPHQL", "a%", new String[]{"TABLE"})) {
                printTableResults(aTables);
            }

            // Test getting only views
            System.out.println("\n3. Getting only views:");
            try (VectorSchemaRoot views = wrapper.getTables(null, "GRAPHQL", null, new String[]{"VIEW"})) {
                printTableResults(views);
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed with exception: " + e.getMessage());
        }
    }

    @Test
    public void testGetColumns() {
        try {
            System.out.println("\nTesting getColumns with different patterns...");

            // Get columns for a specific table
            System.out.println("\n1. Getting columns for album table:");
            try (VectorSchemaRoot albumColumns = wrapper.getColumns(null, "GRAPHQL", "Album", null)) {
                printColumnResults(albumColumns);
            }

            // Get specific columns across all tables
            System.out.println("\n2. Getting all 'id' columns across tables:");
            try (VectorSchemaRoot idColumns = wrapper.getColumns(null, "GRAPHQL", "%", "id")) {
                printColumnResults(idColumns);
            }

            // Get columns matching a pattern in specific tables
            System.out.println("\n3. Getting columns starting with 'a' in tables starting with 'A':");
            try (VectorSchemaRoot nameColumns = wrapper.getColumns(null, "GRAPHQL", "A%", "a%")) {
                printColumnResults(nameColumns);
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed with exception: " + e.getMessage());
        }
    }

    @Test
    public void testBatchedQuery() {
        try {
            System.out.println("\nTesting batched query execution...");
            String query = "SELECT * FROM \"Actor\"";

            try (ArrowResultSet resultSet = wrapper.executeQueryBatched(query, 5)) {
                int batchCount = 0;
                int totalRows = 0;

                while (resultSet.hasNext()) {
                    try (VectorSchemaRoot batch = resultSet.nextBatch()) {
                        batchCount++;
                        totalRows += batch.getRowCount();

                        System.out.printf("Batch %d: %d rows%n", batchCount, batch.getRowCount());

                        // Print first row of each batch
                        if (batch.getRowCount() > 0) {
                            System.out.println("First row in batch:");
                            printRow(batch, 0);
                        }
                    }
                }

                System.out.printf("Total batches: %d, Total rows: %d%n", batchCount, totalRows);
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed with exception: " + e.getMessage());
        }
    }

    private void printTableResults(VectorSchemaRoot tables) {
        VarCharVector tableNames = (VarCharVector) tables.getVector("TABLE_NAME");
        VarCharVector tableTypes = (VarCharVector) tables.getVector("TABLE_TYPE");
        VarCharVector tableSchemas = (VarCharVector) tables.getVector("TABLE_SCHEM");

        System.out.printf("Found %d results:%n", tables.getRowCount());
        for (int i = 0; i < tables.getRowCount(); i++) {
            String schema = tableSchemas.isNull(i) ? "null" : new String(tableSchemas.get(i));
            String name = new String(tableNames.get(i));
            String type = new String(tableTypes.get(i));
            System.out.printf("%s.%s (%s)%n", schema, name, type);
        }
    }

    private void printColumnResults(VectorSchemaRoot columns) {
        VarCharVector tableNames = (VarCharVector) columns.getVector("TABLE_NAME");
        VarCharVector columnNames = (VarCharVector) columns.getVector("COLUMN_NAME");
        VarCharVector typeNames = (VarCharVector) columns.getVector("TYPE_NAME");
        IntVector columnSizes = (IntVector) columns.getVector("COLUMN_SIZE");

        System.out.printf("Found %d columns:%n", columns.getRowCount());
        for (int i = 0; i < columns.getRowCount(); i++) {
            String tableName = new String(tableNames.get(i));
            String columnName = new String(columnNames.get(i));
            String typeName = new String(typeNames.get(i));
            int size = columnSizes.get(i);
            System.out.printf("%s.%s (%s[%d])%n", tableName, columnName, typeName, size);
        }
    }

    private void printRow(VectorSchemaRoot batch, int rowIndex) {
        for (int i = 0; i < batch.getFieldVectors().size(); i++) {
            String fieldName = batch.getFieldVectors().get(i).getName();
            Object value = batch.getFieldVectors().get(i).getObject(rowIndex);
            System.out.printf("%s: %s, ", fieldName, value);
        }
        System.out.println();
    }
}