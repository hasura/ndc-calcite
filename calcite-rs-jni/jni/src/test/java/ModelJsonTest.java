import com.google.gson.*;

import java.sql.Connection;

import com.hasura.CalciteQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ModelJsonTest {
    private CalciteQuery query;

    @BeforeAll
    void setup() {
        query = new CalciteQuery();
    }

    Path getModelPath(String name) {
        return Paths.get("./src/test/resources/adapters/" + name + "/model.json");
    }

    @Test
    void testArrowModelJson() {
        testModelSchema(getModelPath("arrow"));
    }

    @Test
    void testBigQueryModelJson() {
        testModelSchema(getModelPath("bigquery"));
    }

    @Test
    void testCassandraModelJson() {
        testModelSchema(getModelPath("cassandra"));
    }

    @Test
    void testCsvModelJson() {
        testModelSchema(getModelPath("csv"));
    }

    @Test
    void testDatabricksModelJson() {
        testModelSchema(getModelPath("databricks"));
    }

    @Test
    void testFileModelJson() {
        testModelSchema(getModelPath("file"));
    }

    @Test
    void testGraphqlModelJson() {
        testModelSchema(getModelPath("graphql"));
    }

    @Test
    void testH2ModelJson() {
        testModelSchema(getModelPath("h2"));
    }

    @Test
    void testHiveModelJson() {
        testModelSchema(getModelPath("hive"));
    }

    @Test
    void testJdbcModelJson() {
        testModelSchema(getModelPath("jdbc"));
    }

    @Test
    void testKafkaModelJson() {
        testModelSchema(getModelPath("kafka"));
    }

    @Test
    void testOsModelJson() {
        testModelSchema(getModelPath("os"));
    }

    @Test
    void testRedisModelJson() {
        testModelSchema(getModelPath("redis"));
    }

    @Test
    void testRedshiftModelJson() {
        testModelSchema(getModelPath("redshift"));
    }

    @Test
    void testSnowflakeModelJson() {
        testModelSchema(getModelPath("snowflake"));
    }

    @Test
    void testSplunkModelJson() {
        testModelSchema(getModelPath("splunk"));
    }

    @Test
    void testSybaseModelJson() {
        testModelSchema(getModelPath("sybase"));
    }

    @Test
    void testTrinoModelJson() {
        testModelSchema(getModelPath("trino"));
    }

    private void testModelSchema(Path modelJsonPath) {
        try {
            System.out.println("\nTesting model: " + modelJsonPath);

            Connection calciteConnection = query.createCalciteConnection(modelJsonPath.toString());
            System.out.println("Connection created successfully");

            String schema = query.getModels();
            assertNotNull(schema, "Schema should not be null for " + modelJsonPath);

            System.out.println("Schema loaded successfully:");
            // Parse and print the schema
            JsonObject schemaJson = JsonParser.parseString(schema).getAsJsonObject();
            System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(schemaJson));

            String randomQuery = generateRandomQuery(schema, 5);
            System.out.println("Generated query: " + randomQuery);
            assertNotNull(randomQuery, "Generated query should not be null");

            QueryResult result = executeQuery(randomQuery);
            System.out.println("Query executed successfully. Row count: " + result.getRowCount());
            assertNotNull(result, "Query result should not be null");
            assertTrue(result.getRowCount() <= 5, "Result should have 5 or fewer rows");

            calciteConnection.close();
            System.out.println("Connection closed successfully\n");

        } catch (Exception e) {
            System.err.println("Error testing " + modelJsonPath + ": " + e.getMessage());
            e.printStackTrace();
            fail("Failed to test model.json at " + modelJsonPath + ": " + e.getMessage());
        }
    }

    private String generateRandomQuery(String schemaStr, int rowLimit) {
        JsonObject schema = new Gson().fromJson(schemaStr, JsonObject.class);
        String[] tables = schema.keySet().toArray(new String[0]);

        String table = tables[new Random().nextInt(tables.length)];
        JsonObject tableSchema = schema.getAsJsonObject(table);

        JsonObject columns = tableSchema.getAsJsonObject("columns");
        String[] columnNames = columns.keySet().toArray(new String[0]);

        int numColumns = 2 + new Random().nextInt(4);
        Set<String> selectedColumns = new HashSet<>();
        while (selectedColumns.size() < numColumns) {
            selectedColumns.add(columnNames[new Random().nextInt(columnNames.length)]);
        }

        return String.format("SELECT %s FROM \"%s\" LIMIT %d",
                String.join(", ", selectedColumns),
                table,
                rowLimit
        );
    }

    private QueryResult executeQuery(String query) {
        try {
            String jsonResult = this.query.queryModels(query);
            JsonArray results = new Gson().fromJson(jsonResult, JsonArray.class);
            System.out.println("Query results:");
            System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(results));
            QueryResult queryResult = new QueryResult();
            queryResult.rowCount = results.size();
            return queryResult;
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute query: " + query, e);
        }
    }

    private class QueryResult {
        private int rowCount = 0;

        public int getRowCount() {
            return rowCount;
        }
    }
}