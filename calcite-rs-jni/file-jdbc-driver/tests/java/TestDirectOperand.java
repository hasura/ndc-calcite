import java.sql.*;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.adapter.file.FileSchemaFactory;

public class TestDirectOperand {
    public static void main(String[] args) throws Exception {
        String nestedDir = "/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/nested";
        
        System.out.println("Testing direct operand approach with nested directory: " + nestedDir);
        System.out.println("Directory exists: " + new File(nestedDir).exists());
        
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
             CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            
            // Use EXACT same approach as adapter test
            Map<String, Object> operand = new HashMap<>();
            operand.put("directory", nestedDir);
            operand.put("recursive", true);
            
            System.out.println("Operand map: " + operand);
            
            rootSchema.add("nested_test", FileSchemaFactory.INSTANCE.create(rootSchema, "nested_test", operand));

            try (Statement statement = connection.createStatement()) {
                System.out.println("Checking tables discovered:");
                ResultSet tables = connection.getMetaData().getTables(null, "nested_test", "%", null);
                int count = 0;
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("  - " + tableName);
                    count++;
                }
                System.out.println("Total tables found: " + count);
                
                if (count > 0) {
                    System.out.println("✅ Direct operand approach works!");
                } else {
                    System.out.println("❌ Direct operand approach also fails - might be a data issue");
                }
            }
            
        } catch (Exception e) {
            System.out.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}