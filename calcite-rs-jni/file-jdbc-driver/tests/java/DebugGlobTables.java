import java.sql.*;
import java.util.Properties;

public class DebugGlobTables {
    public static void main(String[] args) {
        System.out.println("=== Debug Glob Table Discovery ===");
        
        // Register the driver
        try {
            Class.forName("com.hasura.file.FileDriver");
            System.out.println("✅ Driver registered");
        } catch (ClassNotFoundException e) {
            System.err.println("❌ Failed to register driver: " + e.getMessage());
            return;
        }
        
        debugArrowGlob();
    }
    
    private static void debugArrowGlob() {
        System.out.println("\n--- Debugging Arrow Glob Tables ---");
        String url = "jdbc:file://multi?path=/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data&glob=**/*.arrow";
        
        try {
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                System.out.println("✅ Connection succeeded");
                
                // List ALL schemas
                DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet schemas = metaData.getSchemas()) {
                    System.out.println("\nAll schemas:");
                    while (schemas.next()) {
                        String schemaName = schemas.getString("TABLE_SCHEM");
                        System.out.println("  Schema: " + schemaName);
                        
                        // List tables in this schema
                        try (ResultSet tables = metaData.getTables(null, schemaName, "%", new String[]{"TABLE"})) {
                            while (tables.next()) {
                                String tableName = tables.getString("TABLE_NAME");
                                System.out.println("    Table: " + tableName);
                            }
                        }
                    }
                }
                
                // Now check files schema specifically
                System.out.println("\nTables in 'files' schema:");
                try (ResultSet tables = metaData.getTables(null, "files", "%", new String[]{"TABLE"})) {
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  Table: " + tableName);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}