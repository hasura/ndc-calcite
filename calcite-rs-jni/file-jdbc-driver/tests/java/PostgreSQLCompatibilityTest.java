import java.sql.*;
import java.util.Properties;

public class PostgreSQLCompatibilityTest {
    public static void main(String[] args) throws Exception {
        Class.forName("com.hasura.file.FileDriver");
        
        // Create a test CSV file for testing
        String testDataDir = System.getenv("TEST_DATA_DIR");
        if (testDataDir == null) {
            testDataDir = "tests/data";
        }
        
        System.out.println("=== PostgreSQL Compatibility Test ===\n");
        
        // Test with a CSV file
        String csvFile = testDataDir + "/csv/sales.csv";
        String url = "jdbc:file://" + new java.io.File(csvFile).getAbsolutePath();
        
        try (Connection conn = DriverManager.getConnection(url)) {
            System.out.println("✅ Connected to: " + url);
            
            // Test 1: Double-quoted identifiers (PostgreSQL standard)
            System.out.println("\n1. Testing double-quoted identifiers:");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT * FROM files.\"sales\" LIMIT 1");
                if (rs.next()) {
                    System.out.println("   ✅ Double quotes work correctly");
                }
            }
            
            // Test 2: Case-insensitive unquoted identifiers
            System.out.println("\n2. Testing case-insensitive unquoted identifiers:");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT * FROM FILES.SALES LIMIT 1");
                if (rs.next()) {
                    System.out.println("   ✅ Unquoted identifiers are case-insensitive (folded to lowercase)");
                }
            }
            
            // Test 3: PostgreSQL-specific functions
            System.out.println("\n3. Testing PostgreSQL-specific functions:");
            
            // Test STRING_AGG
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(
                    "SELECT STRING_AGG(CAST(order_id AS VARCHAR), ', ') as aggregated " +
                    "FROM files.sales");
                if (rs.next()) {
                    System.out.println("   ✅ STRING_AGG works: " + rs.getString("aggregated"));
                }
            } catch (SQLException e) {
                System.out.println("   ❌ STRING_AGG failed: " + e.getMessage());
            }
            
            // Test ARRAY_AGG
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(
                    "SELECT ARRAY_AGG(order_id) as id_array " +
                    "FROM files.sales");
                if (rs.next()) {
                    System.out.println("   ✅ ARRAY_AGG works");
                }
            } catch (SQLException e) {
                System.out.println("   ❌ ARRAY_AGG failed: " + e.getMessage());
            }
            
            // Test STRPOS
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(
                    "SELECT STRPOS('PostgreSQL', 'SQL') as pos");
                if (rs.next()) {
                    System.out.println("   ✅ STRPOS works: position = " + rs.getInt("pos"));
                }
            } catch (SQLException e) {
                System.out.println("   ❌ STRPOS failed: " + e.getMessage());
            }
            
            // Test 4: PostgreSQL casting with ::
            System.out.println("\n4. Testing PostgreSQL :: casting:");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(
                    "SELECT '123'::INTEGER as num");
                if (rs.next()) {
                    System.out.println("   ✅ :: casting works: " + rs.getInt("num"));
                }
            } catch (SQLException e) {
                System.out.println("   ❌ :: casting failed: " + e.getMessage());
            }
            
            // Test 5: BOOL_AND and BOOL_OR
            System.out.println("\n5. Testing PostgreSQL aggregate functions:");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(
                    "SELECT BOOL_AND(quantity > 0) as all_positive, " +
                    "BOOL_OR(quantity > 5) as any_above_5 " +
                    "FROM files.sales");
                if (rs.next()) {
                    System.out.println("   ✅ BOOL_AND/BOOL_OR work: all_positive=" + 
                        rs.getBoolean("all_positive") + ", any_above_5=" + 
                        rs.getBoolean("any_above_5"));
                }
            } catch (SQLException e) {
                System.out.println("   ❌ BOOL_AND/BOOL_OR failed: " + e.getMessage());
            }
            
            // Test 6: GREATEST and LEAST (PostgreSQL variant that handles nulls)
            System.out.println("\n6. Testing PostgreSQL GREATEST/LEAST:");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(
                    "SELECT GREATEST(order_id, customer_id, quantity) as max_val, " +
                    "LEAST(order_id, customer_id, quantity) as min_val " +
                    "FROM files.sales LIMIT 1");
                if (rs.next()) {
                    System.out.println("   ✅ GREATEST/LEAST work: max=" + 
                        rs.getInt("max_val") + ", min=" + rs.getInt("min_val"));
                }
            } catch (SQLException e) {
                System.out.println("   ❌ GREATEST/LEAST failed: " + e.getMessage());
            }
            
            System.out.println("\n=== Test Summary ===");
            System.out.println("The driver is configured with PostgreSQL-compatible settings:");
            System.out.println("- Double-quoted identifiers (PostgreSQL standard)");
            System.out.println("- Case-insensitive unquoted identifiers");
            System.out.println("- PostgreSQL function library enabled");
            System.out.println("- BABEL conformance for PostgreSQL syntax support");
            
        } catch (SQLException e) {
            System.err.println("❌ Connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}