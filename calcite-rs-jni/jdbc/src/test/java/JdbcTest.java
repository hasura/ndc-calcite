import java.sql.Connection;
import java.sql.DriverManager;

public class JdbcTest {
    public static void main(String[] args) {
        // Database URL, username, and password
        String url = "jdbc:graphql:http://localhost:3000/graphql";
        String role = "admin";

        try {
            // Register JDBC driver
            Class.forName("com.hasura.GraphQLDriver");

            // Open a connection
            System.out.println("Attempting to connect to database...");
            Connection conn = DriverManager.getConnection(url);

            if (conn != null) {
                System.out.println("Database connection successful!");
                System.out.println("Driver: " + conn.getMetaData().getDriverName());
                System.out.println("Driver Version: " + conn.getMetaData().getDriverVersion());
                conn.close();
            }

        } catch (ClassNotFoundException e) {
            System.out.println("JDBC Driver not found.");
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("Database connection failed!");
            e.printStackTrace();
        }
    }
}