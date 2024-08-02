package org.kenstott;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class AthenaRowCountExample {
    public static void test() {
        String jdbcUrl = "jdbc:awsathena://athena.us-west-1.amazonaws.com:443";
        String database = "chinook";
        String query = "SELECT COUNT(*) AS rowcount FROM your_table";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, "ASIAXWKCGTSOK5V3HRGN", "HvJNr8LVfxV4EZ3PWiOXjcpHKD+qmYLw8Tmhw5n8");
             Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            int rowCount = rs.getInt("rowcount");
            System.out.println("Row count: " + rowCount);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
