package com.hasura;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class AthenaRowCountExample {

    private static final Logger logger = LogManager.getLogger(AthenaRowCountExample.class);

    public static void test() {
        try {
            String region = "us-west-1";
            String s3OutputLocation = " s3://hasura-chinook/";
            String awsAccessKeyId = "ASIAXWKCGTSOFND7HFNA";
            String password = "8qX+ilQ5H6HxxFvceOpACeLVVe+LD2JOEDV2HcIZ";
            String jdbcUrl = String.format("jdbc:athena://database=chinook;user=%s;password=%s;AwsRegion=%s;S3OutputLocation=%s", awsAccessKeyId, password, region, s3OutputLocation);
            String database = "chinook";
            String query = "SELECT COUNT(*) AS rowcount FROM your_table";

            try (Connection conn = DriverManager.getConnection(jdbcUrl);
                 Statement stmt = conn.createStatement()) {

                ResultSet rs = stmt.executeQuery(query);
                rs.next();
                int rowCount = rs.getInt("rowcount");
                System.out.println("Row count: " + rowCount);

            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
