package org.kenstott;

import java.io.IOException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * The Main class is the entry point of the application.
 * It demonstrates the usage of the CalciteQuery class to create a Calcite connection
 * and perform queries on the models.
 */
public class Main {
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {

//        String jdbcUrl = "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443?ProjectId=MyProject26996&OAuthType=0&OAuthServiceAcctEmail=kenneth.stott@gmail.com";
//        String encodedUrl = URLEncoder.encode(url, StandardCharsets.UTF_8.toString());
//        System.out.println(encodedUrl);
//        Class.forName("com.simba.googlebigquery.jdbc42.Driver");
//        Connection conn = DriverManager.getConnection(jdbcUrl);

        String modelPath = "../../adapters/databricks/model.json";
        String username = "<username>";
        String password = "<password>";
        Connection calciteConnection = null;

        try {
            CalciteQuery query = new CalciteQuery();
            calciteConnection = query.createCalciteConnection(modelPath);
            String x = query.getModels();
            System.out.println(x);
            String q1 = """
                    SELECT * from "lineitem"
               
                    OFFSET 2 ROWS
                    FETCH NEXT 5 ROWS ONLY
                    """;
            String z1 = query.queryModels(q1);
            System.out.println(z1);
//            String z2 = query.queryModels("""
//                    SELECT "CustomerId" from "FILE"."TEST"
//                    """
//            );
//            System.out.println(z2);
            calciteConnection.close();
            calciteConnection = null;
        } catch (Exception e) {
            System.out.println("An error occurred while creating Calcite connection: " + e.getMessage());
        } finally {
            if (calciteConnection != null) {
                try {
                    calciteConnection.close();
                } catch (Exception e) {
                    /* ignore */
                }
            }
            System.exit(0);
        }
    }
}
