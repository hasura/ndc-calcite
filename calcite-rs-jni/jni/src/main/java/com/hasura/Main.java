package com.hasura;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * The Main class is the entry point of the application.
 * It demonstrates the usage of the CalciteQuery class to create a Calcite connection
 * and perform queries on the models.
 */
public class Main {
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {

//        JdbcTest.test();

        String modelPath = "/Users/kennethstott/ndc-calcite/adapters/splunk/model.json";
        String username = "<username>";
        String password = "<password>";
        Connection calciteConnection = null;

        try {
//            String sql = "SELECT al.\"albumId\", al.\"title\"\n" +
//                    ", COUNT(tr.\"trackId\") AS trackCount\n" +
//                    "FROM \"graphql\".\"Albums\" al\n" +
//                    "JOIN \"graphql\".\"Tracks\" tr ON al.\"albumId\" = tr.\"albumId\"\n" +
//                    "WHERE al.\"albumId\" > 200\n" +
//                    "GROUP BY al.\"albumId\", al.\"title\"\n" +
//                    "ORDER BY trackCount\n" +
//                    "OFFSET 1 ROWS\n" +
//                    "FETCH NEXT 150 ROWS ONLY\n";
//            CalciteVerboseDebugger.debugVerbose(modelPath);
//            try {
//                System.out.println("************QUERY PLANNER************");
//                CalciteModelPlanner.displayQueryPlan(modelPath, sql);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//            System.out.println("************EXECUTE QUERY************");
            CalciteQuery query = new CalciteQuery();
            try {
                calciteConnection = query.createCalciteConnection(modelPath);
            } catch (Throwable e) {
                e.printStackTrace();
            }
            String x = query.getModels();
            System.out.println(x);

            String sql = "SELECT \"splunk\".\"authentication\".\"is_Default_Authentication\", \"splunk\".\"authentication\".\"user\",\"splunk\".\"authentication\".\"dest\" AS \"dest\",\"splunk\".\"authentication\".\"dest_bunit\" AS \"destBunit\",\"splunk\".\"authentication\".\"dest_category\" AS \"destCategory\",\"splunk\".\"authentication\".\"action\" AS \"action\",\"splunk\".\"authentication\".\"app\" AS \"app\",\"splunk\".\"authentication\".\"authentication_method\" AS \"authenticationMethod\",\"splunk\".\"authentication\".\"authentication_service\" AS \"authenticationService\",\"splunk\".\"authentication\".\"dest_nt_domain\" AS \"destNtDomain\",\"splunk\".\"authentication\".\"dest_priority\" AS \"destPriority\",\"splunk\".\"authentication\".\"duration\" AS \"duration\",\"splunk\".\"authentication\".\"_extra\" AS \"extra\",\"splunk\".\"authentication\".\"host\" AS \"host\",\"splunk\".\"authentication\".\"index\" AS \"index\" FROM \"splunk\".\"authentication\" LIMIT 100";
            String z1 = query.queryModels(sql);
//            z1 = query.queryModels(sql);
            System.out.println(z1);
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
