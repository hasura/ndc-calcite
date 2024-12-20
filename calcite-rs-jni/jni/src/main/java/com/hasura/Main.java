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

        String modelPath = "/Users/kennethstott/test/calcite-connector/adapters/databricks/model.json";
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

            String sql = "SELECT \"TEST\".\"orders\".\"o_clerk\" AS \"o_clerk\",\"TEST\".\"orders\".\"o_comment\" AS \"o_comment\",\"TEST\".\"orders\".\"o_custkey\" AS \"o_custkey\",\"TEST\".\"orders\".\"o_orderdate\" AS \"o_orderdate\",\"TEST\".\"orders\".\"o_orderkey\" AS \"o_orderkey\",\"TEST\".\"orders\".\"o_orderpriority\" AS \"o_orderpriority\",\"TEST\".\"orders\".\"o_orderstatus\" AS \"o_orderstatus\",\"TEST\".\"orders\".\"o_shippriority\" AS \"o_shippriority\",\"TEST\".\"orders\".\"o_totalprice\" AS \"o_totalprice\" FROM \"TEST\".\"orders\" WHERE \"o_orderdate\" = __UTF8__1993-05-21__UTF8__  LIMIT 10";
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
