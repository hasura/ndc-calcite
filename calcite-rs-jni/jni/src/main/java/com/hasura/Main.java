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

        String modelPath = "../../adapters/file/model.json";
        String username = "<username>";
        String password = "<password>";
        Connection calciteConnection = null;

        try {
            CalciteQuery query = new CalciteQuery();
            calciteConnection = query.createCalciteConnection(modelPath);
            String x = query.getModels();
            System.out.println(x);
            String q1 = "SELECT \"default\".\"LONG_EMPS\".\"AGE\" AS \"AGE\",\"default\".\"LONG_EMPS\".\"CITY\" AS \"CITY\",\"default\".\"LONG_EMPS\".\"DEPTNO\" AS \"DEPTNO\",\"default\".\"LONG_EMPS\".\"EMPID\" AS \"EMPID\",\"default\".\"LONG_EMPS\".\"EMPNO\" AS \"EMPNO\",\"default\".\"LONG_EMPS\".\"GENDER\" AS \"GENDER\",\"default\".\"LONG_EMPS\".\"JOINEDAT\" AS \"JOINEDAT\",\"default\".\"LONG_EMPS\".\"MANAGER\" AS \"MANAGER\",\"default\".\"LONG_EMPS\".\"NAME\" AS \"NAME\",\"default\".\"LONG_EMPS\".\"SLACKER\" AS \"SLACKER\" FROM \"default\".\"LONG_EMPS\" WHERE \"JOINEDAT\" IN (__UTF8__1996-08-03__UTF8__,__UTF8__2001-01-01__UTF8__)  LIMIT 10";
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
