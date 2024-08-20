package org.kenstott;


import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.Map;

/**
 * The Main class is the entry point of the application.
 * It demonstrates the usage of the CalciteQuery class to create a Calcite connection
 * and perform queries on the models.
 */
public class Main {
    public static void main(String[] args) {

        String modelPath = "../adapters/h2/model.json";
        String username = "<username>";
        String password = "<password>";
        Connection calciteConnection = null;

        try {
//            String classpath = System.getProperty("java.class.path");
//            System.out.println(classpath);
//            AthenaRowCountExample.test();
            CalciteQuery query = new CalciteQuery();
            calciteConnection = query.createCalciteConnection(modelPath);
            System.out.println("Got connection");
            String x = query.getModels();
            System.out.println(x);
//               String zz = query.queryPlanModels("""
//
// """);
//                System.out.println(zz);
            String z1 = query.queryModels("""
                    SELECT "ID" FROM "TEST"."PROJECTS"
                    """
            );
            System.out.println(z1);

//            String z2 = query.queryModels("""
//                    SELECT STREAM * FROM "KAFKA"."TABLE_NAME" LIMIT 2
//                    """
//            );
//            System.out.println(z2);
//            query.queryModels("""
//                    SELECT STREAM * FROM "KAFKA"."TABLE_NAME"
//                    """
//            );
            calciteConnection.close();
            calciteConnection = null;
            // You can now use 'calciteConnection' which is an instance of CalciteQuery
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
