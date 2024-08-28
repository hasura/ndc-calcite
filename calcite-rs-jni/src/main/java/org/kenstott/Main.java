package org.kenstott;

import java.sql.Connection;

/**
 * The Main class is the entry point of the application.
 * It demonstrates the usage of the CalciteQuery class to create a Calcite connection
 * and perform queries on the models.
 */
public class Main {
    public static void main(String[] args) {

        String modelPath = "../adapters/jdbc/model.json";
        String username = "<username>";
        String password = "<password>";
        Connection calciteConnection = null;

        try {
            CalciteQuery query = new CalciteQuery();
            calciteConnection = query.createCalciteConnection(modelPath);
            String x = query.getModels();
            System.out.println(x);
            String z1 = query.queryModels("""
                    SELECT "default"."genres"."Name" AS "name","default"."genres"."GenreId" AS "GenreId" FROM "default"."genres" WHERE "GenreId" IN (1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,1,1,1,1,1,1,1,1,1,1,1,1,1,1)
                    """
            );
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
