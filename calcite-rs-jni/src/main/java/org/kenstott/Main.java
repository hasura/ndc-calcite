package org.kenstott;

import java.io.IOException;
import java.sql.Connection;

/**
 * The Main class is the entry point of the application.
 * It demonstrates the usage of the CalciteQuery class to create a Calcite connection
 * and perform queries on the models.
 */
public class Main {
    public static void main(String[] args) throws IOException {

        String modelPath = "../adapters/file/model.yaml";
        String username = "<username>";
        String password = "<password>";
        Connection calciteConnection = null;

        try {
            CalciteQuery query = new CalciteQuery();
            calciteConnection = query.createCalciteConnection(modelPath);
            String x = query.getModels();
            System.out.println(x);
            String z1 = query.queryModels("""
                    SELECT * from "HR"."EMP"
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
