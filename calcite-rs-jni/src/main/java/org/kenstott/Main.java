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
                    SELECT min(PlaylistId) AS "PlaylistId_min_5667", max(PlaylistId) AS "PlaylistId_max_6029" FROM "emr"."playlists"  LIMIT 10
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
