package org.kenstott;


import java.io.IOException;
import java.sql.Connection;
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

        try {
            CalciteQuery query = new CalciteQuery();
            try (Connection calciteConnection = query.createCalciteConnection(modelPath)) {
                System.out.println("Got connection");
                query.getModels();
                String x = query.getModels();
                System.out.println(x);
                String z1 = query.queryModels("""
 SELECT
               g."GenreId",
               g."Name",
               '[' || (
                   SELECT COALESCE(
         '{' ||
        '"AlbumId": ' || t."AlbumId" || ', ' ||
        '"Bytes": ' || t."Bytes" || ', ' ||
        '"Composer": ' || t."Composer" || ', ' ||
        '"GenreId": ' || t."GenreId" || ', ' ||
        '"MediaTypeId": ' || t."MediaTypeId" || ', ' ||
        '"Milliseconds": ' || t."Milliseconds" || ', ' ||
        '"Name": ' || t."Name" || ', ' ||
        '"TrackId": ' || t."TrackId" || ', ' ||
        '"UnitPrice": ' || t."UnitPrice" ||
        '}', '' )
              FROM "TEST"."tracks" AS t
              WHERE t."GenreId" = g."GenreId"
           ) || ']' AS "tracks"
           FROM "TEST"."genres" AS g
           LIMIT 10
""");
                String z2 = query.queryModels("SELECT * FROM WACKY_COLUMN_NAMES LIMIT 10");
                String z3 = query.queryModels("SELECT \"object\", \"g\" from  \"ARCHERS\"  LIMIT 10");
                System.out.println(z1);
                System.out.println(z2);
                System.out.println(z3);
            }
            // You can now use 'calciteConnection' which is an instance of CalciteQuery
        } catch (Exception e) {
            System.out.println("An error occurred while creating Calcite connection: " + e.getMessage());
        }
    }
}
