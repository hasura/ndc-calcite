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
               String zz = query.queryPlanModels("""
 SELECT COUNT(*) as "count", JSON_ARRAYAGG("TrackId") as "tracks", "c"."FirstName", "c"."LastName" FROM "TEST"."invoice_items"
 JOIN "TEST"."invoices" as "i" USING("InvoiceId")
 JOIN "TEST"."customers" as "c" USING("CustomerId")
 GROUP BY "c"."FirstName", "c"."LastName", "i"."InvoiceId"
 
 """);
                System.out.println(zz);
                String z1 = query.queryModels("""
 SELECT COUNT(*) as "count", JSON_ARRAYAGG("TrackId") as "tracks", "c"."FirstName", "c"."LastName" FROM "TEST"."invoice_items"
 JOIN "TEST"."invoices" as "i" USING("InvoiceId")
 JOIN "TEST"."customers" as "c" USING("CustomerId")
 GROUP BY "c"."FirstName", "c"."LastName", "i"."InvoiceId"
 
 """
               );

                System.out.println(z1);
//                System.out.println(z2);

            }
            // You can now use 'calciteConnection' which is an instance of CalciteQuery
        } catch (Exception e) {
            System.out.println("An error occurred while creating Calcite connection: " + e.getMessage());
        }
    }
}
