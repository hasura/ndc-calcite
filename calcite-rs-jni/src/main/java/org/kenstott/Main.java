package org.kenstott;


import java.sql.Connection;
import java.util.Collection;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        String modelPath = "../ndc-calcite/adapters/file/model.json";
        String username = "<username>";
        String password ="<password>";

        try {
            CalciteQuery query = new CalciteQuery();
            try (Connection calciteConnection = query.createCalciteConnection(modelPath)) {
                System.out.println("Got connection");
                Collection<String> list = query.getTableNames();
                System.out.println(list.toString());
                Map<String, String> c = query.getTableColumnInfo("FINDPRODUCTS");
                System.out.println(c.toString());
                String x = query.getModels();
                System.out.println(x);
                String z1 = query.queryModels("SELECT * FROM \"DATES\" LIMIT 10");
                String z2 = query.queryModels("SELECT * FROM WACKY_COLUMN_NAMES LIMIT 10");
                String z3 = query.queryModels("SELECT COUNT(\"characters\") AS \"characters_count\", COUNT(DISTINCT \"characters\") AS \"characters_distinct_count\", COUNT(\"dow\") AS \"dow_count\", COUNT(DISTINCT \"dow\") AS \"dow_distinct_count\", COUNT(\"id\") AS \"id_count\", COUNT(DISTINCT \"id\") AS \"id_distinct_count\", COUNT(DISTINCT \"longDate\") AS \"longDate_distinct_count\", COUNT(\"script\") AS \"script_count\", COUNT(DISTINCT \"script\") AS \"script_distinct_count\", COUNT(\"summary\") AS \"summary_count\", COUNT(DISTINCT \"summary\") AS \"summary_distinct_count\", COUNT(\"title\") AS \"title_count\", COUNT(DISTINCT \"title\") AS \"title_distinct_count\", COUNT(\"x\") AS \"x_count\", COUNT(DISTINCT \"x\") AS \"x_distinct_count\" FROM \"ARCHERS\"  LIMIT 10");
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
