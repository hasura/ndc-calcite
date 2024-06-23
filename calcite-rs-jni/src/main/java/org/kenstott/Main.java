package org.kenstott;


import java.io.IOException;
import java.sql.Connection;
import java.util.Collection;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        String modelPath = "../adapters/file/model.json";
        String username = "<username>";
        String password ="<password>";

//        try {
//            PrintAllVectorsExample.printFile("/Users/kennethstott/Documents/GitHub/ndc-calcite/ndc-calcite/adapters/arrow/resources/arrow/findProducts.arrow");
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }

        try {
            CalciteQuery query = new CalciteQuery();
            try (Connection calciteConnection = query.createCalciteConnection(modelPath)) {
                System.out.println("Got connection");
                Collection<String> list = query.getTableNames();
                System.out.println(list.toString());
                Map<String, String> c = query.getTableColumnInfo("ARCHERS");
                System.out.println(c.toString());
                String x = query.getModels();
                System.out.println(x);
                String z1 = query.queryModels("SELECT \"DEPTNO\" AS \"zz\" FROM \"LONG_EMPS\" LIMIT 10");
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
