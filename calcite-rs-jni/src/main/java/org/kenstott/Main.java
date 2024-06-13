package org.kenstott;


import java.sql.Connection;
import java.util.Collection;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        String modelPath = "/Users/kennethstott/calcite/example/csv/src/test/resources/model.json";
        String username = "<username>";
        String password ="<password>";

        try {
            CalciteQuery query = new CalciteQuery();
            try (Connection calciteConnection = query.createCalciteConnection(modelPath)) {
                System.out.println("Got connection");
                Collection<String> list = query.getTableNames();
                System.out.println(list.toString());
                Map<String, String> c = query.getTableColumnInfo("EMPS");
                System.out.println(c.toString());
                String x = query.getModels();
                System.out.println(x);
                String z = query.queryModels("SELECT \"DEPTNO\" AS \"DEPTNO\", \"NAME\" AS \"NAME\" FROM DEPTS WHERE NAME LIKE '%a%'  LIMIT 10");
                System.out.println(z);
            }
            // You can now use 'calciteConnection' which is an instance of CalciteQuery
        } catch (Exception e) {
            System.out.println("An error occurred while creating Calcite connection: " + e.getMessage());
        }
    }
}
