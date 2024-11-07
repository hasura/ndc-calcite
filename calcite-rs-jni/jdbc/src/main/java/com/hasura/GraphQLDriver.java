package com.hasura;

import org.apache.calcite.adapter.graphql.GraphQLSchemaFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;

import java.sql.*;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;

public class GraphQLDriver extends Driver {
    private static final String PREFIX = "jdbc:graphql:";

    static {
        try {
            DriverManager.registerDriver(new GraphQLDriver());
        } catch (Exception e) {
            throw new RuntimeException("Failed to register GraphQL JDBC driver", e);
        }
    }

    @Override
    protected String getConnectStringPrefix() {
        return PREFIX;
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return new DriverVersion(
                "Hasura GraphQL JDBC Driver",  // productName
                "1.0",                         // productVersion
                "Hasura GraphQL",              // productName
                "1.0",                         // driverVersion
                true,                          // jdbcCompliant
                1,                             // majorVersion
                0,                             // minorVersion
                0,                             // buildVersion
                0                              // serialVersionUID
        );
    }

    private void parseUrlOptions(String url, Map<String, Object> operand, Properties calciteProps) {
        String[] parts = url.split(";");
        for (int i = 1; i < parts.length; i++) {
            String option = parts[i].trim();
            if (option.isEmpty()) continue;

            String[] keyValue = option.split("=", 2);
            if (keyValue.length != 2) continue;

            String key = keyValue[0].trim();
            String value = keyValue[1].trim();

            if (key.startsWith("operand.")) {
                String operandKey = key.substring("operand.".length());
                if (operandKey.contains(".")) {
                    String[] nestedKeys = operandKey.split("\\.", 2);
                    Map<String, Object> nestedMap = (Map<String, Object>) operand.computeIfAbsent(nestedKeys[0], k -> new HashMap<String, Object>());
                    nestedMap.put(nestedKeys[1], value);
                } else {
                    operand.put(operandKey, value);
                }
            } else if (key.startsWith("calcite.")) {
                String calciteKey = key.substring("calcite.".length());
                calciteProps.setProperty(calciteKey, value);
            } else if (key.equals("user") || key.equals("role") || key.equals("auth")) {
                operand.put(key, value);
            }
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        String urlWithoutPrefix = url.substring(PREFIX.length());
        String[] parts = urlWithoutPrefix.split(";");
        String baseUrl = parts[0];

        Map<String, Object> operand = new HashMap<>();
        operand.put("endpoint", baseUrl);

        if (info.containsKey("user")) operand.put("user", info.getProperty("user"));
        if (info.containsKey("role")) operand.put("role", info.getProperty("role"));
        if (info.containsKey("auth")) operand.put("auth", info.getProperty("auth"));

        Properties calciteProps = new Properties();
        calciteProps.setProperty("fun", "standard");
        calciteProps.setProperty("caseSensitive", "true");
        calciteProps.setProperty("unquotedCasing", "UNCHANGED");
        calciteProps.setProperty("quotedCasing", "UNCHANGED");

        parseUrlOptions(urlWithoutPrefix, operand, calciteProps);

        Connection connection = DriverManager.getConnection("jdbc:calcite:", calciteProps);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        GraphQLSchemaFactory factory = new GraphQLSchemaFactory();
        SchemaPlus graphqlSchema = rootSchema.add("GRAPHQL",
                factory.create(rootSchema, "GRAPHQL", operand));

        calciteConnection.setSchema("GRAPHQL");

        // Wrap the connection with our enhanced metadata support
        return connection;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public boolean jdbcCompliant() {
        return true;
    }
    @Override
    public Logger getParentLogger() {
        return Logger.getLogger(getClass().getPackage().getName());
    }
}