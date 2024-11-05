package com.hasura;

import org.apache.calcite.adapter.graphql.GraphQLSchemaFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.*;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;

public class GraphQLDriver extends UnregisteredDriver implements Driver {
    private static final String PREFIX = "jdbc:graphql:";

    static {
        try {
            Class.forName("org.apache.calcite.avatica.remote.Driver");
            Class.forName("org.apache.calcite.jdbc.Driver");
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

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        String connectionUrl = url.substring(PREFIX.length());

        Map<String, Object> operand = new HashMap<>();
        operand.put("endpoint", connectionUrl);

        if (info.containsKey("user")) {
            operand.put("user", info.getProperty("user"));
        }
        if (info.containsKey("role")) {
            operand.put("role", info.getProperty("role"));
        }
        if (info.containsKey("auth")) {
            operand.put("auth", info.getProperty("auth"));
        }

        Properties calciteProps = new Properties();
        calciteProps.setProperty("fun", "standard");

        Connection connection = DriverManager.getConnection("jdbc:calcite:", calciteProps);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        GraphQLSchemaFactory factory = new GraphQLSchemaFactory();
        SchemaPlus graphqlSchema = rootSchema.add("GRAPHQL",
                factory.create(rootSchema, "GRAPHQL", operand));

        calciteConnection.setSchema("GRAPHQL");

        return connection;
    }

    @Override
    public Logger getParentLogger()  {
        return null;
    }

    @Override
    public Meta createMeta(AvaticaConnection connection) {
        return null;
    }
}