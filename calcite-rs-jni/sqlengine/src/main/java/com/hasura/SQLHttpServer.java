package com.hasura;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import org.json.JSONObject;
import org.json.JSONArray;

import java.io.*;
import java.net.InetSocketAddress;
import java.sql.*;
import java.util.*;
import java.nio.charset.StandardCharsets;

public class SQLHttpServer {
    private static final String JDBC_URL = System.getenv("JDBC_URL");
    private static final int PORT = getPortFromEnv();

    static {
        try {
            Class.forName("com.hasura.GraphQLDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static int getPortFromEnv() {
        String portStr = System.getenv("PORT");
        if (portStr != null) {
            try {
                return Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        System.err.println("Warning: Invalid PORT environment variable. Using default port 8080");
        return 8080;
    }

    public static void main(String[] args) throws IOException {
        // Validate environment variables
        if (JDBC_URL == null) {
            System.err.println("Error: Required environment variable JDBC_URL must be set");
            System.exit(1);
        }

        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/sql", new SQLHandler());
        server.createContext("/v1/sql", new SQLHandler());
        server.createContext("/health", new HealthHandler());  // add this line
        server.setExecutor(null);
        server.start();
        System.out.println("Server started on port " + PORT);
    }

    static class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            JSONObject json = new JSONObject();
            json.put("health", "OK");
            byte[] response = json.toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        }
    }

    static class SQLHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!exchange.getRequestMethod().equals("POST")) {
                sendResponse(exchange, 405, "Method not allowed");
                return;
            }

            // Extract connection properties from headers
            Properties connectionProps = new Properties();

            // Get user from X-Hasura-User header
            String user = exchange.getRequestHeaders().getFirst("X-Hasura-User");
            if (user != null) {
                connectionProps.setProperty("user", user);
            }

            // Get role from X-Hasura-Role header
            String role = exchange.getRequestHeaders().getFirst("X-Hasura-Role");
            if (role != null) {
                connectionProps.setProperty("role", role);
            }

            // Get auth from Authorization header
            String auth = exchange.getRequestHeaders().getFirst("Authorization");
            if (auth != null) {
                connectionProps.setProperty("auth", auth);
            }

            // Get password from password header
            String password = exchange.getRequestHeaders().getFirst("Password");
            if (password != null) {
                connectionProps.setProperty("password", password);
            }

            try {
                // Read request body
                String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                JSONObject jsonRequest = new JSONObject(requestBody);

                String sql = jsonRequest.getString("sql");
                boolean disallowMutations = jsonRequest.getBoolean("disallowMutations");

                // Validate SQL type against allowMutations flag
                if (disallowMutations && isMutationQuery(sql)) {
                    sendResponse(exchange, 400, "Mutations not allowed");
                    return;
                }

                // Execute SQL and get results
                JSONArray results = executeSQLQuery(sql, connectionProps);

                // Send response
                sendResponse(exchange, 200, results.toString());

            } catch (SQLException e) {
                e.printStackTrace();
                sendResponse(exchange, 500, "Database Error: " + e.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
                sendResponse(exchange, 500, "Internal Server Error: " + e.getMessage());
            }
        }

        private boolean isMutationQuery(String sql) {
            String upperSql = sql.trim().toUpperCase();
            return upperSql.startsWith("INSERT") ||
                    upperSql.startsWith("UPDATE") ||
                    upperSql.startsWith("DELETE") ||
                    upperSql.startsWith("DROP") ||
                    upperSql.startsWith("CREATE") ||
                    upperSql.startsWith("ALTER");
        }

        private JSONArray executeSQLQuery(String sql, Properties connectionProps) throws SQLException {
            JSONArray jsonArray = new JSONArray();

            // Create a new connection for each request using the provided properties
            try (Connection conn = DriverManager.getConnection(JDBC_URL, connectionProps);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {

                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (rs.next()) {
                    JSONObject row = new JSONObject();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object value = rs.getObject(i);
                        row.put(columnName, value);
                    }
                    jsonArray.put(row);
                }
            }

            return jsonArray;
        }

        private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(statusCode, responseBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        }
    }
}