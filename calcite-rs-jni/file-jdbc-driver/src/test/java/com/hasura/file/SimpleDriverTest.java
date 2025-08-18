package com.hasura.file;

import java.sql.*;

public class SimpleDriverTest {
    public static void main(String[] args) throws Exception {
        // Try loading the driver explicitly
        try {
            Class.forName("com.hasura.file.FileDriver");
            System.out.println("✓ FileDriver loaded successfully");
        } catch (ClassNotFoundException e) {
            System.out.println("✗ Failed to load FileDriver: " + e.getMessage());
        }
        
        // Check if driver is registered
        System.out.println("\nRegistered drivers:");
        java.util.Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            System.out.println("  - " + driver.getClass().getName());
        }
        
        // Test URL acceptance
        String testUrl = "jdbc:file:/tmp/test";
        System.out.println("\nTesting URL: " + testUrl);
        
        drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            try {
                boolean accepts = driver.acceptsURL(testUrl);
                System.out.println("  - " + driver.getClass().getName() + " accepts URL: " + accepts);
            } catch (SQLException e) {
                System.out.println("  - " + driver.getClass().getName() + " error: " + e.getMessage());
            }
        }
        
        // Try getting a connection
        System.out.println("\nTrying to get connection...");
        try {
            Connection conn = DriverManager.getConnection(testUrl);
            System.out.println("✓ Got connection: " + conn);
            conn.close();
        } catch (SQLException e) {
            System.out.println("✗ Failed to get connection: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
