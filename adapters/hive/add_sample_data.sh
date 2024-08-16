#!/bin/bash

# Connect to the Hive server and execute Hive commands
docker exec -i hive-hive-server-1 beeline --verbose -u jdbc:hive2://localhost:10000 << EOF

-- Create a new database
CREATE DATABASE IF NOT EXISTS sample_db;

-- Use the new database
USE sample_db;

-- Create a sample table
CREATE TABLE IF NOT EXISTS employees (
    id INT,
    name STRING,
    department STRING,
    salary DOUBLE
);

-- Insert sample data
INSERT INTO employees VALUES
    (1, 'John Doe', 'HR', 50000.00),
    (2, 'Jane Smith', 'Engineering', 75000.00),
    (3, 'Bob Johnson', 'Sales', 60000.00),
    (4, 'Alice Brown', 'Marketing', 55000.00),
    (5, 'Charlie Davis', 'Engineering', 80000.00);

-- Verify the data
SELECT * FROM employees;

EOF