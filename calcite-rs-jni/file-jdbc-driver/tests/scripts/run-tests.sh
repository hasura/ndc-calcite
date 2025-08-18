#!/bin/bash

# Test runner for File JDBC Driver - Comprehensive Tests
# This script establishes all constants and runs the CompleteRetest.java

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Constants
BASE_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
TESTS_DIR="$BASE_DIR/tests"
JAVA_DIR="$TESTS_DIR/java"
RESULTS_DIR="$TESTS_DIR/results"
OUTPUT_FILE="$RESULTS_DIR/calcite-adapter-bugs-analysis.md"
JAR_FILE="$BASE_DIR/target/file-jdbc-driver-1.0.0-jar-with-dependencies.jar"
TEST_CLASS="CompleteRetest"

# Test data directories
export TEST_DATA_DIR="$TESTS_DIR/data"
export TEST_NESTED_DIR="$TESTS_DIR/data/nested"
export TEST_CSV_FILE="$TEST_DATA_DIR/csv/sales.csv"
export TEST_TSV_FILE="$TEST_DATA_DIR/tsv/inventory.tsv"
export TEST_JSON_FILE="$TEST_DATA_DIR/json/customers.json"
export TEST_YAML_FILE="$TEST_DATA_DIR/yaml/config.yaml"
export TEST_XLSX_FILE="$TEST_DATA_DIR/xlsx/company_data.xlsx"
export TEST_HTML_FILE="$TEST_DATA_DIR/html/report.html"
export TEST_PARQUET_FILE="$TEST_DATA_DIR/parquet/sample.parquet"
export TEST_ARROW_FILE="$TEST_DATA_DIR/arrow/sample.arrow"

# Remote URIs for transport tests
export TEST_HTTP_URI="https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv"
export TEST_S3_URI="s3://redshift-chinook/album.csv"

# Ensure directories exist
mkdir -p "$RESULTS_DIR"

echo "========================================"
echo "File JDBC Driver - Comprehensive Tests"
echo "========================================"
echo ""
echo "Configuration:"
echo "  Base Directory: $BASE_DIR"
echo "  JAR File: $JAR_FILE"
echo "  Output File: $OUTPUT_FILE"
echo "  Test Class: $TEST_CLASS"
echo ""
echo "Test Data Files:"run
echo "  CSV: $TEST_CSV_FILE"
echo "  TSV: $TEST_TSV_FILE"
echo "  JSON: $TEST_JSON_FILE"
echo "  YAML: $TEST_YAML_FILE"
echo "  XLSX: $TEST_XLSX_FILE"
echo "  HTML: $TEST_HTML_FILE"
echo "  Parquet: $TEST_PARQUET_FILE"
echo "  Arrow: $TEST_ARROW_FILE"
echo ""
echo "Remote URIs:"
echo "  HTTP: $TEST_HTTP_URI"
echo "  S3: $TEST_S3_URI"
echo ""

# Check if JAR exists
if [ ! -f "$JAR_FILE" ]; then
    echo -e "${RED}Error: JAR file not found at $JAR_FILE${NC}"
    echo "Please build the project first with: mvn clean package"
    exit 1
fi

# Clean previous results
if [ -f "$OUTPUT_FILE" ]; then
    echo "Removing previous test results..."
    rm -f "$OUTPUT_FILE"
fi

# Compile the test
echo -e "${YELLOW}Compiling test class...${NC}"
cd "$BASE_DIR"
javac -cp "$JAR_FILE" "$JAVA_DIR/$TEST_CLASS.java"

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to compile test class${NC}"
    exit 1
fi

# Run the test
echo -e "${YELLOW}Running comprehensive tests...${NC}"
echo ""

java --add-opens=java.base/java.nio=ALL-UNNAMED -cp "$JAR_FILE:$JAVA_DIR" "$TEST_CLASS"

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✅ Tests completed successfully${NC}"
    echo -e "Results written to: ${GREEN}$OUTPUT_FILE${NC}"
else
    echo ""
    echo -e "${RED}❌ Tests failed${NC}"
    echo -e "Check results at: ${RED}$OUTPUT_FILE${NC}"
    exit 1
fi

# Display summary if file exists
if [ -f "$OUTPUT_FILE" ]; then
    echo ""
    echo "Summary:"
    echo "--------"
    # Count successful and failed tests in the output
    SUCCESS_COUNT=$(grep -c "✅ Query successful" "$OUTPUT_FILE" || true)
    FAIL_COUNT=$(grep -c "❌" "$OUTPUT_FILE" || true)
    echo -e "${GREEN}Successful tests: $SUCCESS_COUNT${NC}"
    echo -e "${RED}Failed tests: $FAIL_COUNT${NC}"
fi