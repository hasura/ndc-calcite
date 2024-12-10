#!/bin/bash

# Exit on any error
set -e

# Function to clean macOS metadata files if possible
clean_mac_metadata() {
    if [[ "$OSTYPE" == "darwin"* ]] && command -v dot_clean >/dev/null 2>&1; then
        echo "Cleaning macOS metadata files..."
        dot_clean "$1"
    fi
}

# Function to check if directory exists
check_directory() {
    if [ ! -d "$1" ]; then
        echo "Error: Directory $1 not found"
        exit 1
    fi
}

# Function to run Gradle commands with error handling
run_gradle() {
    # Create build directory if it doesn't exist
    mkdir -p build

    if ! ./gradlew $1; then
        echo "Gradle $1 failed. Checking if this is the first build..."
        if [ "$1" == "clean" ]; then
            echo "First build detected, continuing..."
        else
            echo "Error: Gradle $1 failed"
            exit 1
        fi
    fi
    
    clean_mac_metadata "."
}

# Main build process
main() {
    # Check if calcite directory exists
    check_directory "calcite"
    
    # Navigate to calcite directory
    echo "Entering calcite directory..."
    cd calcite || exit 1
    clean_mac_metadata "."
    
    # Run Gradle commands
    echo "Running Gradle clean..."
    run_gradle "clean"
    
    echo "Running Gradle assemble..."
    run_gradle "assemble"
    
    # Return to parent directory
    echo "Returning to parent directory..."
    cd ..
    
    # Run Maven commands
    echo "Running Maven clean install..."
    if ! mvn clean install; then
        echo "Error: Maven clean install failed"
        exit 1
    fi
    clean_mac_metadata "."
    
    echo "Running Maven dependency copy..."
    if ! mvn dependency:copy-dependencies; then
        echo "Error: Maven dependency copy failed"
        exit 1
    fi
    clean_mac_metadata "."
    
    # Setup Python environment
    check_directory "py_graphql_sql"
    cd py_graphql_sql || exit 1
    
    echo "Creating Python virtual environment..."
    python3 -m venv .venv
    clean_mac_metadata "."
    
    echo "Activating virtual environment..."
    source .venv/bin/activate
    clean_mac_metadata "."
    
    echo "Installing poetry..."
    if ! pip install poetry; then
        echo "Error: Failed to install poetry"
        exit 1
    fi
    clean_mac_metadata "."
    
    echo "Running Python build..."
    if ! python3 build.py; then
        echo "Error: Python build failed"
        exit 1
    fi
    clean_mac_metadata "."
    
    cd ..
    
    echo "Build process completed successfully!"
}

# Run the main function
main
