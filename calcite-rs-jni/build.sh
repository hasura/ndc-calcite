#!/bin/bash

set -eo pipefail

# Constants
CALCITE_VERSION="1.38.0-SNAPSHOT"
CALCITE_ARTIFACTS=(
    "core/calcite-core"
    "graphql/calcite-graphql"
    "linq4j/calcite-linq4j"
)

# Function to handle errors
error_handler() {
    echo "Error occurred in script at line: ${1}"
    exit 1
}

trap 'error_handler ${LINENO}' ERR

# Function to check directory existence
check_directory() {
    local dir=$1
    if [ ! -d "$dir" ]; then
        echo "Error: Directory $dir not found"
        exit 1
    fi
}

# Function to install calcite artifacts
install_calcite_artifact() {
    local path=$1
    local artifact_id=$(basename "$path")
    local jar_path="calcite/${path}/build/libs/${artifact_id}-${CALCITE_VERSION}.jar"

    echo "Installing ${artifact_id}..."
    mvn install:install-file \
        -Dfile="$jar_path" \
        -DgroupId=org.apache.calcite \
        -DartifactId="$artifact_id" \
        -Dversion="$CALCITE_VERSION" \
        -Dpackaging=jar
}

# Function to run Gradle commands
run_gradle() {
    local command=$1
    echo "Running Gradle $command..."
    ./gradlew "$command" || {
        if [ "$command" = "clean" ]; then
            echo "First build detected, continuing..."
        else
            echo "Error: Gradle $command failed"
            exit 1
        fi
    }
}

# Main build process
main() {
    echo "Starting build process..."

    # Check and build calcite
    check_directory "calcite"
    cd calcite
    run_gradle "clean"
    run_gradle "assemble"
    cd ..

    # Install calcite artifacts
    for artifact in "${CALCITE_ARTIFACTS[@]}"; do
        install_calcite_artifact "$artifact"
    done

    # Maven build steps
    echo "Running Maven build steps..."
    mvn clean install
    mvn dependency:copy-dependencies

    # Python setup (if running locally)
    if [ -z "$DOCKER_BUILD" ]; then
        check_directory "py_graphql_sql"
        cd py_graphql_sql

        echo "Setting up Python environment..."
        python3 -m venv .venv
        # shellcheck disable=SC1091
        source .venv/bin/activate
        pip install poetry
        python3 build.py
        cd ..
    fi

    echo "Build process completed successfully!"
}

# Run main function with error handling
main
