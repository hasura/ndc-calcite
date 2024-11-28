#!/bin/bash

# Check if version argument is provided
if [ $# -ne 1 ]; then
    echo "Error: Version argument is required"
    echo "Usage: $0 <version>"
    echo "Example: $0 1.0.0"
    exit 1
fi

VERSION=$1

# Store the script's starting directory
INITIAL_DIR=$(pwd)

# Function to handle errors
handle_error() {
    echo "Error: $1"
    cd "$INITIAL_DIR"  # Return to initial directory
    exit 1
}

# Clean Maven artifacts
cd calcite-rs-jni || handle_error "Failed to change directory to calcite-rs-jni"
mvn clean || handle_error "Maven clean failed"

# Clean Gradle artifacts
cd calcite || handle_error "Failed to change directory to calcite"
./gradlew clean || handle_error "Gradle clean failed"

# Build Docker image
cd ../.. || handle_error "Failed to return to root directory"
docker build . -t "ghcr.io/hasura/ndc-calcite:${VERSION}" || handle_error "Docker build failed"

# Build and install Java artifacts
cd calcite-rs-jni/calcite || handle_error "Failed to change directory to calcite-rs-jni/calcite"
./gradlew assemble || handle_error "Gradle assemble failed"
cd .. || handle_error "Failed to change directory"
mvn install -DskipTests || handle_error "Maven install failed"

echo "Build completed successfully!"
echo "Docker image tagged as: ghcr.io/hasura/ndc-calcite:${VERSION}"
