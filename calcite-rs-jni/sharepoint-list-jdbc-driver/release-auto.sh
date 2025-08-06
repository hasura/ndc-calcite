#!/bin/bash

# Automated release script for SharePoint List JDBC Driver (for CI/CD)

set -e

# Configuration
PROJECT_NAME="SharePoint List JDBC Driver"
ARTIFACT_ID="sharepoint-list-jdbc-driver"
GROUP_ID="com.kenstott.components"

echo "Starting automated release process for $PROJECT_NAME..."

# Check required environment variables
REQUIRED_VARS=("SONATYPE_USERNAME" "SONATYPE_PASSWORD" "GPG_PRIVATE_KEY_BASE64" "GPG_PASSPHRASE")
for var in "${REQUIRED_VARS[@]}"; do
    if [[ -z "${!var}" ]]; then
        echo "Error: Required environment variable $var is not set"
        exit 1
    fi
done

# Setup GPG
echo "Setting up GPG..."
./setup-gpg.sh

# Get version from command line argument or determine automatically
if [[ -n "$1" ]]; then
    RELEASE_VERSION="$1"
    echo "Using provided release version: $RELEASE_VERSION"
else
    # Determine version automatically from current version
    CURRENT_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
    RELEASE_VERSION=${CURRENT_VERSION%-SNAPSHOT}
    echo "Auto-determined release version: $RELEASE_VERSION from current version: $CURRENT_VERSION"
fi

# Calculate next development version
IFS='.' read -ra VERSION_PARTS <<< "$RELEASE_VERSION"
MAJOR=${VERSION_PARTS[0]}
MINOR=${VERSION_PARTS[1]}
PATCH=${VERSION_PARTS[2]}

NEXT_PATCH=$((PATCH + 1))
NEXT_DEV_VERSION="$MAJOR.$MINOR.$NEXT_PATCH-SNAPSHOT"

echo "Release plan:"
echo "  Release version: $RELEASE_VERSION"
echo "  Next development version: $NEXT_DEV_VERSION"

# Create settings.xml from template
echo "Creating Maven settings..."
envsubst < settings.xml.template > ~/.m2/settings.xml

# Set release version
echo "Setting release version..."
mvn versions:set -DnewVersion="$RELEASE_VERSION" -DgenerateBackupPoms=false

# Build, test, and deploy
echo "Building, testing, and deploying..."
mvn clean deploy -P release-sign-artifacts

echo "Deployed to Maven Central staging repository."

# Upload GPG key to key servers
echo "Uploading GPG key to key servers..."
./upload-gpg-key.sh

# Create git tag if in git repository
if git rev-parse --git-dir > /dev/null 2>&1; then
    echo "Creating git tag..."
    git config user.name "SharePoint List JDBC Driver Release Bot"
    git config user.email "noreply@kenstott.com"
    
    git add pom.xml
    git commit -m "Release version $RELEASE_VERSION [skip ci]"
    git tag -a "v$RELEASE_VERSION" -m "$PROJECT_NAME version $RELEASE_VERSION"
    
    # Set next development version
    mvn versions:set -DnewVersion="$NEXT_DEV_VERSION" -DgenerateBackupPoms=false
    git add pom.xml
    git commit -m "Prepare for next development iteration: $NEXT_DEV_VERSION [skip ci]"
    
    echo "Git tag created: v$RELEASE_VERSION"
    
    # Push if we have a remote
    if git remote get-url origin > /dev/null 2>&1; then
        echo "Pushing changes and tags..."
        git push origin HEAD
        git push origin "v$RELEASE_VERSION"
    fi
else
    echo "Not in a git repository, skipping git operations"
fi

echo ""
echo "Automated release complete!"
echo "Version $RELEASE_VERSION has been deployed to Maven Central staging repository."
echo ""
echo "The release should be automatically promoted to Maven Central."
echo "If manual action is required, visit: https://oss.sonatype.org/"

# Output artifacts information
JAR_FILE="target/${ARTIFACT_ID}-${RELEASE_VERSION}.jar"
JAR_WITH_DEPS="target/${ARTIFACT_ID}-${RELEASE_VERSION}-jar-with-dependencies.jar"

echo ""
echo "Artifacts built:"
if [[ -f "$JAR_FILE" ]]; then
    echo "  - $JAR_FILE"
fi
if [[ -f "$JAR_WITH_DEPS" ]]; then
    echo "  - $JAR_WITH_DEPS"
fi