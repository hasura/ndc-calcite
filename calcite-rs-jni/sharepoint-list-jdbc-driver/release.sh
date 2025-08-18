#!/bin/bash

# Manual release script for SharePoint List JDBC Driver

set -e

# Configuration
PROJECT_NAME="SharePoint List JDBC Driver"
ARTIFACT_ID="sharepoint-list-jdbc-driver"
GROUP_ID="com.kenstott.components"

echo "Starting manual release process for $PROJECT_NAME..."

# Check if we're on the right branch
CURRENT_BRANCH=$(git branch --show-current)
if [[ "$CURRENT_BRANCH" != "main" && "$CURRENT_BRANCH" != "master" ]]; then
    echo "Warning: You're not on main/master branch. Current branch: $CURRENT_BRANCH"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check for uncommitted changes
if [[ -n $(git status --porcelain) ]]; then
    echo "Error: You have uncommitted changes. Please commit or stash them first."
    git status --short
    exit 1
fi

# Get the current version from pom.xml
CURRENT_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo "Current version: $CURRENT_VERSION"

# Determine release version (remove -SNAPSHOT if present)
RELEASE_VERSION=${CURRENT_VERSION%-SNAPSHOT}
echo "Release version: $RELEASE_VERSION"

# Determine next development version
IFS='.' read -ra VERSION_PARTS <<< "$RELEASE_VERSION"
MAJOR=${VERSION_PARTS[0]}
MINOR=${VERSION_PARTS[1]}
PATCH=${VERSION_PARTS[2]}

# Increment patch version for next development cycle
NEXT_PATCH=$((PATCH + 1))
NEXT_DEV_VERSION="$MAJOR.$MINOR.$NEXT_PATCH-SNAPSHOT"
echo "Next development version: $NEXT_DEV_VERSION"

# Confirm release
echo ""
echo "Release Plan:"
echo "  Current version: $CURRENT_VERSION"
echo "  Release version: $RELEASE_VERSION"
echo "  Next dev version: $NEXT_DEV_VERSION"
echo ""
read -p "Proceed with release? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Release cancelled."
    exit 1
fi

# Update version to release version
echo "Setting release version..."
mvn versions:set -DnewVersion="$RELEASE_VERSION" -DgenerateBackupPoms=false

# Build and test
echo "Building and testing..."
mvn clean test

# Package
echo "Packaging..."
mvn package

# Check if artifacts exist
JAR_FILE="target/${ARTIFACT_ID}-${RELEASE_VERSION}.jar"
JAR_WITH_DEPS="target/${ARTIFACT_ID}-${RELEASE_VERSION}-jar-with-dependencies.jar"

if [[ ! -f "$JAR_FILE" ]]; then
    echo "Error: JAR file not found: $JAR_FILE"
    exit 1
fi

if [[ ! -f "$JAR_WITH_DEPS" ]]; then
    echo "Error: JAR with dependencies not found: $JAR_WITH_DEPS"
    exit 1
fi

# Commit release version
echo "Committing release version..."
git add pom.xml
git commit -m "Release version $RELEASE_VERSION"

# Create git tag
echo "Creating git tag..."
git tag -a "v$RELEASE_VERSION" -m "$PROJECT_NAME version $RELEASE_VERSION"

# Deploy to Maven Central (if GPG is set up and credentials are available)
if [[ -n "$SONATYPE_USERNAME" && -n "$SONATYPE_PASSWORD" && -n "$GPG_PASSPHRASE" ]]; then
    echo "Deploying to Maven Central..."
    
    # Create settings.xml from template
    envsubst < settings.xml.template > ~/.m2/settings.xml
    
    # Deploy
    mvn clean deploy -P release-sign-artifacts
    
    echo "Deployed to Maven Central staging repository."
    echo "Please go to https://oss.sonatype.org/ to release the staging repository."
else
    echo "Skipping Maven Central deployment (missing credentials)"
    echo "Artifacts are available in target/ directory:"
    echo "  - $JAR_FILE"
    echo "  - $JAR_WITH_DEPS"
fi

# Set next development version
echo "Setting next development version..."
mvn versions:set -DnewVersion="$NEXT_DEV_VERSION" -DgenerateBackupPoms=false

# Commit next development version
git add pom.xml
git commit -m "Prepare for next development iteration: $NEXT_DEV_VERSION"

# Push changes and tags
echo "Pushing changes and tags..."
git push origin "$CURRENT_BRANCH"
git push origin "v$RELEASE_VERSION"

echo ""
echo "Release process complete!"
echo "Released version: $RELEASE_VERSION"
echo "Git tag: v$RELEASE_VERSION"
echo "Artifacts:"
echo "  - $JAR_FILE"
echo "  - $JAR_WITH_DEPS"

if [[ -n "$SONATYPE_USERNAME" ]]; then
    echo ""
    echo "Next steps:"
    echo "1. Go to https://oss.sonatype.org/"
    echo "2. Log in with your Sonatype credentials"
    echo "3. Find your staging repository"
    echo "4. Close and release the repository to publish to Maven Central"
fi