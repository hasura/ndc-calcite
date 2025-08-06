#!/bin/bash

# Automated Release Script for Splunk JDBC Driver
# This script handles version incrementing and deployment

set -e  # Exit on error

echo "Splunk JDBC Driver - Automated Release"
echo "======================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to increment version
increment_version() {
    local version=$1
    local release_type=$2
    
    # Split version into array
    IFS='.' read -ra VERSION_PARTS <<< "$version"
    
    case $release_type in
        major)
            ((VERSION_PARTS[0]++))
            VERSION_PARTS[1]=0
            VERSION_PARTS[2]=0
            ;;
        minor)
            ((VERSION_PARTS[1]++))
            VERSION_PARTS[2]=0
            ;;
        patch)
            ((VERSION_PARTS[2]++))
            ;;
    esac
    
    echo "${VERSION_PARTS[0]}.${VERSION_PARTS[1]}.${VERSION_PARTS[2]}"
}

# Check if we're on a clean working directory
if [[ -n $(git status -s) ]]; then
    echo -e "${RED}Error: Working directory has uncommitted changes${NC}"
    echo "Please commit or stash your changes before releasing"
    exit 1
fi

# Get current version
CURRENT_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout | grep -v '\[')
echo -e "Current version: ${BLUE}$CURRENT_VERSION${NC}"

# Remove -SNAPSHOT if present
BASE_VERSION=${CURRENT_VERSION%-SNAPSHOT}

# Ask for release type
echo -e "\n${YELLOW}Select release type:${NC}"
echo "1) Patch release (bug fixes)"
echo "2) Minor release (new features, backward compatible)"
echo "3) Major release (breaking changes)"
echo "4) Custom version"
read -p "Enter choice (1-4): " choice

case $choice in
    1)
        RELEASE_TYPE="patch"
        NEW_VERSION=$(increment_version $BASE_VERSION patch)
        ;;
    2)
        RELEASE_TYPE="minor"
        NEW_VERSION=$(increment_version $BASE_VERSION minor)
        ;;
    3)
        RELEASE_TYPE="major"
        NEW_VERSION=$(increment_version $BASE_VERSION major)
        ;;
    4)
        read -p "Enter custom version: " NEW_VERSION
        RELEASE_TYPE="custom"
        ;;
    *)
        echo -e "${RED}Invalid choice${NC}"
        exit 1
        ;;
esac

echo -e "\nWill release version: ${GREEN}$NEW_VERSION${NC}"
read -p "Continue? (y/n): " -n 1 -r
echo

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Release cancelled"
    exit 0
fi

# Build Calcite dependencies first
echo -e "\n${YELLOW}Building Calcite dependencies...${NC}"
cd ../calcite
./gradlew assemble || echo "Calcite build had issues, but JARs might exist"
cd ../splunk-jdbc-driver

# Option 1: Using maven-release-plugin (recommended)
use_release_plugin() {
    echo -e "\n${YELLOW}Using maven-release-plugin...${NC}"
    
    # Prepare the release (updates versions, creates tag)
    mvn release:prepare \
        -DreleaseVersion=$NEW_VERSION \
        -DdevelopmentVersion=$(increment_version $NEW_VERSION patch)-SNAPSHOT \
        -DautoVersionSubmodules=true \
        -Dresume=false
    
    # Perform the release (builds and deploys)
    mvn release:perform
}

# Option 2: Manual version update and deploy
manual_release() {
    echo -e "\n${YELLOW}Performing manual release...${NC}"
    
    # Update version
    mvn versions:set -DnewVersion=$NEW_VERSION
    mvn versions:commit
    
    # Clean and test
    mvn clean test
    
    # Deploy
    mvn clean deploy -P release
    
    # Create git tag
    git add pom.xml
    git commit -m "Release version $NEW_VERSION"
    git tag -a "splunk-jdbc-driver-$NEW_VERSION" -m "Release version $NEW_VERSION"
    
    # Update to next development version
    NEXT_VERSION=$(increment_version $NEW_VERSION patch)-SNAPSHOT
    mvn versions:set -DnewVersion=$NEXT_VERSION
    mvn versions:commit
    git add pom.xml
    git commit -m "Prepare for next development iteration"
}

# Ask which method to use
echo -e "\n${YELLOW}Select release method:${NC}"
echo "1) Use maven-release-plugin (recommended)"
echo "2) Manual release"
read -p "Enter choice (1-2): " method_choice

case $method_choice in
    1)
        use_release_plugin
        ;;
    2)
        manual_release
        ;;
    *)
        echo -e "${RED}Invalid choice${NC}"
        exit 1
        ;;
esac

echo -e "\n${GREEN}Release complete!${NC}"
echo -e "Version ${GREEN}$NEW_VERSION${NC} has been deployed to Maven Central"
echo -e "\nNext steps:"
echo -e "1. Push commits and tags: ${YELLOW}git push origin main --tags${NC}"
echo -e "2. Check deployment at: ${BLUE}https://central.sonatype.com/artifact/com.kenstott.components/splunk-jdbc-driver/$NEW_VERSION${NC}"
echo -e "3. Maven Central sync takes ~30 minutes"