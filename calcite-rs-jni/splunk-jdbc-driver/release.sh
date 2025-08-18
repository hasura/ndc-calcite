#!/bin/bash

# Release Script for Splunk JDBC Driver
# This script automates the Maven Central deployment process

set -e  # Exit on error

echo "Splunk JDBC Driver - Maven Central Release"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo -e "${RED}Error: Maven is not installed${NC}"
    exit 1
fi

# Check if settings.xml exists
if [ ! -f "$HOME/.m2/settings.xml" ]; then
    echo -e "${RED}Error: ~/.m2/settings.xml not found${NC}"
    echo "Please copy settings.xml.template to ~/.m2/settings.xml and update credentials"
    exit 1
fi

# Check if GPG is configured
if ! gpg --list-secret-keys | grep -q "sec"; then
    echo -e "${RED}Error: No GPG keys found${NC}"
    echo "Please run ./setup-gpg.sh first"
    exit 1
fi

# Get current version
CURRENT_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo -e "Current version: ${GREEN}$CURRENT_VERSION${NC}"

# Check if it's a SNAPSHOT
if [[ $CURRENT_VERSION == *"-SNAPSHOT"* ]]; then
    echo -e "${YELLOW}Warning: Current version is a SNAPSHOT${NC}"
    read -p "Do you want to release a SNAPSHOT? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        # Remove SNAPSHOT for release
        RELEASE_VERSION=${CURRENT_VERSION%-SNAPSHOT}
        echo -e "Updating version to: ${GREEN}$RELEASE_VERSION${NC}"
        mvn versions:set -DnewVersion=$RELEASE_VERSION
        mvn versions:commit
    fi
fi

# Build Calcite first
echo -e "\n${YELLOW}Building Calcite dependencies...${NC}"
cd ../calcite
./gradlew assemble
cd ../splunk-jdbc-driver

# Clean and test
echo -e "\n${YELLOW}Running tests...${NC}"
mvn clean test

if [ $? -ne 0 ]; then
    echo -e "${RED}Tests failed! Aborting release.${NC}"
    exit 1
fi

# Ask for GPG passphrase
echo -e "\n${YELLOW}GPG Configuration${NC}"
read -s -p "Enter GPG passphrase: " GPG_PASSPHRASE
echo

# Deploy
echo -e "\n${YELLOW}Deploying to Maven Central...${NC}"
mvn clean deploy -P release -Dgpg.passphrase="$GPG_PASSPHRASE"

if [ $? -eq 0 ]; then
    echo -e "\n${GREEN}Deployment successful!${NC}"
    echo -e "Next steps:"
    echo -e "1. Log in to https://s01.oss.sonatype.org"
    echo -e "2. Check Staging Repositories"
    echo -e "3. Close and Release your repository"
    
    # Ask about next version
    echo -e "\n${YELLOW}Version Management${NC}"
    read -p "Update to next SNAPSHOT version? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        read -p "Enter next version (without -SNAPSHOT): " NEXT_VERSION
        mvn versions:set -DnewVersion=${NEXT_VERSION}-SNAPSHOT
        mvn versions:commit
        echo -e "${GREEN}Version updated to ${NEXT_VERSION}-SNAPSHOT${NC}"
    fi
else
    echo -e "${RED}Deployment failed!${NC}"
    exit 1
fi

echo -e "\n${GREEN}Release process complete!${NC}"