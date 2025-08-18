#!/bin/bash

echo "This script will clear DataGrip's cache to remove stale JDBC driver information."
echo "Please make sure DataGrip is closed before proceeding."
read -p "Is DataGrip closed? (y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Please close DataGrip and run this script again."
    exit 1
fi

CACHE_DIR="$HOME/Library/Caches/JetBrains/DataGrip2024.1"

if [ -d "$CACHE_DIR" ]; then
    echo "Clearing DataGrip cache..."
    
    # Clear main caches
    echo "Clearing main cache files..."
    rm -rf "$CACHE_DIR/caches/"*
    
    # Clear data source cache
    echo "Clearing data source cache..."
    rm -rf "$CACHE_DIR/data-source/"*
    
    # Clear index files
    echo "Clearing index files..."
    rm -f "$CACHE_DIR"/*.dat
    
    # Clear jar registry if it exists
    if [ -d "$CACHE_DIR/jarRepository" ]; then
        echo "Clearing JAR repository..."
        rm -rf "$CACHE_DIR/jarRepository/"*
    fi
    
    echo "Cache cleared successfully!"
    echo
    echo "Next steps:"
    echo "1. Start DataGrip"
    echo "2. Go to your Splunk driver configuration"
    echo "3. Remove and re-add the JAR file:"
    echo "   $HOME/ndc-calcite/calcite-rs-jni/splunk-jdbc-driver/target/splunk-jdbc-driver-1.0.1-jar-with-dependencies.jar"
    echo "4. Select 'com.hasura.SplunkDriver' from the driver class dropdown"
else
    echo "DataGrip cache directory not found at: $CACHE_DIR"
    echo "You may have a different version of DataGrip."
fi