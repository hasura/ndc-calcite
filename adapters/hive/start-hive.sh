#!/bin/bash

set -e

echo "Starting Hive service: $1"
echo "Current user: $(whoami)"
echo "Current directory: $(pwd)"
echo "Listing Hive directory:"
ls -la $HIVE_HOME
echo "Hive configuration:"
cat $HIVE_HOME/conf/hive-site.xml

if [ "$1" = "metastore" ]; then
    echo "Initializing or upgrading metastore schema..."
    schematool -dbType postgres -initSchema
    echo "Starting Metastore service..."
    exec hive --service metastore
elif [ "$1" = "hiveserver2" ]; then
    echo "Starting HiveServer2 service..."
    exec hive --service hiveserver2
else
    echo "Unknown service: $1"
    exit 1
fi