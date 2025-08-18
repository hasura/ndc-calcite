#!/bin/bash

echo "Configuring DataGrip to use Java 21..."

# Create the JDK configuration file for DataGrip
DATAGRIP_CONFIG_DIR="$HOME/Library/Application Support/JetBrains/DataGrip2024.1"
JAVA_21_HOME="/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home"

if [ ! -d "$JAVA_21_HOME" ]; then
    echo "Error: Java 21 not found at $JAVA_21_HOME"
    echo "Please ensure Java 21 is installed"
    exit 1
fi

# Create the JDK configuration file
echo "$JAVA_21_HOME" > "$DATAGRIP_CONFIG_DIR/datagrip.jdk"

echo "DataGrip has been configured to use Java 21"
echo
echo "Next steps:"
echo "1. Start DataGrip"
echo "2. It should now use Java 21 and be able to load the driver"
echo
echo "If DataGrip doesn't start or has issues:"
echo "  Delete the file: $DATAGRIP_CONFIG_DIR/datagrip.jdk"
echo "  to revert to the default JDK"