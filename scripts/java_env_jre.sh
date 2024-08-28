#!/bin/bash
# Setting JAVA_HOME based on architecture
export JAVA_HOME_ARM64=/usr/lib/jvm/jre-21-openjdk-arm64
export JAVA_HOME_AMD64=/usr/lib/jvm/jre-21-openjdk-amd64
if [ "$(uname -m)" = "aarch64" ]; then
    # Setting for ARM64
    echo "Setting JAVA_HOME for ARM64"
    ln -s ${JAVA_HOME_ARM64} /usr/local/java_home
else
    # Setting for AMD64
    echo "Setting JAVA_HOME for AMD64"
    ln -s ${JAVA_HOME_AMD64} /usr/local/java_home
fi
export JAVA_HOME=/usr/local/java_home
export PATH=$JAVA_HOME/bin:$PATH
