#!/bin/bash

JAVA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED"

java $JAVA_OPTS -jar target/calcite-databricks-connector-1.0-SNAPSHOT.jar "$@"
