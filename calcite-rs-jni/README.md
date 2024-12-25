# Hasura components dependent on Apache Calcite

* `jni` - Provides a Rust JNI interface between Apache Calcite and the Hasura Calcite NDC.
* `jni-arrow` - Provides a JNI interface using Arrow buffers for data transfer
* `py-graphl-sql` - Provides a Python DB API library for advanced analytics, AI and GenAI support
* `sqlengine` - Provides an HTTP server for SQL queries
* `jdbc` - Provides a JDBC driver for interacting with a Hasura DDN endpoint
* `calcite` - A custom fork of Apache Calcite
* `bigquery` - Packages bigquery jars for deployment
* `build.sh` - Builds all components (The key issue is managing the handoff and dependencies between gradle and maven builds)