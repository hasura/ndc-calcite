
cd calcite
./gradlew assemble
cd ..
mvn clean install -Dcalcite_core=/Users/kennethstott/calcite/core/build/libs -X
mvn dependency:copy-dependencies -Dcalcite_core=/Users/kennethstott/calcite/core/build/libs