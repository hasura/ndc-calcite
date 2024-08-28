
cd calcite
./gradlew clean
./gradlew assemble
cd ..
mvn clean install
mvn dependency:copy-dependencies
