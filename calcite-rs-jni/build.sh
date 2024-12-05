
cd calcite
./gradlew clean
./gradlew assemble
cd ..
mvn clean install
mvn dependency:copy-dependencies
cd py_graphql_sql
python3 build.py
cd ..
