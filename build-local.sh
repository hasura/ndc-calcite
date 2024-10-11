# prevents local build artifacts being added to image
cd calcite-rs-jni
mvn clean
cd calcite
./gradlew clean

 create a tag name from the last connector release
cd ../..
docker build . -t ghcr.io/hasura/ndc-calcite:latest

cd calcite-rs-jni/calcite
./gradlew assemble
cd ..
mvn install -DskipTests
