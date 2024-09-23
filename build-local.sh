# prevents local build artifacts being added to image
cd calcite-rs-jni
mvn clean
cd calcite
./gradlew clean

 create a tag name from the last connector release
cd ../..
#release_info=$(curl -L \
#  -H "Accept: application/vnd.github+json" \
#  -H "Authorization: Bearer $GITHUB_TOKEN" \
#  -H "X-GitHub-Api-Version: 2022-11-28" \
#  https://api.github.com/repos/hasura/ndc-calcite/releases/latest)
#TAG=$(echo "$release_info" | grep 'tag_name' | awk -F':' '{print $2}' | tr -d ' "",')
docker build . --platform linux/arm64 -t ghcr.io/hasura/meta_connector:latest
#docker buildx build --platform linux/arm64 --output type=oci,dest=./image.tar .
#docker tag ghcr.io/hasura/meta_connector:latest ghcr.io/hasura/meta_connector:"$TAG"
#docker push ghcr.io/hasura/meta_connector:latest
#docker push ghcr.io/hasura/meta_connector:"$TAG"

cd calcite-rs-jni/calcite
./gradlew assemble
cd ..
mvn install -DskipTests
