release_info=$(curl -L \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer $GITHUB_TOKEN" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/hasura/ndc-calcite/releases/latest)
TAG=$(echo "$release_info" | grep 'tag_name' | awk -F':' '{print $2}' | tr -d ' "",')
tar -czvf connector-definition.tgz  connector-definition
docker build . --platform linux/arm64,linux/amd64 -t kstott/meta_connector:latest
docker tag kstott/meta_connector:latest kstott/meta_connector:"$TAG"
docker push kstott/meta_connector:latest
docker push kstott/meta_connector:"$TAG"
