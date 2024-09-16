cd ../..
docker build --platform linux/arm64,linux/amd64 . -t ghcr.io/hasura/meta_connector:latest
