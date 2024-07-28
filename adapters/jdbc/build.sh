cd ../..
docker build . --platform linux/amd64,linux/arm64 -t kstott/jdbc_connector:latest
