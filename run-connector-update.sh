cd adapters/$1
set -a; source .env; set +a
if cargo run --bin ndc-calcite-cli -- --context connector-context update; then
    echo "Configuration generated successfully."
fi
