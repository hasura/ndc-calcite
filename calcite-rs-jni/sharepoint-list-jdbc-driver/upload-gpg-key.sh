#!/bin/bash

# Upload GPG public key to key servers for SharePoint List JDBC Driver

set -e

echo "Uploading GPG public key to key servers..."

# Check if GPG is installed
if ! command -v gpg &> /dev/null; then
    echo "Error: GPG is not installed. Please install GPG first."
    exit 1
fi

# Get the key ID
export GPG_KEY_ID=$(gpg --list-secret-keys --keyid-format LONG | grep sec | head -1 | sed 's/.*\/\([A-F0-9]*\).*/\1/')

if [[ -z "$GPG_KEY_ID" ]]; then
    echo "Error: No GPG key found. Please run setup-gpg.sh first."
    exit 1
fi

echo "Found GPG Key ID: $GPG_KEY_ID"

# Upload to multiple key servers for redundancy
KEY_SERVERS=(
    "hkp://keyserver.ubuntu.com:80"
    "hkp://keys.openpgp.org:80"
    "hkp://pgp.mit.edu:80"
)

for server in "${KEY_SERVERS[@]}"; do
    echo "Uploading to $server..."
    if gpg --keyserver "$server" --send-keys "$GPG_KEY_ID"; then
        echo "Successfully uploaded to $server"
    else
        echo "Failed to upload to $server (continuing...)"
    fi
done

echo "GPG key upload process complete!"
echo "Key ID: $GPG_KEY_ID"

# Show the public key for manual verification
echo ""
echo "Public key (for manual verification):"
gpg --armor --export "$GPG_KEY_ID"