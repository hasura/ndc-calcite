#!/bin/bash

# Setup GPG for SharePoint List JDBC Driver release

set -e

echo "Setting up GPG for SharePoint List JDBC Driver release..."

# Check if GPG is installed
if ! command -v gpg &> /dev/null; then
    echo "Error: GPG is not installed. Please install GPG first."
    exit 1
fi

# Check if required environment variables are set
if [[ -z "$GPG_PRIVATE_KEY_BASE64" ]]; then
    echo "Error: GPG_PRIVATE_KEY_BASE64 environment variable is not set"
    exit 1
fi

if [[ -z "$GPG_PASSPHRASE" ]]; then
    echo "Error: GPG_PASSPHRASE environment variable is not set"
    exit 1
fi

# Import GPG private key
echo "Importing GPG private key..."
echo "$GPG_PRIVATE_KEY_BASE64" | base64 --decode | gpg --batch --import

# List imported keys to verify
echo "Imported GPG keys:"
gpg --list-secret-keys

# Get the key ID
export GPG_KEY_ID=$(gpg --list-secret-keys --keyid-format LONG | grep sec | head -1 | sed 's/.*\/\([A-F0-9]*\).*/\1/')
echo "GPG Key ID: $GPG_KEY_ID"

# Configure git to use GPG key for signing
git config user.signingkey $GPG_KEY_ID
git config commit.gpgsign true

echo "GPG setup complete!"
echo "Key ID: $GPG_KEY_ID"