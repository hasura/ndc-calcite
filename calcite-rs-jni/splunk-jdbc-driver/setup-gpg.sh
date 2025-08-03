#!/bin/bash

# GPG Key Setup Script for Maven Central Deployment
# This script helps set up GPG keys for signing artifacts

echo "GPG Key Setup for Maven Central Deployment"
echo "=========================================="

# Check if GPG is installed
if ! command -v gpg &> /dev/null; then
    echo "Error: GPG is not installed. Please install GPG first."
    echo "  macOS: brew install gnupg"
    echo "  Ubuntu/Debian: sudo apt-get install gnupg"
    echo "  RHEL/CentOS: sudo yum install gnupg"
    exit 1
fi

# List existing keys
echo ""
echo "Existing GPG keys:"
gpg --list-secret-keys --keyid-format LONG

# Ask if user wants to create a new key
echo ""
read -p "Do you want to create a new GPG key? (y/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Creating new GPG key..."
    echo "Please follow the prompts. Recommended settings:"
    echo "  - Key type: RSA and RSA (default)"
    echo "  - Key size: 4096 bits"
    echo "  - Validity: 2 years"
    echo ""
    gpg --full-generate-key
fi

# Display keys again
echo ""
echo "Current GPG keys:"
gpg --list-secret-keys --keyid-format LONG

# Get key ID
echo ""
echo "Please copy your GPG key ID from the sec line above (after rsa4096/)"
read -p "Enter your GPG key ID: " KEY_ID

# Export public key
echo ""
echo "Exporting public key..."
gpg --armor --export $KEY_ID > gpg-public-key.asc
echo "Public key exported to: gpg-public-key.asc"

# Instructions for uploading to key servers
echo ""
echo "Uploading key to public key servers..."
gpg --keyserver hkp://keyserver.ubuntu.com --send-keys $KEY_ID
gpg --keyserver hkp://keys.openpgp.org --send-keys $KEY_ID
gpg --keyserver hkp://pgp.mit.edu --send-keys $KEY_ID

echo ""
echo "GPG Setup Complete!"
echo "=================="
echo ""
echo "Next steps:"
echo "1. Keep your GPG private key secure!"
echo "2. Remember your GPG passphrase"
echo "3. Update settings.xml with your GPG passphrase"
echo "4. Your public key has been uploaded to key servers"
echo "5. You may need to verify the key on https://keys.openpgp.org"
echo ""
echo "For Maven deployment, ensure your settings.xml contains:"
echo "  <gpg.passphrase>YOUR_PASSPHRASE</gpg.passphrase>"