my mist#!/bin/bash

# Manual GPG Key Upload Script
# Use this if automatic upload failed

echo "Manual GPG Key Upload"
echo "===================="

# Get the key ID from the user or detect it
if [ -z "$1" ]; then
    echo "Detecting your GPG key..."
    KEY_ID=$(gpg --list-secret-keys --keyid-format LONG | grep sec | head -1 | awk '{print $2}' | cut -d'/' -f2)
    echo "Found key: $KEY_ID"
else
    KEY_ID=$1
fi

echo ""
echo "Option 1: Upload via Web (RECOMMENDED)"
echo "======================================"
echo "1. Visit: https://keys.openpgp.org/upload"
echo "2. Upload the file: gpg-public-key.asc"
echo "3. Check your email for verification link"
echo "4. Click the link to verify your key"
echo ""

echo "Option 2: Try alternative key servers"
echo "====================================="
echo "Trying alternative servers..."

# Try with explicit port
echo "Trying keyserver.ubuntu.com with port 80..."
gpg --keyserver hkp://keyserver.ubuntu.com:80 --send-keys $KEY_ID

# Try HTTPS protocol
echo "Trying keys.openpgp.org with HTTPS..."
gpg --keyserver https://keys.openpgp.org --send-keys $KEY_ID

# Try pool
echo "Trying pool.sks-keyservers.net..."
gpg --keyserver hkp://pool.sks-keyservers.net --send-keys $KEY_ID

echo ""
echo "Option 3: Use curl to upload"
echo "============================"
echo "If GPG upload fails, you can manually upload:"
echo ""
echo "curl -X POST https://keys.openpgp.org/upload -F keytext=@gpg-public-key.asc"
echo ""

echo "Option 4: For corporate networks"
echo "================================"
echo "If behind a proxy, set:"
echo "export http_proxy=http://your-proxy:port"
echo "export https_proxy=http://your-proxy:port"
echo ""

echo "Verification"
echo "============"
echo "After upload, verify your key at:"
echo "https://keys.openpgp.org/search?q=$KEY_ID"
echo ""
echo "Your key ID: $KEY_ID"