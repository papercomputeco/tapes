#!/bin/bash

# tapes install script for Linux based operating systems.
# Requirements:
# * curl
# * uname
# * /tmp directory

set -e

VERSION="${TAPES_VERSION:-latest}"
BASE_URL="https://download.tapes.dev"

# Detect OS
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$OS" in
  linux*) OS="linux" ;;
  darwin*) OS="darwin" ;;
  *) echo "Unsupported OS: $OS"; exit 1 ;;
esac

# Detect architecture
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64) ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

# Download and install
DOWNLOAD_URL="$BASE_URL/$VERSION/$OS/$ARCH/tapes"
INSTALL_DIR="${TAPES_INSTALL_DIR:-/usr/local/bin}"

echo "Downloading tapes $VERSION for $OS/$ARCH ..."
curl -fsSL "$DOWNLOAD_URL" -o /tmp/tapes
chmod +x /tmp/tapes

echo "Installing to $INSTALL_DIR ..."
sudo mv /tmp/tapes "$INSTALL_DIR/tapes"

echo "Installed tapes version:"
tapes version
