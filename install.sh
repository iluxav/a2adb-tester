#!/bin/bash
set -e

# a2adb-tester installer
# Usage: curl -fsSL https://raw.githubusercontent.com/iluxav/a2adb-tester/main/install.sh | bash

REPO="iluxav/a2adb-tester"
BINARY_NAME="a2adb-tester"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

info() { echo -e "${GREEN}[INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# Detect OS and architecture
detect_platform() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)

    case "$OS" in
        linux)
            OS="linux"
            ;;
        darwin)
            OS="darwin"
            ;;
        *)
            error "Unsupported operating system: $OS (supported: linux, darwin)"
            ;;
    esac

    case "$ARCH" in
        x86_64|amd64)
            ARCH="amd64"
            ;;
        arm64|aarch64)
            ARCH="arm64"
            ;;
        *)
            error "Unsupported architecture: $ARCH (supported: x86_64/amd64, arm64/aarch64)"
            ;;
    esac

    PLATFORM="${OS}-${ARCH}"
    info "Detected platform: $PLATFORM"
}

# Get version (defaults to 'latest')
get_version() {
    VERSION="${VERSION:-latest}"
    info "Using version: $VERSION"
}

# Download and install binary
install_binary() {
    ARTIFACT_NAME="${BINARY_NAME}-${PLATFORM}"
    DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${ARTIFACT_NAME}"
    CHECKSUM_URL="${DOWNLOAD_URL}.sha256"

    info "Downloading ${ARTIFACT_NAME}..."

    TMP_DIR=$(mktemp -d)
    trap "rm -rf $TMP_DIR" EXIT

    TMP_FILE="${TMP_DIR}/${ARTIFACT_NAME}"
    TMP_CHECKSUM="${TMP_DIR}/${ARTIFACT_NAME}.sha256"

    # Download binary
    if ! curl -fsSL "$DOWNLOAD_URL" -o "$TMP_FILE"; then
        error "Failed to download binary from $DOWNLOAD_URL"
    fi

    # Download and verify checksum
    if curl -fsSL "$CHECKSUM_URL" -o "$TMP_CHECKSUM" 2>/dev/null; then
        info "Verifying checksum..."
        cd "$TMP_DIR"
        if command -v sha256sum &> /dev/null; then
            sha256sum -c "${ARTIFACT_NAME}.sha256" || error "Checksum verification failed"
        elif command -v shasum &> /dev/null; then
            shasum -a 256 -c "${ARTIFACT_NAME}.sha256" || error "Checksum verification failed"
        else
            warn "No sha256sum or shasum found, skipping checksum verification"
        fi
        cd - > /dev/null
    else
        warn "Checksum file not found, skipping verification"
    fi

    # Install binary
    chmod +x "$TMP_FILE"

    if [ -w "$INSTALL_DIR" ]; then
        mv "$TMP_FILE" "${INSTALL_DIR}/${BINARY_NAME}"
    else
        info "Requesting sudo to install to $INSTALL_DIR"
        sudo mv "$TMP_FILE" "${INSTALL_DIR}/${BINARY_NAME}"
    fi

    info "Installed ${BINARY_NAME} to ${INSTALL_DIR}/${BINARY_NAME}"
}

# Verify installation
verify_installation() {
    if command -v "$BINARY_NAME" &> /dev/null; then
        info "Installation successful!"
        echo ""
        echo "Run '${BINARY_NAME}' to start the server"
    else
        warn "Binary installed but not in PATH. Add ${INSTALL_DIR} to your PATH or run:"
        echo "  ${INSTALL_DIR}/${BINARY_NAME}"
    fi
}

main() {
    echo ""
    echo "  a2adb-tester installer"
    echo "  ======================"
    echo ""

    detect_platform
    get_version
    install_binary
    verify_installation

    echo ""
}

main
