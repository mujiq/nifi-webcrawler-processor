#!/bin/bash

# Set strict error handling
set -e

echo "============================================"
echo "NiFi Web Crawler Environment Cleanup Script"
echo "============================================"
echo "This script will:"
echo "  - Stop and remove any running NiFi containers"
echo "  - Remove all associated volumes"
echo "  - Clean all Maven build artifacts"
echo "  - Remove temporary and generated files"
echo ""
echo "WARNING: This will DELETE ALL containers, volumes, and build artifacts!"
echo "============================================"

# Confirm before proceeding
read -p "Are you sure you want to proceed? (y/n): " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cleanup aborted."
    exit 0
fi

# Define container and volume names
CONTAINER_NAME="nifi-webcrawler"
DATA_VOLUME="${CONTAINER_NAME}-data"
CONFIG_VOLUME="${CONTAINER_NAME}-config"
LOGS_VOLUME="${CONTAINER_NAME}-logs"
NAR_VOLUME="${CONTAINER_NAME}-nars"

# Check if podman is installed and running
if command -v podman &> /dev/null; then
    echo "Found Podman installation, cleaning Podman resources..."
    
    # Stop and remove containers
    echo "Stopping and removing containers..."
    if podman ps -a | grep -q "$CONTAINER_NAME"; then
        podman stop "$CONTAINER_NAME" 2>/dev/null || true
        podman rm -f "$CONTAINER_NAME" 2>/dev/null || true
        echo "  - Container '$CONTAINER_NAME' stopped and removed"
    else
        echo "  - No container named '$CONTAINER_NAME' found"
    fi
    
    # Remove all associated volumes
    echo "Removing volumes..."
    for VOLUME in "$DATA_VOLUME" "$CONFIG_VOLUME" "$LOGS_VOLUME" "$NAR_VOLUME"; do
        if podman volume ls | grep -q "$VOLUME"; then
            podman volume rm -f "$VOLUME" 2>/dev/null || true
            echo "  - Volume '$VOLUME' removed"
        else
            echo "  - No volume named '$VOLUME' found"
        fi
    done
else
    echo "Podman not found, skipping container and volume cleanup"
fi

# Clean Maven build artifacts
echo "Cleaning Maven build artifacts..."
if command -v mvn &> /dev/null; then
    mvn clean -q || true
    echo "  - Maven clean completed"
else
    echo "  - Maven not found, removing target directories manually"
    find . -name "target" -type d -exec rm -rf {} + 2>/dev/null || true
fi

# Remove specific build artifacts
echo "Removing build artifacts and temporary files..."
rm -rf nifi-webcrawler-processors/target/ 2>/dev/null || true
rm -rf nifi-webcrawler-nar/target/ 2>/dev/null || true

# Remove credential files and logs
echo "Removing credential files and logs..."
rm -f nifi_credentials.txt 2>/dev/null || true
rm -f *.log 2>/dev/null || true

# Remove any Maven repository cached artifacts for this project (optional, commented out by default)
# echo "Removing Maven repository cached artifacts for this project..."
# rm -rf "${HOME}/.m2/repository/com/example/nifi" 2>/dev/null || true

echo "============================================"
echo "Cleanup completed successfully!"
echo "The following has been cleaned:"
echo "  - NiFi containers and volumes"
echo "  - Build artifacts and target directories"
echo "  - Temporary files and credentials"
echo "============================================" 