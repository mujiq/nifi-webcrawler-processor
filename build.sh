#!/bin/bash

set -e

# This script builds only the Maven project to generate the NAR file
# The deployment script will use a pre-built NiFi image from Docker Hub

echo "=========================================="
echo "NiFi Web Crawler Builder"
echo "This will build the Maven project and generate the NAR file"
echo "Run ./deploy.sh afterward to deploy using the official NiFi image"
echo "=========================================="

# Check if Java 21 is installed
if ! command -v java &> /dev/null; then
    echo "Java is not installed, installing Java 21..."
    brew install openjdk@21
    sudo ln -sfn $(brew --prefix)/opt/openjdk@21/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-21.jdk
else
    java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F '.' '{print $1}')
    if [ "$java_version" != "21" ]; then
        echo "Java $java_version is installed, but we need Java 21. Installing Java 21..."
        brew install openjdk@21
        sudo ln -sfn $(brew --prefix)/opt/openjdk@21/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-21.jdk
    fi
fi

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo "Maven is not installed, installing Maven..."
    brew install maven
else
    echo "Maven is already installed."
fi

# Set JAVA_HOME to Java 21
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
echo "Using Java: $JAVA_HOME"

# Clean up the target directories
echo "Cleaning Maven target directories..."
rm -rf nifi-webcrawler-nar/target/ || true
rm -rf nifi-webcrawler-processors/target/ || true

# Extract NiFi version from pom.xml (macOS compatible)
NIFI_VERSION=$(grep "<nifi.version>" pom.xml | sed -E 's/.*<nifi.version>([^<]+)<\/nifi.version>.*/\1/')
echo "Building for NiFi version: $NIFI_VERSION"

# Build the Maven project
echo "Building the Nifi Web Crawler Maven project..."
mvn clean install -DskipTests

echo "=========================================="
echo "NAR file build completed successfully!"
echo "To deploy using the official NiFi $NIFI_VERSION image, run:"
echo "  ./deploy.sh"
echo "=========================================="

# Display the NAR file details
echo "NAR file details:"
ls -lh nifi-webcrawler-nar/target/nifi-webcrawler-nar-*.nar 