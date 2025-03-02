#!/bin/bash

set -e

# Extract NiFi version from pom.xml to ensure we use matching versions
NIFI_VERSION=$(grep "<nifi.version>" pom.xml | sed -E 's/.*<nifi.version>([^<]+)<\/nifi.version>.*/\1/')
if [ -z "$NIFI_VERSION" ]; then
    echo "ERROR: Could not determine NiFi version from pom.xml"
    exit 1
fi

echo "Using NiFi version: $NIFI_VERSION"

# Define constants
CONTAINER_NAME="nifi-webcrawler"
IMAGE_NAME="apache/nifi:$NIFI_VERSION"
DATA_VOLUME="${CONTAINER_NAME}-data"
CONFIG_VOLUME="${CONTAINER_NAME}-config"
LOGS_VOLUME="${CONTAINER_NAME}-logs"
NAR_VOLUME="${CONTAINER_NAME}-nars"

# NiFi credentials
NIFI_ADMIN_USERNAME="admin"
NIFI_ADMIN_PASSWORD=$(openssl rand -base64 12)

# Check if podman is installed
if ! command -v podman &> /dev/null; then
    echo "Podman is not installed. Installing podman..."
    brew install podman
    
    # Initialize podman if not already initialized
    if ! podman machine list 2>/dev/null | grep -q "podman-machine-default"; then
        echo "Initializing podman..."
        podman machine init --cpus 4 --memory 8192 --disk-size 40960
    fi
    
    # Start podman if not already running
    if ! podman machine list 2>/dev/null | grep -q "Currently running"; then
        echo "Starting podman..."
        podman machine start
    else
        echo "Podman machine is already running."
    fi
else
    echo "Podman is already installed."
    
    # Check if podman machine exists
    if ! podman machine list 2>/dev/null | grep -q "podman-machine-default"; then
        echo "Initializing podman machine..."
        podman machine init --cpus 4 --memory 8192 --disk-size 40960
        podman machine start
    elif ! podman machine list 2>/dev/null | grep -q "Currently running"; then
        echo "Starting podman machine..."
        podman machine start
    else
        echo "Podman machine is already running."
    fi
fi

# Check if our NAR file exists
NAR_FILE=$(find nifi-webcrawler-nar/target -name "nifi-webcrawler-nar-*.nar" -type f)
if [ -z "$NAR_FILE" ]; then
    echo "ERROR: NAR file not found. Did you run the build script?"
    echo "Please run ./build.sh first to build the NAR file."
    exit 1
fi

echo "Found NAR file: $NAR_FILE"

# Check if NiFi container is already running
if podman ps -a --filter name=$CONTAINER_NAME --filter status=running | grep -q $CONTAINER_NAME; then
    echo "NiFi container is already running. Just updating the NAR file..."
    
    # Copy the NAR file to the container's lib directory
    echo "Copying NAR file to container..."
    podman cp $NAR_FILE $CONTAINER_NAME:/opt/nifi/nifi-current/lib/
    
    # Copy to extensions directory as well for completeness
    echo "Copying NAR file to extensions directory..."
    podman exec $CONTAINER_NAME mkdir -p /opt/nifi/nifi-current/extensions
    podman cp $NAR_FILE $CONTAINER_NAME:/opt/nifi/nifi-current/extensions/
    
    echo "=========================================="
    echo "NAR file has been updated in the running NiFi container."
    echo "You may need to restart the processor or NiFi to load the new version."
    echo "To restart NiFi: podman restart $CONTAINER_NAME"
    echo "=========================================="
    exit 0
fi

# If we get here, the container isn't running, so continue with full deployment

# Pull the official NiFi image
echo "Pulling official Apache NiFi image: $IMAGE_NAME for arm64 architecture"
podman pull --platform linux/arm64 $IMAGE_NAME

# Check if necessary volumes exist, create if they don't
echo "Checking volumes..."
if ! podman volume exists $DATA_VOLUME; then
    echo "Creating data volume: $DATA_VOLUME"
    podman volume create $DATA_VOLUME
fi

if ! podman volume exists $CONFIG_VOLUME; then
    echo "Creating config volume: $CONFIG_VOLUME"
    podman volume create $CONFIG_VOLUME
fi

if ! podman volume exists $LOGS_VOLUME; then
    echo "Creating logs volume: $LOGS_VOLUME"
    podman volume create $LOGS_VOLUME
fi

if ! podman volume exists $NAR_VOLUME; then
    echo "Creating NAR volume: $NAR_VOLUME"
    podman volume create $NAR_VOLUME
fi

# Stop and remove existing container if it exists
echo "Cleaning up any existing containers..."
# Check if container exists and is running
if podman container exists $CONTAINER_NAME; then
    echo "  - Stopping container: $CONTAINER_NAME"
    podman stop -t 10 $CONTAINER_NAME || true
    echo "  - Removing container: $CONTAINER_NAME"
    podman rm -f $CONTAINER_NAME || true
fi

# Extra check for any containers with same name in any state
CONTAINER_ID=$(podman ps -a -q --filter name=$CONTAINER_NAME)
if [ -n "$CONTAINER_ID" ]; then
    echo "  - Found additional container with id $CONTAINER_ID, removing it..."
    podman rm -f $CONTAINER_ID || true
fi

# Sleep briefly to ensure cleanup is complete
echo "  - Waiting for cleanup to complete..."
sleep 2

# Get volume mount paths
NAR_VOLUME_PATH=$(podman volume inspect $NAR_VOLUME --format "{{.Mountpoint}}")
echo "NAR volume path: $NAR_VOLUME_PATH"

# Copy NAR file to the volume
echo "Copying NAR file to volume..."
podman run --platform linux/arm64 --rm -v $NAR_VOLUME:/nars -v $(pwd)/nifi-webcrawler-nar/target:/src:ro alpine sh -c "mkdir -p /nars && cp /src/nifi-webcrawler-nar-*.nar /nars/"

# Create a temporary container to generate initial configuration and to copy our NAR
echo "Setting up initial configuration..."
TEMP_CONTAINER=$(podman run --platform linux/arm64 -d $IMAGE_NAME)
sleep 5

# Copy the NAR file to the container's lib directory
podman cp $NAR_FILE $TEMP_CONTAINER:/opt/nifi/nifi-current/lib/
echo "NAR file copied to container's lib directory"

# Get the auto-generated credentials and properties
echo "Extracting auto-generated configuration..."
USERNAME_PROP=$(podman exec $TEMP_CONTAINER grep "nifi.security.identity.mapping.pattern.dn" /opt/nifi/nifi-current/conf/nifi.properties || echo "")
IDENTITY_PROP=$(podman exec $TEMP_CONTAINER grep "nifi.security.identity.mapping.value.dn" /opt/nifi/nifi-current/conf/nifi.properties || echo "")

# Stop and remove the temporary container
podman stop $TEMP_CONTAINER
podman rm $TEMP_CONTAINER

# Set environment variable for Java 21
JAVA_OPTS="-Dnifi.properties.dir=/opt/nifi/nifi-current/conf -Xms4g -Xmx8g"

# Run NiFi container
echo "Starting NiFi container..."
podman run --platform linux/arm64 -d \
    --name $CONTAINER_NAME \
    --restart unless-stopped \
    -p 8080:8080 \
    -e NIFI_WEB_HTTP_PORT=8080 \
    -e SINGLE_USER_CREDENTIALS_USERNAME=$NIFI_ADMIN_USERNAME \
    -e SINGLE_USER_CREDENTIALS_PASSWORD=$NIFI_ADMIN_PASSWORD \
    -e JAVA_OPTS="$JAVA_OPTS" \
    -v $DATA_VOLUME:/opt/nifi/nifi-current/data \
    -v $CONFIG_VOLUME:/opt/nifi/nifi-current/conf \
    -v $LOGS_VOLUME:/opt/nifi/nifi-current/logs \
    -v $NAR_VOLUME:/opt/nifi/nifi-current/extensions \
    -v $(pwd)/$NAR_FILE:/opt/nifi/nifi-current/lib/$(basename $NAR_FILE):ro \
    --cpus 4 \
    --memory 8g \
    $IMAGE_NAME

echo "Waiting for NiFi container to start..."
sleep 10

# Check if container is running
if podman container inspect $CONTAINER_NAME --format '{{.State.Running}}' | grep -q "true"; then
    echo "NiFi container is running!"
else
    echo "ERROR: NiFi container failed to start!"
    podman logs $CONTAINER_NAME
    exit 1
fi

# Get the logs to ensure NiFi is initializing properly
echo "Container logs:"
podman logs $CONTAINER_NAME | tail -n 20

echo "=========================================="
echo "NiFi is now available at: http://localhost:8080/nifi"
echo "Admin Username: $NIFI_ADMIN_USERNAME"
echo "Admin Password: $NIFI_ADMIN_PASSWORD"
echo "=========================================="
echo "To view logs: podman logs -f $CONTAINER_NAME"
echo "To stop: podman stop $CONTAINER_NAME"
echo "To restart: podman restart $CONTAINER_NAME"

# Save credentials to a file for future reference
echo "Saving credentials to nifi_credentials.txt"
cat > nifi_credentials.txt << EOL
NiFi Admin Username: $NIFI_ADMIN_USERNAME
NiFi Admin Password: $NIFI_ADMIN_PASSWORD
EOL
chmod 600 nifi_credentials.txt