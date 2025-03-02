# NiFi Web Crawler Processor

A custom Apache NiFi processor for recursively crawling REST APIs using a Depth-First Search (DFS) approach. This processor is particularly useful for paginated APIs and APIs with deeply nested resources.

## Features

- Recursively crawl REST APIs using DFS approach
- Configure pagination link extraction via JSONPath
- Extract and follow resource links using JSONPath
- Filter URLs to crawl using regex patterns
- Support for various authentication mechanisms (Basic, Bearer, API Key)
- TLS/SSL support
- Complete crawl metadata as FlowFile attributes

## Requirements

- Java 21
- Maven 3.x
- Podman (for container deployment)
- macOS (for the build scripts)

## Quick Start

### Build and Deploy

1. Make the scripts executable:
   ```
   chmod +x build.sh deploy.sh
   ```

2. Build the NAR file (processor module):
   ```
   ./build.sh
   ```

3. Deploy using the official Apache NiFi container:
   ```
   ./deploy.sh
   ```

4. Access the NiFi web UI at http://localhost:8080/nifi

### Workflow Explanation

The workflow is divided into two separate scripts:

- `build.sh`: Builds only the Maven project to create the NAR file
- `deploy.sh`: Deploys using the official Apache NiFi image and installs the NAR file

This separation allows you to:
1. Build the processor once
2. Deploy multiple times without rebuilding
3. Update the processor code and rebuild only when needed

### Processor Configuration

After adding the processor to your NiFi canvas, configure it with these key properties:

- **Base URL**: The starting URL for the crawler
- **Maximum Depth**: How deep to crawl (0 means only crawl the base URL)
- **Pagination Link JSONPath**: JSONPath to extract pagination links (e.g., `$.links.next`)
- **Resource Links JSONPath**: JSONPath to extract resource links (e.g., `$.items[*].href`)
- **URL Pattern**: Optional regex to filter which URLs to follow
- **Authentication Type**: Type of authentication to use (None, Basic, Bearer, API Key)

## Processor Properties

| Property | Description | Default |
|----------|-------------|---------|
| Base URL | The base URL to start crawling from | |
| Maximum Depth | Maximum recursion depth | 5 |
| URL Pattern | Regex pattern to match URLs for recursion | |
| Pagination Link JSONPath | JSONPath expression to extract pagination link | |
| Resource Links JSONPath | JSONPath expression to extract resource links | |
| Connection Timeout | Max wait time for connection (ms) | 30000 |
| Read Timeout | Max wait time for data (ms) | 30000 |
| SSL Context Service | SSL Context Service for TLS/SSL connections | |
| Request Headers | Additional headers as key-value pairs | |
| Authentication Type | Authentication type to use | None |

## FlowFile Attributes

The processor adds the following attributes to generated FlowFiles:

- `webcrawler.url`: The URL of the API endpoint
- `webcrawler.depth`: The depth level of the crawled resource
- `webcrawler.parent.url`: The parent URL that led to this resource
- `webcrawler.content.type`: Content type of the response
- `webcrawler.status.code`: HTTP status code
- `webcrawler.timestamp`: Timestamp when the resource was crawled
- `webcrawler.error.message`: Error message (for failed requests)

## Architecture

The processor implements a recursive DFS approach:
1. Start with the base URL
2. For each URL:
   - Fetch the content
   - Create a FlowFile with the response
   - Extract links using configured JSONPath expressions
   - Add new links to the crawl stack
   - Continue until the stack is empty or max depth is reached

## Container Resources

The Podman container is configured with:
- 4 CPU cores
- 8GB RAM
- 80GB disk space

## Development

### Project Structure

```
.
├── build.sh                  # Build script for NAR file
├── deploy.sh                 # Deployment script for NiFi container
├── pom.xml                   # Maven parent project
├── nifi-webcrawler-processors/ # Processor implementation
└── nifi-webcrawler-nar/      # NAR packaging
```

### Building from Source

```
mvn clean install
```

### Running Tests

```
mvn test
```

## License

Apache License 2.0 