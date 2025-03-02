# Contributing to NiFi Web Crawler Processor

First off, thank you for considering contributing to the NiFi Web Crawler Processor! It's people like you that make this processor better for everyone.

## How Can I Contribute?

### Reporting Bugs

This section guides you through submitting a bug report. Following these guidelines helps maintainers understand your report, reproduce the behavior, and find related reports.

* Use the bug report template when creating an issue
* Include as many details as possible
* Describe the exact steps to reproduce the problem
* Explain which behavior you expected to see instead and why
* Include screenshots if possible

### Suggesting Enhancements

This section guides you through submitting an enhancement suggestion, including completely new features and minor improvements to existing functionality.

* Use the feature request template when creating an issue
* Provide a clear and detailed explanation of the feature you want
* Explain why this enhancement would be useful to most users
* List some other applications where this enhancement exists, if applicable

### Pull Requests

* Fill in the required template
* Do not include issue numbers in the PR title
* Include screenshots and animated GIFs in your pull request whenever possible
* Follow the Java style guide
* Include appropriate test cases
* End all files with a newline
* Avoid platform-dependent code

## Development Setup

### Prerequisites

* Java 21
* Maven 3.x
* Podman or Docker (for testing)

### Building and Testing

1. Fork the repository
2. Clone your fork
3. Build the processor:
   ```
   ./build.sh
   ```
4. Test your changes:
   ```
   mvn test
   ```

### Code Style

* Follow standard Java naming conventions
* Use descriptive variable names
* Write JavaDoc comments for all public methods
* Keep lines under 120 characters
* Use 4 spaces for indentation
* Remove unused imports

## Pull Request Process

1. Ensure your code builds and all tests pass
2. Update the README.md with details of changes to the interface, if applicable
3. Update the project version in pom.xml files if you're adding features
4. The PR will be merged once it receives approval from maintainers

## License

By contributing, you agree that your contributions will be licensed under the project's Apache License 2.0. 