# metabase-pinot-driver
Apache Pinot driver for the Metabase business intelligence front-end

## Overview

This repository contains the Apache Pinot driver for Metabase, allowing you to connect Metabase to Apache Pinot databases for data analysis and visualization.

## Features

- Connect to Apache Pinot databases via HTTP API
- Support for authentication (Basic and Bearer tokens)
- Query options configuration
- SSH tunnel support
- Full MBQL to Pinot SQL translation
- Support for aggregations, filters, grouping, and ordering

## Prerequisites

- Git
- Docker
- Java 17
- Clojure 1.12.1.1550
- NodeJS 22
- NPM 10
- Yarn 1.22
- Apache Pinot 1.3.0
- Metabase v0.55.7

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/metabase-pinot-driver.git
   cd metabase-pinot-driver
   ```

2. Build the driver:
   ```bash
   make build
   ```

3. Start Metabase with the Pinot driver:
   ```bash
   make server
   ```

## Configuration

### Connection Properties

- **Controller endpoint**: The URL of your Pinot controller (e.g., `http://localhost:9000`)
- **Database name**: Optional database name for multi-tenant setups
- **Authentication**: Optional Basic or Bearer token authentication
- **Query options**: Optional semicolon-separated query options (e.g., `timeoutMs=10000;useMultistageEngine=false`)

### Example Configuration

```yaml
controller-endpoint: http://localhost:9000
database-name: my_database
auth-enabled: true
auth-token-type: Bearer
auth-token-value: your-token-here
query-options: timeoutMs=10000;useMultistageEngine=false
```

## Development

### Project Structure

```
metabase-pinot-driver/
├── drivers/pinot/
│   ├── src/metabase/driver/pinot/
│   │   ├── pinot.clj              # Main driver file
│   │   ├── client.clj             # HTTP client for Pinot API
│   │   ├── execute.clj            # Query execution
│   │   ├── query_processor.clj    # MBQL to SQL translation
│   │   └── sync.clj               # Database and table sync
│   ├── resources/
│   │   └── metabase-plugin.yaml   # Driver configuration
│   └── deps.edn                   # Dependencies
├── Makefile                       # Build automation
├── app_versions.json             # Version configuration
└── package.json                  # Node.js dependencies
```

### Building

```bash
# Build the driver
make driver

# Build everything (Metabase + driver)
make build

# Run tests
make test
```

### Testing

The driver includes comprehensive tests. To run them:

```bash
make test
```

This will:
1. Start a Pinot instance (if not already running)
2. Link the driver to Metabase
3. Run the test suite

## Releases

### Creating a Release

This project uses GitHub Actions for automated releases. There are two ways to create a release:

#### 1. Using GitHub Actions (Recommended)

1. Go to the [Actions tab](https://github.com/your-username/metabase-pinot-driver/actions)
2. Select the "Release" workflow
3. Click "Run workflow"
4. Fill in the required parameters:
   - **Version**: Semantic version (e.g., `1.0.0`, `1.0.0-alpha.1`)
   - **Commit SHA**: The full commit SHA to release from
   - **Prerelease**: Check if this is a prerelease

#### 2. Using the Release Script

Use the provided release script for a more convenient CLI experience:

```bash
# Basic release
./scripts/release.sh 1.0.0 abc123def456...

# Prerelease
./scripts/release.sh --prerelease 1.0.0-alpha.1 abc123def456...

# Get help
./scripts/release.sh --help
```

**Prerequisites for the script:**
- [GitHub CLI](https://cli.github.com/) installed and authenticated
- Permissions to create releases on the repository

### Release Process

The release workflow will:
1. Validate the version format and commit SHA
2. Build the Pinot driver JAR
3. Create release artifacts with checksums
4. Generate release notes
5. Create a GitHub release with the artifacts attached

### Release Artifacts

Each release includes:
- `pinot.metabase-driver-v{version}.jar` - The driver JAR file
- `pinot.metabase-driver-v{version}.jar.sha256` - SHA256 checksum
- Automatically generated release notes

### Installing a Release

1. Download the driver JAR from the [releases page](https://github.com/your-username/metabase-pinot-driver/releases)
2. Place it in your Metabase `plugins` directory
3. Restart Metabase
4. The Pinot driver will be available for database connections

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details.

## Support

For issues and questions:
- Create an issue on GitHub
- Check the [Apache Pinot documentation](https://pinot.apache.org/docs)
- Check the [Metabase documentation](https://www.metabase.com/docs)

## Related Links

- [Apache Pinot](https://pinot.apache.org/)
- [Metabase](https://www.metabase.com/)
