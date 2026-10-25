# metabase-pinot-driver
[![CI](https://github.com/startreedata/metabase-pinot-driver/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/startreedata/metabase-pinot-driver/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/startreedata/metabase-pinot-driver/branch/main/graph/badge.svg)](https://codecov.io/gh/startreedata/metabase-pinot-driver)
[![License](https://img.shields.io/github/license/startreedata/metabase-pinot-driver)](LICENSE)

Apache Pinot driver for the Metabase business intelligence front-end

## Overview

This repository contains the Apache Pinot driver for Metabase, allowing you to connect Metabase to Apache Pinot databases for data analysis and visualization.

[Integrate Apache Pinot with Metabase](https://docs.pinot.apache.org/integrations/metabase)


## Features

- Connect to Apache Pinot databases via HTTP API
- Support for authentication (Basic and Bearer tokens)
- Query options configuration
- SSH tunnel support
- Full MBQL to Pinot SQL translation
- Support for aggregations, filters, grouping, and ordering

## Prerequisites

- Git
- Java 25 (Eclipse Temurin)
- Clojure CLI 1.12.3.1577
- Apache Pinot 1.5.1
- Metabase v0.64.3 (the pinned build target)

Building the Metabase frontend with `make build` or `make front_end` also requires NodeJS 22.23.2 and Bun 1.3.14. `make driver` only requires Git, Java, Clojure CLI, Make, and jq.

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/startreedata/metabase-pinot-driver.git
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

On **Admin > Databases > Add a database**, select **StarTree(Pinot)**. The Metabase
frontend built by `make build` or `make front_end` includes the StarTree logo.
Stock Metabase v0.63/v0.64 does not load connector logos from driver JARs; a JAR-only
installation changes the name and keeps the generic database icon.

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
1. Clone the pinned Metabase source (if not already present)
2. Link the driver to Metabase
3. Run the existing driver test suite once with coverage

These tests use mocked Pinot responses. Release compatibility also requires loading the packaged driver in the pinned Metabase version and checking a real Pinot connection, schema sync, native SQL, MBQL, and SQL parameters.

`make test` requires Python 3 and enforces 68% line coverage. Form coverage is reported separately because upstream logging macro expansion changes its denominator between Metabase versions.

### Tested Compatibility

On 2026-10-25, the same unmodified **0.64.3** driver JAR passed all 13 packaged
runtime checks for each combination below (Metabase on Java 25, Pinot on Java 21):

| Metabase runtime | Pinot release | Result |
|:-----------------|:--------------|:-------|
| v0.63.21 (`0a1771f`) | 1.5.1 | 13/13 passed |
| v0.64.3 (`dc449ac`) | 1.5.1 | 13/13 passed |

Connection, schema sync, native SQL, MBQL selection and aggregation, ordering,
and number/text/field/optional parameters matched direct Pinot results for each
release. The existing unit suite passed with 97.77% source-line coverage.
CI builds once, runs the unit suite once, and checks both declared Metabase
runtimes against the pinned Pinot 1.5.1 quickstart. The build target and embedded
driver version are v0.64.3 and 0.64.3 respectively.
See [the reproducible smoke command](docs/development.md#packaged-driver-compatibility).

### Manual Testing Notes (Variable Substitution)

The following behaviors were manually verified for **variable substitution** (SQL parameters: https://www.metabase.com/docs/latest/questions/native-editor/sql-parameters) in Metabase with the Pinot driver:

| Variable type | Filter widget | Result | Notes |
| --- | --- | --- | --- |
| Number | Dropdown list (custom or from another model/question) | Pass | |
| Number | Search box (custom only) | Pass | Multiple selections are allowed. Use `IN` for multi-select; `=` only works for a single value. `IN` works for both single and multiple values. |
| Number | Input box | Pass | |
| Text | Input box | Pass | |
| Text | Search box (custom or from another model/question) | Pass | |
| Text | Dropdown list (custom or from another model/question) | Pass | |
| Date | Input box (only option available) | Pass | |
| Field Filter | Location is (=`=` or `IN` for multiple values) | Pass | Do not include the column name in SQL. Map the variable to a field in the side panel. Example: `SELECT count(*) FROM products WHERE 1=1 [[AND {{category}}]]` |
| Field Filter | Location is not (`!=` or `NOT IN` for multiple values) | Pass | |
| Field Filter | Location contains (`LIKE %val%`) | Pass | For multiple values, use `LIKE` with `OR` across values. |
| Field Filter | Location does not contain (`NOT LIKE %val%`) | Pass | For multiple values, use `NOT LIKE` with `OR` across values. |
| Field Filter | Location starts with (`LIKE val%`) | Pass | For multiple values, use `LIKE` with `OR` across values. |
| Field Filter | Location ends with (`LIKE %val`) | Pass | For multiple values, use `LIKE` with `OR` across values. |

## Releases

New driver releases mirror stable Metabase major and point versions: Metabase
`v0.64.3` uses driver tag `v0.64.3` and embedded plugin version `0.64.3`.
Hotfixes and previews are excluded by the [maintenance policy](MAINTENANCE.md).
Historical v1.x driver releases remain unchanged.

### Creating a Release

This project uses GitHub Actions for automated releases. There are two ways to create a release:

#### 1. Using GitHub Actions (Recommended)

1. Go to the [Actions tab](https://github.com/startreedata/metabase-pinot-driver/actions)
2. Select the "Release" workflow
3. Click "Run workflow"
4. Fill in the required parameters:
   - **Version**: Matching stable Metabase major or point version without `v` (e.g., `0.64.3`, `0.65.1`)
   - **Commit SHA**: The full commit SHA to release from
   - **Prerelease**: Leave unchecked under the maintenance policy

#### 2. Using the Release Script

Use the provided release script for a more convenient CLI experience:

```bash
# Release matching stable Metabase v0.64.3
./scripts/release.sh 0.64.3 abc123def456...

# Get help
./scripts/release.sh --help
```

**Prerequisites for the script:**
- [GitHub CLI](https://cli.github.com/) installed and authenticated
- Permissions to create releases on the repository

### Release Process

The release workflow will:
1. Validate the source commit and require the release version to match pinned Metabase
2. Require successful CI for that exact commit and reject an existing release or tag
3. Build the driver with Java 25 and the Clojure version in `app_versions.json`
4. Smoke test that same JAR against pinned Pinot in the build Metabase version and every `metabase_compatibility` version from `app_versions.json`, retaining separate runtime evidence even on failure
5. Verify every runtime passed with the same JAR digest and expected version, then publish that JAR with its checksum and all tested versions in the release notes

### Release Artifacts

Each release includes:
- `pinot.metabase-driver-v{version}.jar` - The driver JAR file
- `pinot.metabase-driver-v{version}.jar.sha256` - SHA256 checksum
- Automatically generated release notes

### Installing a Release

1. Download the driver JAR from the [releases page](https://github.com/startreedata/metabase-pinot-driver/releases)
2. Place it in your Metabase `plugins` directory
3. Restart Metabase
4. Select **StarTree(Pinot)** when adding a database connection

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
- Check the [Apache Pinot documentation](https://docs.pinot.apache.org/integrations/metabase)
- Check the [Metabase documentation](https://www.metabase.com/docs)

## Related Links

- [Apache Pinot](https://pinot.apache.org/)
- [Metabase](https://www.metabase.com/)
