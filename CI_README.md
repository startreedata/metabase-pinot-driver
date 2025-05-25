# GitHub Actions CI/CD

This repository includes GitHub Actions workflows for building and testing the Metabase Pinot driver, optimized for PR submissions.

## Workflows

### CI Workflow (`.github/workflows/ci.yml`)

The main CI workflow runs on every push to the `main` branch and on pull requests targeting `main`. It includes:

#### Jobs:

1. **test** - Main build and test job
   - Sets up Java 17, Clojure 1.11.1.1262, and Node.js 18
   - Clones Metabase repository (v1.52.2.2)
   - Links the Pinot driver to Metabase
   - Builds Metabase frontend
   - Builds the Pinot driver
   - Runs tests against a Pinot server (Docker container)
   - Uploads test results as artifacts

2. **lint** - Code linting
   - Runs clj-kondo linting on the driver source code
   - Checks for code style and potential issues

3. **security** - Security scanning
   - Runs security-focused linting with clj-kondo
   - Identifies potential security vulnerabilities

## Prerequisites

The workflows require:

- **Java 17** - For Clojure and Metabase
- **Clojure 1.11.1.1262** - For building and testing
- **Node.js 18** - For Metabase frontend builds
- **Yarn** - For Node.js dependency management
- **Docker** - For Pinot server container

## Local Development

To run the same steps locally:

```bash
# Clone Metabase
git clone -b v1.52.2.2 --depth 1 https://github.com/metabase/metabase.git

# Install dependencies
cd metabase
yarn install --frozen-lockfile

# Link driver
ln -s ../../drivers/pinot modules/drivers/pinot

# Update deps files (see Makefile for details)
# ... update deps.edn files ...

# Build frontend
export MB_EDITION=ee
export NODE_OPTIONS='--max-old-space-size=4096'
yarn build-static-viz
export WEBPACK_BUNDLE=production
yarn build-release
yarn build-static-viz

# Build driver
./bin/build-driver.sh pinot

# Run tests (requires Pinot server running on port 9000)
export DRIVERS=pinot
export MB_PINOT_TEST_PORT=9000
clojure -X:dev:drivers:drivers-dev:test
```

## Pinot Server

The workflows use a Docker container for Pinot server:

```yaml
services:
  pinot:
    image: apachepinot/pinot:latest
    ports:
      - 9000:9000
      - 8099:8099
```

For local development, you can start Pinot using:

```bash
docker run -p 9000:9000 -p 8099:8099 apachepinot/pinot:latest
```

## Cache Strategy

The workflows use GitHub Actions caching to speed up builds:

- **Clojure dependencies**: `~/.m2`, `~/.gitlibs`, `.cpcache`
- **Node modules**: `node_modules`, `*/*/node_modules`

Cache keys are based on dependency file hashes for optimal cache hits.

## Artifacts

The workflows generate and upload:

- **Test results**: Coverage reports and test output

## PR Workflow

When you submit a PR to the `main` branch, the workflow will:

1. **Automatically trigger** on PR creation/updates
2. **Run in parallel**:
   - Build and test the driver
   - Lint the code
   - Scan for security issues
3. **Provide feedback** through GitHub's PR interface
4. **Upload artifacts** for test results

## Branch Strategy

- **Main branch**: Protected with CI checks
- **PRs to main**: Must pass all CI jobs
- **Direct pushes to main**: Will trigger CI (use with caution)

## Troubleshooting

### Common Issues

1. **Pinot server not ready**: The workflow waits up to 60 seconds for Pinot to be healthy
2. **Memory issues**: Node.js is configured with `--max-old-space-size=4096`
3. **Dependency conflicts**: Clear cache and rebuild if needed

### Debugging

To debug workflow issues:

1. Check the "Wait for Pinot" step output
2. Review test results artifacts
3. Verify dependency versions match `app_versions.json`

## Configuration

Key configuration files:

- `app_versions.json` - Version specifications
- `drivers/pinot/deps.edn` - Clojure dependencies and aliases
- `Makefile` - Local development commands
- `.github/workflows/ci.yml` - Main CI workflow 