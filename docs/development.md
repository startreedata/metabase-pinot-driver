---
layout: default
title: Development
nav_order: 4
---

# Development
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Project Structure

```
metabase-pinot-driver/
├── drivers/pinot/
│   ├── src/metabase/driver/pinot/
│   │   ├── pinot.clj              # Main driver entry point
│   │   ├── client.clj             # HTTP client for Pinot API
│   │   ├── execute.clj            # Query execution
│   │   ├── query_processor.clj    # MBQL to SQL translation
│   │   └── sync.clj               # Database and table sync
│   ├── test/metabase/driver/pinot/
│   │   ├── client_test.clj
│   │   ├── execute_test.clj
│   │   ├── query_processor_test.clj
│   │   └── sync_test.clj
│   ├── resources/
│   │   └── metabase-plugin.yaml   # Driver plugin manifest
│   └── deps.edn                   # Clojure dependencies
├── Makefile                       # Build automation
├── app_versions.json              # Version pinning
└── package.json                   # Node.js dependencies
```

## Build Commands

| Command | Description |
|:--------|:------------|
| `make build` | Build Metabase and the Pinot driver |
| `make driver` | Build only the Pinot driver |
| `make server` | Start Metabase with the driver |
| `make test` | Run the full test suite |

## Running Tests

```bash
make test
```

This will:

1. Start a Pinot instance (if not already running)
2. Link the driver to Metabase
3. Run the test suite with coverage reporting

## Security Scan

CI stores the NVD database separately from Maven dependencies. On a cold cache,
`scripts/bootstrap-nvd.clj` imports the [maintainer's bulk JSON feed](https://github.com/dependency-check/DependencyCheck/discussions/8633)
without an API token. Restored databases skip this import. Both paths require a
non-future boundary less than 120 days old, followed by a fresh NIST API delta and
the full dependency scan. The final config omits the feed URL and sets
`:valid-for-hours 0`; otherwise a new bulk seed can incorrectly skip the API check.

The isolated `:security-scan` alias uses Dependency-Check 12.2.2 for the
[reference URL overflow fix](https://github.com/dependency-check/DependencyCheck/pull/8467).
Earlier engines can silently omit CVE software matches. CI rejects logged CVE
ingestion errors and verifies the final report's API timestamps before saving a
new cache snapshot. The job is bounded to 240 minutes; failed scans never save it.

## Architecture

The driver is composed of several key modules:

### `pinot.clj` — Driver Entry Point

Registers the `:pinot` driver with Metabase and implements the core driver multimethods (e.g., `can-connect?`, `db-default-timezone`).

### `client.clj` — HTTP Client

Handles all HTTP communication with the Pinot controller, including:
- Executing SQL queries via the Pinot query API
- Fetching table and schema metadata
- Authentication header management

### `execute.clj` — Query Execution

Translates Metabase query results from Pinot's response format into Metabase's expected result format. Handles type coercion and result set streaming.

### `query_processor.clj` — MBQL Translation

Converts Metabase's internal query language (MBQL) into Pinot-compatible SQL. Handles aggregations, filters, joins, grouping, ordering, and pagination.

### `sync.clj` — Database Sync

Implements database introspection: discovering tables, reading column metadata, and mapping Pinot types to Metabase's type system.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run `make test` to verify
6. Submit a pull request
