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

1. Clone the pinned Metabase source (if not already present)
2. Link the driver to Metabase
3. Run the existing driver tests once with coverage reporting

These tests use mocked Pinot responses.

`make test` requires Python 3 and enforces 68% line coverage. Form coverage is reported separately because upstream logging macro expansion changes its denominator between Metabase versions.

## Packaged Driver Compatibility

After `make driver`, exercise the JAR in the exact Metabase version pinned in
`app_versions.json`. Java 25, Python 3.9+, the official Metabase JAR, and a running
Pinot quickstart with the `baseballStats` table are required:

```bash
python3 scripts/smoke-test.py \
  --driver-jar metabase/resources/modules/pinot.metabase-driver.jar \
  --metabase-jar /path/to/metabase.jar \
  --controller-url http://localhost:9000 \
  --output-dir target/smoke
```

The command starts a disposable Metabase application, loads the packaged plugin,
connects to Pinot, syncs its schema, and compares native SQL, MBQL, aggregations,
and native parameter results with direct Pinot queries. It stops its application
and removes its temporary database on exit. Query responses, version information,
artifact hashes, and logs remain in the output directory. CI runs this gate against
the pinned Pinot quickstart before reporting compatibility.

The gate verifies the Pinot runtime version against `app_versions.json` (currently
1.5.1). To check another concrete release, pass `--pinot-version`, for example
`--pinot-version 1.5.0` when that release is running at the supplied controller URL.

The driver accepts Metabase's MBQL 5 input through its legacy-query conversion API
and retains the Pinot SQL compiler. Native parameters use the current stage-based
hook; execution reads the compiled query from `:qp/compiled`.

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
