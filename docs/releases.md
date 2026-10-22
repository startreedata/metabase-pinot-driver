---
layout: default
title: Releases
nav_order: 5
---

# Releases
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Download

Download the latest release from the [GitHub Releases page](https://github.com/startreedata/metabase-pinot-driver/releases).

See the [maintenance policy](https://github.com/startreedata/metabase-pinot-driver/blob/main/MAINTENANCE.md)
for upstream release alignment and compatibility requirements. New driver
versions mirror the target Metabase version: Metabase `v0.64.1` maps to driver
`v0.64.1`, including artifact `pinot.metabase-driver-v0.64.1.jar` and embedded
plugin version `0.64.1`. Four-component upstream hotfix versions are mirrored too.
Release notes must name the exact Metabase versions tested; historical driver
`v1.x` releases remain available.

Each release includes:

- `pinot.metabase-driver-v{version}.jar` — The driver JAR file
- `pinot.metabase-driver-v{version}.jar.sha256` — SHA256 checksum
- Automatically generated release notes

## Creating a Release

### Using GitHub Actions (Recommended)

1. Go to the [Actions tab](https://github.com/startreedata/metabase-pinot-driver/actions)
2. Select the **Release** workflow
3. Click **Run workflow**
4. Fill in the parameters:

   | Parameter | Description |
   |:----------|:------------|
   | Version | Target Metabase version without `v` (e.g., `0.64.1`, `0.64.1.1`, `0.65.0-beta`) |
   | Commit SHA | Full commit SHA to release from |
   | Prerelease | Check if this is a prerelease |

### Using the Release Script

```bash
# Basic release
./scripts/release.sh 0.64.1 abc123def456...

# Prerelease
./scripts/release.sh --prerelease 0.65.0-beta abc123def456...

# Get help
./scripts/release.sh --help
```

{: .note }
The release script requires [GitHub CLI](https://cli.github.com/) installed and authenticated, with permissions to create releases on the repository.

## Release Process

The automated release workflow will:

1. Validate the source commit and require the release version to match pinned Metabase
2. Require successful CI for that exact commit and reject an existing release or tag
3. Build the driver with Java 25 and the Clojure version in `app_versions.json`
4. Smoke test that JAR in the official pinned Metabase against pinned Pinot, retaining runtime evidence even on failure
5. Verify the tested JAR's digest, then publish that same JAR with its checksum and tested versions in the release notes

## Installing a Release

1. Download the driver JAR from the [Releases page](https://github.com/startreedata/metabase-pinot-driver/releases)
2. Place it in your Metabase `plugins/` directory
3. Restart Metabase
4. The Pinot driver will be available for database connections
