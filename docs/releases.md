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
for upstream release alignment and compatibility requirements. Driver versions
use independent semantic versioning; release notes must name the exact Metabase
versions tested.

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
   | Version | Semantic version (e.g., `1.0.0`, `1.0.0-alpha.1`) |
   | Commit SHA | Full commit SHA to release from |
   | Prerelease | Check if this is a prerelease |

### Using the Release Script

```bash
# Basic release
./scripts/release.sh 1.0.0 abc123def456...

# Prerelease
./scripts/release.sh --prerelease 1.0.0-alpha.1 abc123def456...

# Get help
./scripts/release.sh --help
```

{: .note }
The release script requires [GitHub CLI](https://cli.github.com/) installed and authenticated, with permissions to create releases on the repository.

## Release Process

The automated release workflow will:

1. Validate the version format and commit SHA
2. Build the Pinot driver JAR
3. Create release artifacts with checksums
4. Generate release notes
5. Create a GitHub release with the artifacts attached

## Installing a Release

1. Download the driver JAR from the [Releases page](https://github.com/startreedata/metabase-pinot-driver/releases)
2. Place it in your Metabase `plugins/` directory
3. Restart Metabase
4. The Pinot driver will be available for database connections
