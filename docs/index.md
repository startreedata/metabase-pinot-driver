---
layout: home
title: Home
nav_order: 1
---

# Metabase Pinot Driver

[![CI](https://github.com/startreedata/metabase-pinot-driver/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/startreedata/metabase-pinot-driver/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/startreedata/metabase-pinot-driver/branch/main/graph/badge.svg)](https://codecov.io/gh/startreedata/metabase-pinot-driver)
[![License](https://img.shields.io/github/license/startreedata/metabase-pinot-driver)](https://github.com/startreedata/metabase-pinot-driver/blob/main/LICENSE)
{: .mb-6 }

Connect [Metabase](https://www.metabase.com/) to [Apache Pinot](https://pinot.apache.org/) for real-time analytics and data visualization.
{: .fs-6 .fw-300 }

[Get Started]({{ site.baseurl }}/installation){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/startreedata/metabase-pinot-driver){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## Overview

This driver enables Metabase to connect to Apache Pinot databases, bringing Pinot's real-time analytics capabilities to Metabase's intuitive business intelligence interface. Build dashboards, run queries, and explore your Pinot data — all from within Metabase.

## Features

- **Pinot HTTP API** — Connects to Apache Pinot via its native HTTP API
- **Authentication** — Supports Basic and Bearer token authentication
- **Query Options** — Configurable query options (timeout, multistage engine, etc.)
- **SSH Tunnels** — Built-in SSH tunnel support for secure connections
- **Full MBQL Translation** — Translates Metabase's query language (MBQL) to Pinot SQL
- **SQL Parameters** — Native support for Metabase SQL variable substitution and field filters
- **Aggregations & Filters** — Full support for aggregations, filters, grouping, and ordering

## Compatibility

| Component | Version |
|:----------|:--------|
| Apache Pinot | 1.5.1 |
| Metabase build target | v0.65.2 |
| Java | 25 |
| Clojure CLI | 1.12.3.1577 |

## Quick Start

```bash
# Clone the repository
git clone https://github.com/startreedata/metabase-pinot-driver.git
cd metabase-pinot-driver

# Build the driver
make build

# Start Metabase with the Pinot driver
make server
```

For detailed instructions, see the [Installation Guide]({{ site.baseurl }}/installation).

## Resources

- [Apache Pinot + Metabase Integration Guide](https://docs.pinot.apache.org/integrations/metabase)
- [Apache Pinot Documentation](https://docs.pinot.apache.org/)
- [Metabase Documentation](https://www.metabase.com/docs)
