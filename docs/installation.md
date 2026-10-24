---
layout: default
title: Installation
nav_order: 2
---

# Installation
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Prerequisites

Before installing the Metabase Pinot Driver, ensure you have the following:

- **Git**
- **Java 25 (Eclipse Temurin)**
- **Clojure CLI 1.12.3.1577**
- A running **Apache Pinot 1.5.1** instance
- **Metabase v0.64.1**

Building only the driver requires Git, Java, Clojure CLI, Make, and jq. Building the Metabase frontend with `make build` also requires NodeJS 22.23.2 and Bun 1.3.14.

## Install from Release

The easiest way to install the driver is to download a pre-built JAR from the [Releases page](https://github.com/startreedata/metabase-pinot-driver/releases).

1. Download the latest `pinot.metabase-driver-v{version}.jar` from the releases page
2. Place the JAR file in your Metabase `plugins/` directory
3. Restart Metabase
4. The **StarTree(Pinot)** driver will now be available when adding a new database connection

Metabase v0.63/v0.64 bundles connector logos in its frontend. Installing only the
driver JAR updates the connector name but retains Metabase's generic database icon.
For the StarTree logo, use the Metabase frontend built by `make build` below;
`make front_end` also applies the logo integration when rebuilding an existing checkout.

{: .note }
If your Metabase `plugins/` directory doesn't exist, create it in the same directory as your Metabase JAR file.

## Build from Source

1. Clone the repository:

   ```bash
   git clone https://github.com/startreedata/metabase-pinot-driver.git
   cd metabase-pinot-driver
   ```

2. Build the driver:

   ```bash
   make build
   ```

   This builds both Metabase and the Pinot driver.

3. To build only the driver (if you already have Metabase):

   ```bash
   make driver
   ```

4. Start Metabase with the Pinot driver:

   ```bash
   make server
   ```

## Verify Installation

After starting Metabase:

1. Navigate to **Admin** > **Databases** > **Add a database**
2. Look for **StarTree(Pinot)** in the database type dropdown
3. If it appears, the driver is installed correctly
