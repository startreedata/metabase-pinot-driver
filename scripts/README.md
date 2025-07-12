# Pinot Quickstart Scripts

This directory contains scripts to easily start and stop an Apache Pinot cluster locally for testing and development.

## Scripts

### start-pinot-quickstart.sh
Starts a local Apache Pinot cluster with sample data for testing.

**Features:**
- Downloads and sets up Apache Pinot automatically
- Starts a multi-stage quickstart cluster
- Includes sample datasets: airlineStats, baseballStats, dimBaseballTeams, githubComplexTypeEvents, githubEvents, starbucksStores
- Waits for cluster to be ready and validates data availability
- Configurable via environment variables

**Usage:**
```bash
./scripts/start-pinot-quickstart.sh
```

**Environment Variables:**
- `PINOT_VERSION`: Pinot version to use (default: 1.3.0)
- `PINOT_HOME`: Installation directory (default: /tmp/pinot)
- `BROKER_PORT_FORWARD`: Broker port (default: 8000)

**Example with custom settings:**
```bash
export PINOT_VERSION="1.2.0"
export PINOT_HOME="/usr/local/pinot"
export BROKER_PORT_FORWARD="8080"
./scripts/start-pinot-quickstart.sh
```

### stop-pinot-quickstart.sh
Stops the running Pinot cluster processes.

**Usage:**
```bash
./scripts/stop-pinot-quickstart.sh
```

## Access Points

Once started, you can access:
- **Pinot Controller UI**: http://localhost:9000
- **Pinot Broker**: http://localhost:8000 (or your custom `BROKER_PORT_FORWARD`)

## Sample Queries

After starting the cluster, you can test with these sample queries:

```sql
-- Count records in baseball stats
SELECT COUNT(*) FROM baseballStats

-- Get airline stats by carrier
SELECT Carrier, COUNT(*) as flights 
FROM airlineStats 
GROUP BY Carrier 
ORDER BY flights DESC 
LIMIT 10

-- GitHub events by type
SELECT type, COUNT(*) as event_count 
FROM githubEvents 
GROUP BY type 
ORDER BY event_count DESC
```

## Requirements

- Java 8+ (for running Pinot)
- curl (for downloading Pinot)
- jq (for JSON parsing in validation)

## Notes

- The start script will automatically download Apache Pinot if not already present
- The cluster includes sample data that's perfect for testing the Metabase Pinot driver
- Scripts are based on the [startreedata/pinot-client-go](https://github.com/startreedata/pinot-client-go) repository
- For production use, consider using proper Pinot deployment methods instead of these quickstart scripts 